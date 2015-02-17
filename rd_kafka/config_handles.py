import json
import logging
import weakref

from .headers import ffi, lib
from .message import Message
from . import utils


logger = logging.getLogger(__name__)


class ConfigManager(object):
    """
    Helper class for KafkaHandle that manages callback handles

    Usage notes: as this class manages cffi handles internally, it should be
    kept alive for as long as the kafka_handle is alive.  After initing it
    with a config dict, the thus constructed rd_kafka_conf_t handle should be
    obtained via pop_config()
    """

    def __init__(self, kafka_handle, config_dict):
        self.kafka_handle = weakref.proxy(kafka_handle) # avoid circular ref
        self.cdata = lib.rd_kafka_conf_new() # NB see pop_config() below
        self.callbacks = {} # keeps cffi callback handles alive
        self.set(config_dict)

    def pop_config(self):
        """
        Return rd_kafka_conf_t handle after removing it from self

        (Awkward but needed, because rd_kafka_new() destroys the conf struct)
        """
        cdata, self.cdata = self.cdata, None
        return cdata

    def set(self, config_dict):
        if self.cdata is None:
            raise LibrdkafkaException("Called after pop_config()?")
        for name, value in config_dict.items():
            try: # if this is a callback setter:
                getattr(self, "set_" + name)(value)
            except AttributeError:
                errstr = utils.mk_errstr()
                res = lib.rd_kafka_conf_set(
                          self.cdata, name, value, errstr, len(errstr))
                if res != lib.RD_KAFKA_CONF_OK:
                    raise LibrdkafkaException(ffi.string(errstr))

    def set_dr_cb(self, callback_func):
        raise NotImplementedError("Try dr_msg_cb instead?")

    def set_dr_msg_cb(self, callback_func):
        """
        Set python callback to accept delivery reports

        Pass a callback_func with signature f(msg, **kwargs), where msg will
        be a message.Message and kwargs currently provides 'kafka_handle' and
        'opaque'
        """
        @ffi.callback("void (rd_kafka_t *, const rd_kafka_message_t *, void *)")
        def func(rdk_handle, msg, opaque):
            try:
                msg = Message(msg, manage_memory=False)
                opq = None if opaque == ffi.NULL else ffi.from_handle(opaque)
                # Note that here, rdk_handle will point to the same data as
                # self.kafka_handle.cdata, so it makes more sense to hand users
                # the richer self.kafka_handle
                callback_func(msg, kafka_handle=self.kafka_handle, opaque=opq)
            finally:
                # Clear the handle we created for the msg_opaque, as we don't
                # expect to see it after this:
                msg._free_opaque()

        lib.rd_kafka_conf_set_dr_msg_cb(self.cdata, func)
        self.callbacks["dr_msg_cb"] = func

    def set_error_cb(self, callback_func):
        """
        """
        @ffi.callback("void (rd_kafka_t *, int, const char *, void *)")
        def func(rdk_handle, err, reason, opaque):
            pass # TODO (if unset, errors will flow to log_cb instead, anyway)
        raise NotImplementedError

    def set_log_cb(self, callback_func):
        """
        Set a logging callback with the same signature as logging.Logger.log()
        """
        @ffi.callback("void (rd_kafka_t *, int, const char*, const char *)")
        def func(rdk_handle, syslog_level, facility, message):
            callback_func(syslog_level=syslog_level,
                          facility=ffi.string(facility),
                          message=ffi.string(message),
                          kafka_handle=self.kafka_handle)

        lib.rd_kafka_conf_set_log_cb(self.cdata, func)
        self.callbacks["log_cb"] = func

    def set_stats_cb(self, callback_func):
        """
        Set python callback to accept statistics data

        Pass a callback_func with signature f(stats, **kwargs), where 'stats'
        is a dict of librdkafka statistics, and kwargs currently provides
        'kafka_handle' and 'opaque'
        """
        @ffi.callback("int (rd_kafka_t *, char *, size_t, void *)")
        def func(rdk_handle, stats, stats_len, opaque):
            stats = json.loads(ffi.string(stats, maxlen=stats_len))
            opq = None if opaque == ffi.NULL else ffi.from_handle(opaque)
            callback_func(stats, kafka_handle=self.kafka_handle, opaque=opq)
            return 0 # tells librdkafka to free the json pointer

        lib.rd_kafka_conf_set_stats_cb(self.cdata, func)
        self.callbacks["stats_cb"] = func


def topic_conf_set_partitioner_cb(topic_conf_handle, callback_func):
    """
    Python callback to partition messages

    Set a python callback_func with signature
        'key_string, partitions_available_list => item'
    where item is selected from partition_list, or None if the function
    couldn't decide a partition.

    NB returns a cffi callback handle that must be kept alive
    """
    @ffi.callback("int32_t (const rd_kafka_topic_t *, const void *,"
                  "         size_t, int32_t, void *, void *)")
    def func(topic, key, key_len, partition_cnt, t_opaque, m_opaque):
        key = utils.voidp2bytes(key, key_len)[:]
        partition_list = range(partition_cnt)
        while partition_list:
            p = callback_func(key, partition_list)
            if (p is None or
                    lib.rd_kafka_topic_partition_available(topic, p)):
                break
            else:
                partition_list.remove(p)
                p = None
        return lib.RD_KAFKA_PARTITION_UA if p is None else p

    lib.rd_kafka_topic_conf_set_partitioner_cb(topic_conf_handle, func)
    return func


class StdlibLoggerCallback(object):
    """
    Convenience class to tie ConfigManager.set_log_cb() to a logging.Logger

    Usage: config_dict["log_cb"] = StdlibLoggerCallback(logger_instance)
    """
    MAP_SYSLOG_LEVELS = { # cf sys/syslog.h
            0: logging.CRITICAL,
            1: logging.CRITICAL,
            2: logging.CRITICAL,
            3: logging.ERROR,
            4: logging.WARNING,
            5: logging.INFO,
            6: logging.INFO,
            7: logging.DEBUG}

    def __init__(self, logger=logger):
        self.logger = logger

    def __call__(self, syslog_level, facility, message, kafka_handle):
        self.logger.log(
                self.MAP_SYSLOG_LEVELS[syslog_level],
                message,
                extra=dict(syslog_level=syslog_level,
                           facility=facility,
                           kafka_handle=kafka_handle))


def default_config():
    """
    A dictionary pre-populated with rd_kafka_conf_t defaults

    Note that any settings that aren't set by default, as well as any
    callbacks, will be missing from this.
    """
    # Dump a fresh config:
    conf_cdata = lib.rd_kafka_conf_new()
    dump_len = ffi.new("size_t *")
    dump_cdata = lib.rd_kafka_conf_dump(conf_cdata, dump_len)

    # What we got are alternating strings of keys and values; re-shuffle:
    keys_values = [ffi.string(dump_cdata[i]) for i in range(dump_len[0])]
    keys_values = zip(keys_values[0::2], keys_values[1::2])

    # Before we're done, purge the callbacks, which come as stringified
    # memory addresses - not really useful to build a new config with:
    keys_values = filter(lambda kv: not kv[0].endswith("_cb"),
                         keys_values)

    # Free memory:
    lib.rd_kafka_conf_dump_free(dump_cdata, dump_len[0])
    lib.rd_kafka_conf_destroy(conf_cdata)

    return dict(keys_values)
