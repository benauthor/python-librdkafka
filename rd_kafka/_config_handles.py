import json

from .headers import ffi as _ffi, lib as _lib
from .message import Message
from .utils import _voidp2bytes


class ConfigCbManager(object):
    """ Helper class for KafkaHandle that manages callback handles """

    def __init__(self, kafka_handle):
        self.kafka_handle = kafka_handle
        self.callbacks = {} # keeps cffi callback handles alive

    def set_dr_msg_cb(self, conf_handle, callback_func):
        """
        Set python callback to accept delivery reports

        Pass a callback_func with signature f(msg, **kwargs), where msg will be
        a message.Message and kwargs is currently empty (but should eventually
        provide the KafkaHandle and the configured opaque handle)
        """
        @_ffi.callback("void (rd_kafka_t *,"
                       "      const rd_kafka_message_t *, void *)")
        def func(kafka_handle, msg, opaque):
            try:
                # XXX modify KafkaHandle so we can wrap it here and pass it on?
                msg = Message(msg, manage_memory=False)
                opaque = None if opaque == _ffi.NULL else _ffi.from_handle(opaque)
                callback_func(msg, opaque=opaque)
            finally:
                # Clear the handle we created for the msg_opaque, as we don't
                # expect to see it after this:
                msg._free_opaque()

        _lib.rd_kafka_conf_set_dr_msg_cb(conf_handle, func)
        self.callbacks["dr_msg_cb"] = func

    def set_stats_cb(self, conf_handle, callback_func):
        """
        Set python callback to accept statistics data

        """
        @_ffi.callback("int (rd_kafka_t *, char *, size_t, void *)")
        def func(kafka_handle, stats, stats_len, opaque):
            # TODO actually use callback_func
            print json.loads(_ffi.string(stats, maxlen=stats_len))
            return 0

        _lib.rd_kafka_conf_set_stats_cb(conf_handle, func)
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
    @_ffi.callback("int32_t (const rd_kafka_topic_t *, const void *,"
                   "         size_t, int32_t, void *, void *)")
    def func(topic, key, key_len, partition_cnt, t_opaque, m_opaque):
        key = _voidp2bytes(key, key_len)[:]
        partition_list = range(partition_cnt)
        while partition_list:
            p = callback_func(key, partition_list)
            if (p is None or
                    _lib.rd_kafka_topic_partition_available(topic, p)):
                break
            else:
                partition_list.remove(p)
                p = None
        return _lib.RD_KAFKA_PARTITION_UA if p is None else p

    _lib.rd_kafka_topic_conf_set_partitioner_cb(topic_conf_handle, func)
    return func
