from .headers import ffi as _ffi, lib as _lib
from .message import Message
from .utils import _voidp2bytes


callback_funcs = [] # TODO need a better place to keep our cffi callbacks alive


def conf_set_dr_msg_cb(conf_handle, callback_func):
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
            callback_func(msg, opaque=None)
            # TODO the above should become opaque=_ffi.from_handle(opaque)
            # but this requires that we always configure the opaque (eg.
            # set it to None by default)
        finally:
            # Clear the handle we created for the msg_opaque, as we don't
            # expect to see it after this:
            msg._free_opaque()

    callback_funcs.append(func) # prevent garbage-collection of func
    _lib.rd_kafka_conf_set_dr_msg_cb(conf_handle, func)


def topic_conf_set_partitioner_cb(topic_conf_handle, callback_func):
    """
    Python callback to partition messages

    Set a python callback_func with signature
        'key_string, partitions_available_list => item'
    where item is selected from partition_list, or None if the function
    couldn't decide a partition.
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

    callback_funcs.append(func) # prevent garbage-collection of func
    _lib.rd_kafka_topic_conf_set_partitioner_cb(topic_conf_handle, func)
