from copy import deepcopy
from ctypes import *


_lib = CDLL("librdkafka.so.1", use_errno=True)


RD_KAFKA_PARTITION_UA = c_int32(-1)
RD_KAFKA_MSG_F_COPY = 0x2


def _mk_errstr():
    # probably way oversized:
    return create_string_buffer(512)


def _errno2str(errno=None):
    """ Look up string error message for errno """
    if errno is None:
        errno = get_errno()
    err2str, errno2err = _lib.rd_kafka_err2str, _lib.rd_kafka_errno2err
    err2str.restype = c_char_p
    return err2str(errno2err(errno))


def _mk_global(symbol, restype=None):
    """ Auto-wrap simple _lib functions """
    func = getattr(_lib, 'rd_kafka_' + symbol)
    func.restype = restype
    globals()[symbol] = func


class LibrdkafkaException(Exception):
    pass


class Config(object):
    def __init__(self, pointer=None):
        if pointer is None:
            f = _lib.rd_kafka_conf_new
            f.restype = c_void_p
            self.pointer = f()
        else:
            self.pointer = pointer

    def __copy__(self):
        raise NotImplementedError

    def __deepcopy__(self, memo):
        new_pointer = _lib.rd_kafka_conf_dup(self.pointer)
        return Config(new_pointer)

    def __del__(self):
        if self.pointer:
            # ^^ may be None, eg rd_kafka_new() calls destroy on our behalf
            _lib.rd_kafka_conf_destroy(self.pointer)


    def set(self, name, value):
        errstr = _mk_errstr()
        res = _lib.rd_kafka_conf_set(
                  self.pointer, name, value, errstr, sizeof(errstr))
        if res:
            raise LibrdkafkaException(errstr.value)


class TopicConfig(object):
    # TODO have a dict interface with __getitem__ etc
    def __init__(self, pointer=None):
        if pointer is None:
            f = _lib.rd_kafka_topic_conf_new
            f.restype = c_void_p
            self.pointer = f()
        else:
            self.pointer = pointer

    def __copy__(self):
        raise NotImplementedError

    def __deepcopy__(self, memo):
        new_pointer = _lib.rd_kafka_topic_conf_dup(self.pointer)
        return TopicConfig(new_pointer)

    def __del__(self):
        if self.pointer:
            _lib.rd_kafka_topic_conf_destroy(self.pointer)

    def set(self, name, value):
        errstr = _mk_errstr()
        res = _lib.rd_kafka_topic_conf_set(
                  self.pointer, name, value, errstr, sizeof(errstr))
        if res:
            raise LibrdkafkaException(errstr.value)


# TODO create callback classes for rd_kafka_conf_set_dr_msg_cb, rd_kafka_set_logger, etc


class Producer(object):
    def __init__(self, config):
        f = _lib.rd_kafka_new
        f.restype = c_void_p
        errstr = _mk_errstr()
        cfg = deepcopy(config) # rd_kafka_new() will free config.pointer
        self.pointer = f(0, cfg.pointer, errstr, sizeof(errstr))
        cfg.pointer = None
        if not self.pointer:
            raise LibrdkafkaException(errstr.value)

    def __del__(self):
        _lib.rd_kafka_destroy(self.pointer)


class Topic(object):
    def __init__(self, name, producer, topic_config):
        f = _lib.rd_kafka_topic_new
        f.restype = c_void_p

        # prevent producer getting garbage-collected for the life of this Topic:
        self._producer = producer

        cfg = deepcopy(topic_config) # f() would free topic_config.pointer
        self.pointer = f(self._producer.pointer, name, cfg.pointer)
        cfg.pointer = None # prevent double-free in cfg.__del__()
        if not self.pointer:
            raise LibrdkafkaException(_errno2str())

    def __del__(self):
        _lib.rd_kafka_topic_destroy(self.pointer)

    def produce(
            self, payload, key=None,
            partition=RD_KAFKA_PARTITION_UA, msg_opaque=None):
        rv = _lib.rd_kafka_produce(
                 self.pointer, partition, RD_KAFKA_MSG_F_COPY,
                 payload, len(payload),
                 key, len(key) if key is not None else 0, msg_opaque)
        if rv:
            raise LibrdkafkaException(_errno2str())
