from copy import deepcopy

from cffi import FFI


_ffi = FFI()
_ffi.cdef(
    # Most of this copied verbatim from librdkafka/rdkafka.h:
    """
    typedef enum rd_kafka_type_t {
        RD_KAFKA_PRODUCER, RD_KAFKA_CONSUMER, ... } rd_kafka_type_t;
    typedef ... rd_kafka_t;
    typedef ... rd_kafka_topic_t;
    typedef ... rd_kafka_conf_t;
    typedef ... rd_kafka_topic_conf_t;
    typedef enum { ... } rd_kafka_resp_err_t;

    const char *rd_kafka_err2str (rd_kafka_resp_err_t err);
    rd_kafka_resp_err_t rd_kafka_errno2err (int errnox);

    typedef enum {RD_KAFKA_CONF_OK, ...  } rd_kafka_conf_res_t;
    rd_kafka_conf_t *rd_kafka_conf_new (void);
    rd_kafka_conf_t *rd_kafka_conf_dup (const rd_kafka_conf_t *conf);
    void rd_kafka_conf_destroy (rd_kafka_conf_t *conf);
    rd_kafka_conf_res_t rd_kafka_conf_set (rd_kafka_conf_t *conf,
                                           const char *name,
                                           const char *value,
                                           char *errstr,
                                           size_t errstr_size);

    rd_kafka_topic_conf_t *rd_kafka_topic_conf_new (void);
    rd_kafka_topic_conf_t *rd_kafka_topic_conf_dup (const rd_kafka_topic_conf_t
                                                    *conf);
    void rd_kafka_topic_conf_destroy (rd_kafka_topic_conf_t *topic_conf);
    rd_kafka_conf_res_t rd_kafka_topic_conf_set (rd_kafka_topic_conf_t *conf,
                                                 const char *name,
                                                 const char *value,
                                                 char *errstr,
                                                 size_t errstr_size);

    rd_kafka_t *rd_kafka_new (rd_kafka_type_t type, rd_kafka_conf_t *conf,
                              char *errstr, size_t errstr_size);
    void rd_kafka_destroy (rd_kafka_t *rk);

    rd_kafka_topic_t *rd_kafka_topic_new (rd_kafka_t *rk, const char *topic,
                                          rd_kafka_topic_conf_t *conf);
    void rd_kafka_topic_destroy (rd_kafka_topic_t *rkt);

    #define RD_KAFKA_PARTITION_UA ...
    #define RD_KAFKA_MSG_F_COPY ...

    int rd_kafka_produce (rd_kafka_topic_t *rkt, int32_t partitition,
                          int msgflags,
                          void *payload, size_t len,
                          const void *key, size_t keylen,
                          void *msg_opaque);
    """)
_lib = _ffi.verify("#include <librdkafka/rdkafka.h>", libraries=['rdkafka'])


def _mk_errstr():
    # probably way oversized:
    return _ffi.new("char []", 512)


def _errno2str(errno=None):
    """ Look up string error message for errno """
    if errno is None:
        errno = _ffi.errno
    return _ffi.string(_lib.rd_kafka_err2str(_lib.rd_kafka_errno2err(errno)))


class LibrdkafkaException(Exception):
    pass


class Config(object):
    def __init__(self, cdata=None):
        self.cdata = cdata or _lib.rd_kafka_conf_new()

    def __copy__(self):
        raise NotImplementedError

    def __deepcopy__(self, memo):
        new_cdata = _lib.rd_kafka_conf_dup(self.cdata)
        return Config(new_cdata)

    def __del__(self):
        if self.cdata is not None:
            # NB we must set cdata to None after calling functions on it that
            # destroy it, to avoid a double-free here:
            _lib.rd_kafka_conf_destroy(self.cdata)

    def set(self, name, value):
        errstr = _mk_errstr()
        res = _lib.rd_kafka_conf_set(
                  self.cdata, name, value, errstr, len(errstr))
        if res != _lib.RD_KAFKA_CONF_OK:
            raise LibrdkafkaException(_ffi.string(errstr))


class TopicConfig(object):
    # TODO have a dict interface with __getitem__ etc
    def __init__(self, cdata=None):
        self.cdata = cdata or _lib.rd_kafka_topic_conf_new()

    def __copy__(self):
        raise NotImplementedError

    def __deepcopy__(self, memo):
        new_cdata = _lib.rd_kafka_topic_conf_dup(self.cdata)
        return TopicConfig(new_cdata)

    def __del__(self):
        if self.cdata is not None:
            _lib.rd_kafka_topic_conf_destroy(self.cdata)

    def set(self, name, value):
        # TODO accept dr_msg_cb, log_cb, etc
        errstr = _mk_errstr()
        res = _lib.rd_kafka_topic_conf_set(
                  self.cdata, name, value, errstr, len(errstr))
        if res != _lib.RD_KAFKA_CONF_OK:
            raise LibrdkafkaException(_ffi.string(errstr))


class Producer(object):
    def __init__(self, config):
        errstr = _mk_errstr()
        cfg = deepcopy(config) # rd_kafka_new() will free config.cdata
        self.cdata = _lib.rd_kafka_new(
                         _lib.RD_KAFKA_PRODUCER, cfg.cdata, errstr, len(errstr))
        cfg.cdata = None
        if self.cdata == _ffi.NULL:
            raise LibrdkafkaException(_ffi.string(errstr))

    def __del__(self):
        _lib.rd_kafka_destroy(self.cdata)


class Topic(object):
    def __init__(self, name, producer, topic_config):
        # prevent producer getting garbage-collected for the life of this Topic:
        self._producer = producer

        cfg = deepcopy(topic_config) # next call would free topic_config.cdata
        self.cdata = _lib.rd_kafka_topic_new(
                         self._producer.cdata, name, cfg.cdata)
        cfg.cdata = None # prevent double-free in cfg.__del__()
        if self.cdata == _ffi.NULL:
            raise LibrdkafkaException(_errno2str())

    def __del__(self):
        _lib.rd_kafka_topic_destroy(self.cdata)

    def produce(self, payload, key=None,
                partition=_lib.RD_KAFKA_PARTITION_UA, msg_opaque=None):
        key = key or _ffi.NULL
        msg_opaque = msg_opaque or _ffi.NULL
        rv = _lib.rd_kafka_produce(
                 self.cdata, partition, _lib.RD_KAFKA_MSG_F_COPY,
                 payload, len(payload),
                 key, len(key) if key else 0,
                 msg_opaque)
        if rv:
            raise LibrdkafkaException(_errno2str())
