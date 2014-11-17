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
    typedef enum {RD_KAFKA_RESP_ERR_NO_ERROR, ...} rd_kafka_resp_err_t;

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

    typedef struct rd_kafka_metadata_broker {
            int32_t     id;             /* Broker Id */
            char       *host;           /* Broker hostname */
            int         port;           /* Broker listening port */
    } rd_kafka_metadata_broker_t;

    typedef struct rd_kafka_metadata_partition {
            int32_t     id;             /* Partition Id */
            rd_kafka_resp_err_t err;    /* Partition error reported by broker */
            int32_t     leader;         /* Leader broker */
            int         replica_cnt;    /* Number of brokers in 'replicas' */
            int32_t    *replicas;       /* Replica brokers */
            int         isr_cnt;        /* Number of ISR brokers in 'isrs' */
            int32_t    *isrs;           /* In-Sync-Replica brokers */
    } rd_kafka_metadata_partition_t;

    typedef struct rd_kafka_metadata_topic {
            char       *topic;          /* Topic name */
            int         partition_cnt;  /* Number of partitions in 'partitions' */
            struct rd_kafka_metadata_partition *partitions; /* Partitions */
            rd_kafka_resp_err_t err;    /* Topic error reported by broker */
    } rd_kafka_metadata_topic_t;

    typedef struct rd_kafka_metadata {
            int         broker_cnt;     /* Number of brokers in 'brokers' */
            struct rd_kafka_metadata_broker *brokers;  /* Brokers */
            int         topic_cnt;      /* Number of topics in 'topics' */
            struct rd_kafka_metadata_topic *topics;    /* Topics */
            int32_t     orig_broker_id; /* Broker originating this metadata */
            char       *orig_broker_name; /* Name of originating broker */
    } rd_kafka_metadata_t;

    rd_kafka_resp_err_t
    rd_kafka_metadata (rd_kafka_t *rk, int all_topics,
                       rd_kafka_topic_t *only_rkt,
                       const struct rd_kafka_metadata **metadatap,
                       int timeout_ms);
    void rd_kafka_metadata_destroy (const struct rd_kafka_metadata *metadata);

    """)
_lib = _ffi.verify("#include <librdkafka/rdkafka.h>", libraries=['rdkafka'])


def _mk_errstr():
    # probably way oversized:
    return _ffi.new("char []", 512)


def _err2str(err):
    return _ffi.string(_lib.rd_kafka_err2str(err))


def _errno2str(errno=None):
    """ Look up string error message for errno """
    errno = _ffi.errno if errno is None else errno
    return _err2str(_lib.rd_kafka_errno2err(errno))


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


class Metadata(object):
    def __init__(self, kafka_handle, topic=None, timeout_ms=1000):
        self.kafka_handle = kafka_handle
        self.topic = topic
        self.all_topics = 0 # TODO expose this option usefully?
        self.refresh(timeout_ms)

    def refresh(self, timeout_ms=1000):
        topic = _ffi.NULL if self.topic is None else self.topic.cdata
        meta_dp = _ffi.new("const rd_kafka_metadata_t **")
        err = _lib.rd_kafka_metadata(
                  self.kafka_handle.cdata, self.all_topics,
                  topic, meta_dp, timeout_ms)
        if err != _lib.RD_KAFKA_RESP_ERR_NO_ERROR:
            raise LibrdkafkaException(_err2str(err))

        self.dict = d = {}
        meta = meta_dp[0]
        d['brokers'] = {}
        for i in range(meta.broker_cnt):
            b = meta.brokers[i]
            d['brokers'][b.id] = dict(host=_ffi.string(b.host), port=b.port)
        d['topics'] = {}
        for i in range(meta.topic_cnt):
            t = meta.topics[i]
            d['topics'][_ffi.string(t.topic)] = topic_d = {}
            topic_d['partitions'] = {}
            for j in range(t.partition_cnt):
                p = t.partitions[j]
                topic_d['partitions'][p.id] = dict(
                        err=_err2str(p.err), leader=p.leader,
                        replicas=[p.replicas[r] for r in range(p.replica_cnt)],
                        isrs=[p.isrs[r] for r in range(p.isr_cnt)])
            topic_d['err'] = _err2str(t.err)
        d['orig_broker_id'] = meta.orig_broker_id
        d['orig_broker_name'] = _ffi.string(meta.orig_broker_name)

        _lib.rd_kafka_metadata_destroy(meta_dp[0])
