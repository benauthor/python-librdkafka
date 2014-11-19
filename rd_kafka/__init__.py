from copy import deepcopy
import errno

from headers import ffi as _ffi, lib as _lib
import partition_reader
from utils import _mk_errstr, _err2str, _errno2str


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


class BaseTopic(object):
    def __init__(self, name, kafka_handle, topic_config):
        # prevent handle getting garbage-collected for the life of this Topic:
        self._kafka_handle = kafka_handle # TODO obsolete

        cfg = deepcopy(topic_config) # next call would free topic_config.cdata
        self.cdata = _lib.rd_kafka_topic_new(
                         self._kafka_handle.cdata, name, cfg.cdata)
        cfg.cdata = None # prevent double-free in cfg.__del__()
        if self.cdata == _ffi.NULL:
            raise LibrdkafkaException(_errno2str())

    def __del__(self):
        _lib.rd_kafka_topic_destroy(self.cdata)


class KafkaHandle(object):
    topic_type = BaseTopic

    def __init__(self, handle_type, config):
        errstr = _mk_errstr()
        cfg = deepcopy(config) # rd_kafka_new() will free config.cdata
        self.cdata = _lib.rd_kafka_new(
                         handle_type, cfg.cdata, errstr, len(errstr))
        cfg.cdata = None
        if self.cdata == _ffi.NULL:
            raise LibrdkafkaException(_ffi.string(errstr))

    def __del__(self):
        _lib.rd_kafka_destroy(self.cdata)

    def open_topic(self, name, topic_config):
        return self.topic_type(name, self, topic_config)

    def poll(self, timeout_ms=1000):
        return _lib.rd_kafka_poll(self.cdata, timeout_ms)


class ProducerTopic(BaseTopic):

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


class Producer(KafkaHandle):
    topic_type = ProducerTopic

    def __init__(self, config):
        super(Producer, self).__init__(handle_type=_lib.RD_KAFKA_PRODUCER,
                                       config=config)

    def __del__(self):
        # flush the write queues:
        while _lib.rd_kafka_outq_len(self.cdata) > 0:
            # TODO make sure we can break out of here
            self.poll(100)
        super(Producer, self).__del__()


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


class ConsumerTopic(BaseTopic):
    OFFSET_BEGINNING = partition_reader.OFFSET_BEGINNING
    OFFSET_END = partition_reader.OFFSET_END

    def open_partition(self, partition, start_offset):
        return partition_reader.open_partition(self, partition, start_offset)


class Consumer(KafkaHandle):
    topic_type = ConsumerTopic

    def __init__(self, config):
        super(Consumer, self).__init__(handle_type=_lib.RD_KAFKA_CONSUMER,
                                       config=config)
