import logging

from . import _config_handles, _msg_opaques
from headers import ffi as _ffi, lib as _lib
from .partition_reader import QueueReader, PartitionReaderException
from .utils import _mk_errstr, _err2str, _errno2str


logger = logging.getLogger(__name__)


class LibrdkafkaException(Exception):
    pass


class BaseTopic(object):
    def __init__(self, name, kafka_handle, topic_config_dict):
        self.kafka_handle = kafka_handle

        self.conf_callbacks = [] # keeps callback handles alive
        conf = _lib.rd_kafka_topic_conf_new()
        for k, v in topic_config_dict.items():
            if k == "partitioner":
                self.conf_callbacks.append(
                    _config_handles.topic_conf_set_partitioner_cb(conf, v))
            else:
                errstr = _mk_errstr()
                res = _lib.rd_kafka_topic_conf_set(
                            conf, k, v, errstr, len(errstr))
                if res != _lib.RD_KAFKA_CONF_OK:
                    raise LibrdkafkaException(_ffi.string(errstr))

        self.cdata = _lib.rd_kafka_topic_new(
                         self.kafka_handle.cdata, name, conf)
        if self.cdata == _ffi.NULL:
            raise LibrdkafkaException(_errno2str())

    def __del__(self):
        _lib.rd_kafka_topic_destroy(self.cdata)

    @property
    def name(self):
        return _ffi.string(_lib.rd_kafka_topic_name(self.cdata))

    def metadata(self, timeout_ms=1000):
        return _metadata(self.kafka_handle, topic=self, timeout_ms=timeout_ms)


class KafkaHandle(object):
    topic_type = BaseTopic

    def __init__(self, handle_type, config_dict):
        """
        config_dict -- A dict with keys as per librdkafka's CONFIGURATION.md
        """
        self.config_man = _config_handles.ConfigManager(self, config_dict)
        errstr = _mk_errstr()
        self.cdata = _lib.rd_kafka_new(
                handle_type, self.config_man.pop_config(), errstr, len(errstr))
        if self.cdata == _ffi.NULL:
            raise LibrdkafkaException(_ffi.string(errstr))

    def __del__(self):
        _lib.rd_kafka_destroy(self.cdata)

    def open_topic(self, name, topic_config_dict={}):
        return self.topic_type(name, self, topic_config_dict)

    def metadata(self, all_topics=True, timeout_ms=1000):
        return _metadata(self, all_topics=all_topics, timeout_ms=timeout_ms)

    def poll(self, timeout_ms=1000):
        return _lib.rd_kafka_poll(self.cdata, timeout_ms)


class ProducerTopic(BaseTopic):

    def produce(self, payload, key=None,
                partition=_lib.RD_KAFKA_PARTITION_UA, msg_opaque=None):
        key = key or _ffi.NULL
        msg_opaque = _msg_opaques.get_handle(msg_opaque)
        rv = _lib.rd_kafka_produce(
                 self.cdata, partition, _lib.RD_KAFKA_MSG_F_COPY,
                 payload, len(payload),
                 key, len(key) if key else 0,
                 msg_opaque)
        if rv:
            raise LibrdkafkaException(_errno2str())


class Producer(KafkaHandle):
    topic_type = ProducerTopic

    def __init__(self, config_dict):
        super(Producer, self).__init__(handle_type=_lib.RD_KAFKA_PRODUCER,
                                       config_dict=config_dict)

    def __del__(self):
        # flush the write queues:
        while _lib.rd_kafka_outq_len(self.cdata) > 0:
            # TODO make sure we can break out of here
            self.poll(100)
        super(Producer, self).__del__()


class ConsumerTopic(BaseTopic):
    OFFSET_BEGINNING = QueueReader.OFFSET_BEGINNING
    OFFSET_END = QueueReader.OFFSET_END

    def open_partition(self, partition, start_offset):
        qr = self.kafka_handle.new_queue()
        qr.add_toppar(self, partition, start_offset)
        return qr


class Consumer(KafkaHandle):
    topic_type = ConsumerTopic

    def __init__(self, config_dict):
        super(Consumer, self).__init__(handle_type=_lib.RD_KAFKA_CONSUMER,
                                       config_dict=config_dict)

    def new_queue(self):
        return partition_reader.QueueReader(self)


def _metadata(kafka_handle, all_topics=True, topic=None, timeout_ms=1000):
    """
    Return dict with all information retrievable from librdkafka's Metadata API
    """
    topic = _ffi.NULL if topic is None else topic.cdata
    meta_dp = _ffi.new("const rd_kafka_metadata_t **")
    err = _lib.rd_kafka_metadata(kafka_handle.cdata,
                                 int(all_topics),
                                 topic,
                                 meta_dp,
                                 timeout_ms)
    if err != _lib.RD_KAFKA_RESP_ERR_NO_ERROR:
        raise LibrdkafkaException(_err2str(err))

    d = {}
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
    return d
