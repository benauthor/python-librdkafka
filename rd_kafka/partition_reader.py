import errno
import threading

from headers import ffi, lib
from message import Message
from utils import err2str, errno2str


# NB these must be str, not int:
OFFSET_BEGINNING = 'beginning'
OFFSET_END = 'end'


class PartitionReaderException(Exception):
    pass


class QueueReader(object):
    """
    Wrapper for librdkafka's Queue API

    Usage:
    Create a new QueueReader instance like so:
        r = consumer_instance.new_queue()
        r.add_toppar(topic_a, partition_x, offset_n)
        r.add_toppar(topic_b, partition_y, offset_m)
        r.add_toppar(... etc etc ...)
    where topic_a may be the same or a different topic than topic_b, etc etc,
    as long as all Topic instances were derived from the same Consumer
    instance.  Then:
        msg = r.consume() # or...
        msg_list = r.consume_batch(max_messages=100) # or...
        number_of_msgs = r.consume_callback(your_callback_func) # recommended

    Implementation notes:
    This exposes all of the *_queue() functionality provided by librdkafka;
    cf. https://github.com/edenhill/librdkafka/wiki/Consuming-from-multiple-topics-partitions-from-a-single-thread

    Currently, we have for simplicity assumed that even users who need to
    access only a single topic+partition are happy to consume that via an
    rdkafka-queue; that is, the non-*_queue() equivalents are not available.
    """
    # exporting for convenience:
    OFFSET_BEGINNING = OFFSET_BEGINNING
    OFFSET_END = OFFSET_END

    def __init__(self, kafka_handle):
        self.kafka_handle = kafka_handle
        self.toppars = set() # {(topic, partition_id), }
        self._cdata = lib.rd_kafka_queue_new(self.kafka_handle.cdata)

    def __del__(self):
        self.close()

    @property
    def cdata(self):
        if self._cdata is None:
            raise PartitionReaderException(
                    "You called close() on this handle; get a fresh one.")
        else:
            return self._cdata

    def add_toppar(self, topic, partition_id, start_offset):
        """
        Open given topic+partition for reading through this queue

        Raise exception if another reader holds this toppar already
        """
        # TODO sensible default for start_offset
        if self.kafka_handle.cdata != topic.kafka_handle.cdata:
            raise PartitionReaderException(
                    "Topics must derive from same KafkaHandle as queue")

        self.kafka_handle.toppar_manager.claim(topic.name, partition_id, self)
        self.toppars.add((topic, partition_id))

        if lib.rd_kafka_consume_start_queue(
                topic.cdata,
                partition_id,
                self._to_rdkafka_offset(start_offset),
                self.cdata):
            raise PartitionReaderException(
                    "rd_kafka_consume_start_queue: " + errno2str())
        # FIXME we use the following hack to prevent ETIMEDOUT on a consume()
        # call that comes too soon after rd_kafka_consume_start().  It's slow
        # and it bothers me that I don't get why it's needed.
        topic.kafka_handle.poll()

    def consume(self, timeout_ms=1000):
        msg = lib.rd_kafka_consume_queue(self.cdata, timeout_ms)
        if msg == ffi.NULL:
            if ffi.errno == errno.ETIMEDOUT:
                return None
            elif ffi.errno == errno.ENOENT:
                raise PartitionReaderException(
                        "Got ENOENT while trying to read from queue")
        elif msg.err == lib.RD_KAFKA_RESP_ERR_NO_ERROR:
            return Message(msg)
        elif msg.err == lib.RD_KAFKA_RESP_ERR__PARTITION_EOF:
            # TODO maybe raise StopIteration to distinguish from ETIMEDOUT?
            return None
        else:
            # TODO we should inspect msg.payload here too
            raise PartitionReaderException(err2str(msg.err))

    def consume_batch(self, max_messages, timeout_ms=1000):
        msg_array = ffi.new('rd_kafka_message_t* []', max_messages)
        n_out = lib.rd_kafka_consume_batch_queue(
                    self.cdata, timeout_ms, msg_array, max_messages)
        if n_out == -1:
            raise PartitionReaderException(errno2str())
        else:
            # TODO filter out error messages; eg. sometimes the last
            # message has no payload, but error flag 'No more messages'
            return map(Message, msg_array[0:n_out])

    def consume_callback(self, callback_func, opaque=None, timeout_ms=1000):
        """
        Execute callback function on consumed messages

        The callback function should accept two args: a message.Message,
        and any python object you wish to pass in the 'opaque' above.
        After timeout_ms, return the number of messages consumed.
        """
        opaque_p = ffi.new_handle(opaque)

        @ffi.callback('void (rd_kafka_message_t *, void *)')
        def func(msg, opaque):
            callback_func(Message(msg, manage_memory=False),
                          ffi.from_handle(opaque))

        n_out = lib.rd_kafka_consume_callback_queue(
                    self.cdata, timeout_ms, func, opaque_p)
        if n_out == -1:
            raise PartitionReaderException(errno2str())
        else:
            return n_out

    def close(self):
        try:
            while self.toppars: # stop reading to queue and release each:
                top, par = self.toppars.pop()
                if lib.rd_kafka_consume_stop(top.cdata, par):
                    raise PartitionReaderException(
                            "rd_kafka_consume_stop({}, {}): ".format(top, par)
                            + errno2str())
                self.kafka_handle.toppar_manager.release(top.name, par, self)
        finally:
            if self._cdata is not None:
                lib.rd_kafka_queue_destroy(self.cdata)
            self._cdata = None

    @staticmethod
    def _to_rdkafka_offset(start_offset):
        if start_offset == OFFSET_BEGINNING:
            start_offset = lib.RD_KAFKA_OFFSET_BEGINNING
        elif start_offset == OFFSET_END:
            start_offset = lib.RD_KAFKA_OFFSET_END
        elif start_offset < 0:
            # pythonistas expect this to be relative to end
            start_offset += lib.RD_KAFKA_OFFSET_TAIL_BASE
        return start_offset


class TopparManager(object):
    """
    Helper class for Consumer that keeps track of open topic+partitions

    We need this because of librdkafka's consumer API, where we are only
    allowed 1 call to rd_kafka_consume_start_queue() for every
    rd_kafka_consume_stop() (as per the docs, and as is obvious because it
    would mix up offsets of concurrent readers)
    """
    def __init__(self):
        self.toppar_owner_map = {}
        self.lock = threading.Lock()

    def claim(self, topic_name, partition_id, requester):
        """
        Reserve topic+partition for requester

        Raise exception if toppar is already reserved
        """
        toppar = (topic_name, partition_id)
        with self.lock:
            if toppar in self.toppar_owner_map:
                raise PartitionReaderException(
                        "Partition {} open elsewhere!".format(toppar))
            else:
                self.toppar_owner_map[toppar] = id(requester)

    def release(self, topic_name, partition_id, requester):
        """
        Release toppar if owned by requester

        It should be safe to call this repeatedly
        """
        toppar = (topic_name, partition_id)
        with self.lock:
            m = self.toppar_owner_map
            if toppar in m and m[toppar] == id(requester):
                del m[toppar]
            else:
                logger.warning("Release denied for toppar {}".format(toppar))
