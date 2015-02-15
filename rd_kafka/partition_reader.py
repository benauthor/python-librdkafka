import errno
import logging
import threading

from . import finaliser
from headers import ffi, lib
from message import Message
from utils import err2str, errno2str


# NB these must be str, not int:
OFFSET_BEGINNING = 'beginning'
OFFSET_END = 'end'


logger = logging.getLogger(__name__)


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
        self._cdata = lib.rd_kafka_queue_new(self.kafka_handle.cdata)
        self.partitions = [] # NB only edit in-place; cf close()/_finalise()

        finaliser.register(
                self, QueueReader._finalise, self.partitions, self._cdata)

    @property
    def cdata(self):
        if self._cdata is None:
            raise PartitionReaderException(
                    "You called close() on this handle; get a fresh one.")
            # Setting _cdata to None on close() was necessary in an earlier
            # implementation of this class; probably it would work fine now
            # if we'd do a close() and then some new add_toppar() calls, but
            # it's hard to reason it through, so I've left this at its old
            # behaviour.
        else:
            return self._cdata

    @staticmethod
    def _finalise(partitions, cdata):
        # We should clean-out partitions before destroying the queue handle, so
        # they can be cleanly stopped.  I'm not entirely sure this is
        # sufficient to guarantee ordering of finalisers on every python
        # implementation.  It works on CPython:
        del partitions[:]
        lib.rd_kafka_queue_destroy(cdata)

    def close(self):
        """ Close all partitions """
        # Like self._finalise(), but we don't destroy _cdata here, because
        # there's no way to tell the finaliser not to try destroying it again
        # (which would then segfault).  Note also that we must clean-out
        # self.partitions in-place, because we sent a reference for it to
        # finaliser.register():
        del self.partitions[:]
        self._cdata = None

    def add_toppar(self, topic, partition_id, start_offset):
        """
        Open given topic+partition for reading through this queue

        Raise exception if another reader holds this toppar already
        """
        # TODO sensible default for start_offset
        if self.kafka_handle.cdata != topic.kafka_handle.cdata:
            raise PartitionReaderException(
                    "Topics must derive from same KafkaHandle as queue")
        self.partitions.append(
                ConsumerPartition(topic, partition_id, start_offset, self))
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


class ConsumerPartition(object):
    """
    A partition with a "started" local consumer-queue

    This class controls access to partitions.  For a given Consumer instance
    and topic, only one ConsumerPartition can be held at any time (as per
    librdkafka requirements).  Instantiating another ConsumerPartition for the same
    topic+partition (toppar) will fail.

    The consumer-queue for the toppar will be kept active (active in the sense
    of the rd_kafka_consume_start/consume_stop functions) for as long as the
    ConsumerPartition is alive.
    """
    locked_partitions = set()
    locked_partitions_writelock = threading.Lock()

    def __init__(self, topic, partition_id, start_offset, queue=None):

        self.topic = topic
        self.partition_id = partition_id

        toppar = (topic, partition_id)
        with ConsumerPartition.locked_partitions_writelock:
            if toppar in ConsumerPartition.locked_partitions:
                raise PartitionReaderException(
                        "Partition {} open elsewhere!".format(toppar))
            else:
                ConsumerPartition.locked_partitions.add(toppar)

        # If we got this far, the partition is now locked for us, and we must
        # ensure always to unlock it in due course:
        finaliser.register(self,
                           ConsumerPartition._finalise,
                           topic,
                           partition_id)

        if queue is not None:
            status_code = lib.rd_kafka_consume_start_queue(
                    topic.cdata,
                    partition_id,
                    self._to_rdkafka_offset(start_offset),
                    queue.cdata)
            if status_code:
                raise PartitionReaderException(
                        "rd_kafka_consume_start_queue: " + errno2str())
        else: # TODO we could call the non-queue version of consume_start here
            pass


    @staticmethod
    def _finalise(topic, partition_id):
        """ Remove consumer-queue for toppar and unlock it """
        if lib.rd_kafka_consume_stop(topic.cdata, partition_id):
            logger.error("Error from rd_kafka_consume_stop("
                         "{}, {}): ".format(topic, partition_id)
                         + errno2str())
        with ConsumerPartition.locked_partitions_writelock:
            ConsumerPartition.locked_partitions.remove((topic, partition_id))
        logger.debug("Removed {}:{} from locked_partitions; remaining: "
                     " {}".format(topic,
                                  partition_id,
                                  ConsumerPartition.locked_partitions))

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
