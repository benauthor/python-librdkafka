""" 
Implements a singleton-interface to consume kafka partitions

We need this because of librdkafka's consumer API, where we are only allowed 1
call to rd_kafka_consume_start() for every rd_kafka_consume_stop() (as per the
docs, and as is obvious because it would mix up offsets of concurrent readers)
"""
import errno

from headers import ffi as _ffi, lib as _lib
from message import Message
from utils import _mk_errstr, _err2str, _errno2str


# TODO store topics by name, not object handle, to avoid circular refs:
open_partitions = set() # partitions that have been "started" in rd_kafka

# NB these must be str, not int:
OFFSET_BEGINNING = 'beginning'
OFFSET_END = 'end'


class PartitionReaderException(Exception):
    pass


def _open_partition(queue, topic, partition, start_offset):
    lock = TopparLock((topic, partition))

    if start_offset == OFFSET_BEGINNING:
        start_offset = _lib.RD_KAFKA_OFFSET_BEGINNING
    elif start_offset == OFFSET_END:
        start_offset = _lib.RD_KAFKA_OFFSET_END
    elif start_offset < 0:
        # pythonistas expect this to be relative to end
        start_offset += _lib.RD_KAFKA_OFFSET_TAIL_BASE

    rv = _lib.rd_kafka_consume_start_queue(
            topic.cdata, partition, start_offset, queue)
    if rv:
        raise PartitionReaderException(
                "rd_kafka_consume_start_queue: " + _errno2str())
    # FIXME we use the following hack to prevent ETIMEDOUT on a consume()
    # call that comes too soon after rd_kafka_consume_start().  Slow!
    topic.kafka_handle.poll()
    return lock


class TopparLock(object):
    """ Hold a lock to prevent concurrent Readers on a topic+partition """
    def __init__(self, toppar):
        if toppar in open_partitions:
            self.toppar = None
            raise PartitionReaderException(
                    "Partition {} open elsewhere!".format(toppar))
        else:
            open_partitions.add(toppar)
            self.toppar = toppar
    
    def __del__(self):
        self.release()

    def __copy__(self):
        raise NotImplementedError("Permitting copies would be confusing")

    def __deepcopy__(self, memo):
        raise NotImplementedError("Permitting copies would be confusing")

    def release(self):
        if self.toppar is None:  # don't run twice!
            return
        open_partitions.remove(self.toppar)
        top, par = self.toppar
        rv = _lib.rd_kafka_consume_stop(top.cdata, par)
        if rv:
            raise PartitionReaderException(
                    "rd_kafka_consume_stop({}, {}): ".format(top, par)
                    + _errno2str())
        self.toppar = None


class Reader(object):
    # exporting for convenience:
    OFFSET_BEGINNING = OFFSET_BEGINNING
    OFFSET_END = OFFSET_END

    def __init__(self, topic, partition, start_offset):
        self.cdata = _lib.rd_kafka_queue_new(topic.kafka_handle.cdata)
        # we mustn't reuse this instance after calling self.close(), as
        # someone else might be reading at that point:
        self.dead = False
        self.lock = _open_partition(self.cdata, topic, partition, start_offset)

    def __del__(self):
        if not self.dead:
            self.close()

    def consume(self, timeout_ms=1000):
        self._check_dead()
        msg = _lib.rd_kafka_consume_queue(self.cdata, timeout_ms)
        if msg == _ffi.NULL:
            if _ffi.errno == errno.ETIMEDOUT:
                return None
            elif _ffi.errno == errno.ENOENT:
                raise PartitionReaderException(
                        "Cannot access '{}'/{}".format(topic, partition))
        elif msg.err == _lib.RD_KAFKA_RESP_ERR_NO_ERROR:
            return Message(msg)
        elif msg.err == _lib.RD_KAFKA_RESP_ERR__PARTITION_EOF:
            # TODO maybe raise StopIteration to distinguish from ETIMEDOUT?
            return None
        else:
            # TODO we should inspect msg.payload here too
            raise PartitionReaderException(_err2str(msg.err))

    def consume_batch(self, max_messages, timeout_ms=1000):
        self._check_dead()
        msg_array = _ffi.new('rd_kafka_message_t* []', max_messages)
        n_out = _lib.rd_kafka_consume_batch_queue(
                    self.cdata, timeout_ms, msg_array, max_messages)
        if n_out == -1:
            raise PartitionReaderException(_errno2str())
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
        self._check_dead()
        opaque_p = _ffi.new_handle(opaque)

        @_ffi.callback('void (rd_kafka_message_t *, void *)')
        def func(msg, opaque):
            callback_func(Message(msg, manage_memory=False),
                          _ffi.from_handle(opaque))

        n_out = _lib.rd_kafka_consume_callback_queue(
                    self.cdata, timeout_ms, func, opaque_p)
        if n_out == -1:
            raise PartitionReaderException(_errno2str())
        else:
            return n_out

    def close(self):
        try:
            if hasattr(self, 'lock'):  # False if we didn't acquire the lock
                self.lock.release()
        finally:
            _lib.rd_kafka_queue_destroy(self.cdata)
            self.dead = True

    def _check_dead(self):
        # TODO it's a design-smell that we're in trouble if we forget to
        # call _check_dead(); probably we should copy topic and/or
        # partition to self and set them None on close().
        if self.dead:
            raise PartitionReaderException(
                    "You called close() on this handle; get a fresh one.")
