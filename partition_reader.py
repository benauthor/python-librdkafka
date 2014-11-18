""" 
Implements a singleton-interface to consume kafka partitions

We need this because of librdkafka's consumer API, where we are only allowed 1
call to rd_kafka_consume_start() for every rd_kafka_consume_stop() (as per the
docs, and as is obvious because it would mix up offsets of concurrent readers)
"""
import errno

from headers import ffi as _ffi, lib as _lib


# TODO store topics by name, not object handle, to avoid circular refs:
open_partitions = set() # partitions that have been "started" in rd_kafka


class PartitionReaderException(Exception):
    pass


def _open(topic, partition, start_offset, default_timeout_ms):
    key = topic, partition
    if key in open_partitions:
        raise PartitionReaderException("Partition {} open elsewhere!".format(key))
    rv = _lib.rd_kafka_consume_start(topic.cdata, partition, start_offset)
    if rv:
        raise PartitionReaderException("rd_kafka_consume_start: " + _errno2str())
    open_partitions.add(key)


def open(topic, partition, start_offset, default_timeout_ms=0):
    """ """
    _open(topic, partition, start_offset, default_timeout_ms)

    class Reader(object):
        """ """
        def __init__(self):
            # we mustn't reuse this instance after calling self.close(), as
            # someone else might be reading at that point:
            self.dead = False

        def __del__(self):
            self.close()

        def consume(self, timeout_ms=None):
            if self.dead:
                raise PartitionReaderException(
                        "You called close() on this handle; get a fresh one.")
            msg = _lib.rd_kafka_consume(
                    topic.cdata, partition,
                    default_timeout_ms if timeout_ms is None else timeout_ms)
            if msg == _ffi.NULL:
                if _ffi.errno == errno.ETIMEDOUT:
                    return None
                elif _ffi.errno == errno.ENOENT:
                    raise PartitionReaderException(
                            "Cannot access '{}'/{}".format(topic, partition))
            else:
                return msg # TODO wrap into class, for garbage-collection!
 
        def seek(self, offset):
            self._close() # must throw KeyError if we called self.close()
            _open(topic, partition, offset, default_timeout_ms)
        
        def close(self):
            self.dead = True
            self._close()

        def _close(self):
            # NB seek() relies on us using remove(), not discard():
            open_partitions.remove((topic, partition))
            rv = _lib.rd_kafka_consume_stop(topic.cdata, partition)
            if rv:
                raise PartitionReaderException(
                    "rd_kafka_consume_stop({}, {}): ".format(topic, partition)
                    + _errno2str())

    return Reader()
