"""
Helpers to ensure we unwind librdkafka state correctly on program exit

It is very difficult (impossible?) to have python (or CPython, in any case)
call the __del__() destructor methods reliably - especially for objects defined
at module scope.  There's a great writeup on avoiding __del__s altogether, at
http://code.activestate.com/recipes/577242-calling-c-level-finalizers-without-__del__/
and the following borrows more than a few ideas from it.

Also, we needed to be able to call rd_kafka_wait_destroyed(), to ensure correct
unwinding of any state remaining in background threads - and initially relied
on the atexit library for this.  However, as it turns out, atexit-registered
functions run before module-scope objects are cleaned up, and so
rd_kafka_wait_destroyed() timed-out waiting for the last KafkaHandles to die.
We now handle this below instead.
"""
import logging
import weakref


logger = logging.getLogger(__name__)
_weakref_objects = {}


def register(caller_obj, callback, *callback_args, **callback_kwargs):
    """
    Register finaliser-callback, to call when caller_obj is garbage-collected

    The provided callback will be called with the provided args and kwargs.  Be
    very careful not to (accidentally) close-over any references to caller_obj
    in either callback or any args/kwargs: if this happens, caller_obj shall
    never be garbage-collected!
    """
    # obtain repr() outside the function-def, to avoid closing-over caller_obj:
    debugging_repr = repr(caller_obj)

    def cb(weakref_obj):
        logger.debug("Finalising " + debugging_repr)
        callback(*callback_args, **callback_kwargs)
        del _weakref_objects[id(weakref_obj)]

    ref = weakref.ref(caller_obj, cb)
    _weakref_objects[id(ref)] = ref
