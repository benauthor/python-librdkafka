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

from .headers import lib


__all__ = ["register"]
logger = logging.getLogger(__name__)


registered_objects = {}
""" Keeps alive references for objects that have register()-ed themselves """


_last_remaining = set()
"""
This shall be the very last remaining object in registered_objects

Note the leading underscore here is crucially important: as per the docs at
https://docs.python.org/2/reference/datamodel.html#object.__del__ it ensures
that _last_remaining gets deleted *before* registered_objects.  Were it the
other way around, _last_remaining's finaliser-callback would never run!

Also, why a set()? Because weakref doesn't like object(), that's all.
"""


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
        del registered_objects[id(weakref_obj)]

    ref = weakref.ref(caller_obj, cb)
    registered_objects[id(ref)] = ref


def check_destroyed():
    """ Check if all KafkaHandles have been cleaned up """
    logging.debug("Waiting till all rdkafka handles have been destroyed.")
    if lib.rd_kafka_wait_destroyed(5000):
        logging.error("Timeout in rd_kafka_wait_destroyed; "
                      "unfinalised objs left: {}".format(registered_objects))
    else:
        logging.debug("All rdkafka handles have been destroyed.")


# Under normal circumstances, _last_remaining should be the last object to go,
# and so all the rd_kafka_*_destroy() calls should have been fielded by
# this time.  We now register check_destroyed() as the finaliser for
# _last_remaining, which should make the program wait until those *_destroy()
# tasks have run to completion:
register(_last_remaining, check_destroyed)
