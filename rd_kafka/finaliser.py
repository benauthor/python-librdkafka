"""
Helpers to ensure we unwind librdkafka state correctly on program exit

It is very difficult (impossible?) to have python (or CPython, in any case)
call the __del__() destructor methods reliably - especially for objects defined
at module scope.  There's a great writeup on avoiding __del__s altogether, at
http://code.activestate.com/recipes/577242-calling-c-level-finalizers-without-__del__/
and the following borrows more than a few ideas from it.
"""
import atexit
from collections import OrderedDict
import logging
import weakref

from .headers import lib


__all__ = ["register"]
logger = logging.getLogger(__name__)


registered_objects = OrderedDict()
""" Keeps alive references for objects that have register()-ed themselves """


def register(caller_obj, callback, *callback_args, **callback_kwargs):
    """
    Register finaliser-callback, to call when caller_obj is garbage-collected

    The provided callback will be called with the provided args and kwargs.  Be
    very careful not to (accidentally) close-over any references to caller_obj
    in either callback or any args/kwargs: if this happens, caller_obj shall
    never be garbage-collected!

    Note that the finaliser callback provided *must* be safe to call *before*
    caller_obj is gc'd: at program exit, we forcibly call all finalisers (see
    force_all_finalisers() for details)
    """
    # obtain repr() outside the function-def, to avoid closing-over caller_obj:
    debugging_repr = repr(caller_obj)

    def cb(weakref_obj=None):
        logger.debug("Finalising " + debugging_repr)
        callback(*callback_args, **callback_kwargs)
        if weakref_obj is not None:
            del registered_objects[id(weakref_obj)]

    ref = weakref.ref(caller_obj, cb)
    registered_objects[id(ref)] = (ref, cb)


def check_destroyed():
    """ Check if all KafkaHandles have been cleaned up """
    logging.debug("Waiting till all rdkafka handles have been destroyed.")
    if lib.rd_kafka_wait_destroyed(5000):
        logging.error("Timed-out in rd_kafka_wait_destroyed.")
    else:
        logging.debug("All rdkafka handles have been destroyed.")


@atexit.register
def force_all_finalisers():
    while registered_objects:
        # We'll pop our finalisers in LIFO order, which should be
        # sufficiently orderly with our simple object inter-dependencies
        ref_id, (ref, finaliser_callback) = registered_objects.popitem()
        finaliser_callback()
    check_destroyed()
