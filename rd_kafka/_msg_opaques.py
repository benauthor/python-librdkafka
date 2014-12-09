"""
Internal module to track cffi handles for python-objects passed as msg_opaques

Implementation notes/laments: this was the simplest implementation (with
minimal regard for overhead caused) that I could cook up that still satisfies
all of the following:
 * The cffi handle for a given msg_opaque is kept alive from its creation (in
   Producer.produce()) until its final point of utility in dr_msg_cb()
 * Afterwards, handles get cleaned up to prevent our store of handles growing
   boundlessly with messages produced
 * If users pass the same python object-instance as msg_opaque to multiple
   messages, the handle or handles we create for it should be managed
   carefully enough that we do not clean them up prematurely, or clean up the
   wrong one (ie if we choose to hold multiple handles for the same instance)
 * We can use any python object as a msg_opaque (eg. non-hashable objects too)
"""

import threading
import uuid

from headers import ffi as _ffi


_handles = {}
_lock = threading.Lock()


def get_handle(msg_opaque):
    obj_id = id(msg_opaque)
    with _lock:
        if obj_id in _handles:
            _handles[obj_id][0] += 1
        else:
            _handles[obj_id] = [1, _ffi.new_handle(msg_opaque)]
    return _handles[obj_id][1]


def from_handle(cdata_msg_opaque):
    return _ffi.from_handle(cdata_msg_opaque)


def drop_handle(cdata_msg_opaque):
    obj_id = id(_ffi.from_handle(cdata_msg_opaque))
    with _lock:
        if _handles[obj_id][0] == 1:
            del _handles[obj_id]
        else:
            _handles[obj_id][0] -= 1
