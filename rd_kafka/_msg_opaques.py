"""
Internal module to track cffi handles for python-objects passed as msg_opaques

Implementation notes/laments: this was the simplest implementation (with no
regards for overhead caused) that I could cook up that still satisfies all of
the following:
 * The cffi handle for a given msg_opaque is kept alive from its creation (in
   Producer.produce()) until its final point of utility in dr_msg_cb()
 * Afterwards, handles get cleaned up to prevent our store of handles growing
   boundlessly with messages produced
 * If users pass the same python object-instance as msg_opaque to multiple
   messages, the handle or handles we create for it should be managed
   carefully enough that we do not clean them up prematurely, or clean up the
   wrong one (ie if we choose to hold multiple handles for the same instance)
"""

import uuid

from headers import ffi as _ffi


_handles = {}


def new_handle(msg_opaque):
    if msg_opaque is None:
        return _ffi.NULL
    else:
        guid = uuid.uuid4().int
        handle = _ffi.new_handle((guid, msg_opaque))
        _handles[guid] = handle
        return handle


def _from_handle(cdata_msg_opaque):
    if cdata_msg_opaque == _ffi.NULL:
        return (None, None)
    else:
        return _ffi.from_handle(cdata_msg_opaque)


def from_handle(cdata_msg_opaque):
    guid, msg_opaque = _from_handle(cdata_msg_opaque)
    return msg_opaque


def drop_handle(cdata_msg_opaque):
    guid, msg_opaque = _from_handle(cdata_msg_opaque)
    if guid is not None:
        del _handles[guid]
