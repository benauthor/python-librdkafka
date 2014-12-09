from headers import ffi as _ffi, lib as _lib
from . import _msg_opaques
from utils import _voidp2bytes


class MessageException(Exception):
    pass


class Message(object):
    def __init__(self, cdata, manage_memory=True):
        self.cdata = cdata
        self.manage_memory = manage_memory

    def __del__(self):
        if self.manage_memory:
            _lib.rd_kafka_message_destroy(self.cdata)

    @property
    def payload(self):
        return _voidp2bytes(self.cdata.payload, self.cdata.len)

    @property
    def key(self):
        return _voidp2bytes(self.cdata.key, self.cdata.key_len)

    @property
    def offset(self):
        return self.cdata.offset

    @property
    def opaque(self):
        """
        Return the msg_opaque passed to ProducerTopic.produce()
        """
        if hasattr(self, 'opaque_freed'):
            raise MessageException("As currently implemented, the msg_opaque "
                                   "handle is no longer available after "
                                   "dr_msg_cb() returns.")
        else:
            return _msg_opaques.from_handle(self.cdata._private)

    def _free_opaque(self):
        self.opaque_freed = True  # prevent crash by subsequent opaque() calls
        _msg_opaques.drop_handle(self.cdata._private)
