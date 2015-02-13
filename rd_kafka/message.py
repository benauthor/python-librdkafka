from headers import lib
from . import msg_opaques, finaliser
from utils import voidp2bytes


class MessageException(Exception):
    pass


class Message(object):
    def __init__(self, cdata, manage_memory=True):
        self.cdata = cdata
        if manage_memory:
            finaliser.register(self, lib.rd_kafka_message_destroy, self.cdata)

    @property
    def partition(self):
        return self.cdata.partition

    @property
    def payload(self):
        return voidp2bytes(self.cdata.payload, self.cdata.len)

    @property
    def key(self):
        return voidp2bytes(self.cdata.key, self.cdata.key_len)

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
            return msg_opaques.from_handle(self.cdata._private)

    def _free_opaque(self):
        self.opaque_freed = True  # prevent crash by subsequent opaque() calls
        msg_opaques.drop_handle(self.cdata._private)
