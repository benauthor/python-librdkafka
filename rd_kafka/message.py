from headers import ffi as _ffi, lib as _lib


def _voidp2bytes(cdata, length):
    return _ffi.buffer(_ffi.cast('const char *', cdata), length)


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
