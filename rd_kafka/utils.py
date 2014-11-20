from headers import ffi as _ffi, lib as _lib


def _mk_errstr():
    # probably way oversized:
    return _ffi.new("char []", 512)


def _err2str(err):
    return _ffi.string(_lib.rd_kafka_err2str(err))


def _errno2str(errno=None):
    """ Look up string error message for errno """
    errno = _ffi.errno if errno is None else errno
    return _err2str(_lib.rd_kafka_errno2err(errno))


def _voidp2bytes(cdata, length):
    return _ffi.buffer(_ffi.cast('const char *', cdata), length)
