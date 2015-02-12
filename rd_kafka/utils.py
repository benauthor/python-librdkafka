from headers import ffi, lib


def _mk_errstr():
    # probably way oversized:
    return ffi.new("char []", 512)


def _err2str(err):
    return ffi.string(lib.rd_kafka_err2str(err))


def _errno2str(errno=None):
    """ Look up string error message for errno """
    errno = ffi.errno if errno is None else errno
    return _err2str(lib.rd_kafka_errno2err(errno))


def _voidp2bytes(cdata, length):
    return ffi.buffer(ffi.cast('const char *', cdata), length)
