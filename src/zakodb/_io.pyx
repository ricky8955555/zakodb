# cython: profile=True

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t
from libc.stdlib cimport free, malloc

from os import PathLike
from typing import Any

from zakodb.const import ZAKODB_MAGIC, ZAKODB_PYTHON_TYPES
from zakodb.exc import NotAZakoDbError
from zakodb.types import (
    ZakoDbEntry,
    ZakoDbFieldProperty,
    ZakoDbHashedBytes,
    ZakoDbHashMethod,
    ZakoDbMetadata,
    ZakoDbPythonType,
    ZakoDbType,
    ZakoDbTypedValue,
)


cdef extern from "io.c":
    pass

cdef extern from "io.h":
    cdef int ZAKODB_IO_SUCCESS
    cdef int ZAKODB_IO_FAILURE
    cdef int ZAKODB_IO_EOF

    struct zakodb_io_t:
        pass

    int zakodb_io_open(const char* path, zakodb_io_t** io)
    void zakodb_io_close(zakodb_io_t* io)

    int zakodb_io_read_raw(zakodb_io_t* io, size_t n, void* ptr)
    int zakodb_io_read_bytes(zakodb_io_t* io, uint16_t* n, uint8_t** bytes)
    int zakodb_io_read_cstr(zakodb_io_t* io, char** str)
    int zakodb_io_read_int8(zakodb_io_t* io, int8_t* num)
    int zakodb_io_read_uint8(zakodb_io_t* io, uint8_t* num)
    int zakodb_io_read_int16(zakodb_io_t* io, int16_t* num)
    int zakodb_io_read_uint16(zakodb_io_t* io, uint16_t* num)
    int zakodb_io_read_int32(zakodb_io_t* io, int32_t* num)
    int zakodb_io_read_uint32(zakodb_io_t* io, uint32_t* num)
    int zakodb_io_read_int64(zakodb_io_t* io, int64_t* num)
    int zakodb_io_read_uint64(zakodb_io_t* io, uint64_t* num)
    int zakodb_io_read_float32(zakodb_io_t* io, float* num)
    int zakodb_io_read_float64(zakodb_io_t* io, double* num)

    int zakodb_io_write_raw(zakodb_io_t* io, size_t n, const void* ptr)
    int zakodb_io_write_bytes(zakodb_io_t* io, uint16_t n, const uint8_t* bytes)
    int zakodb_io_write_cstr(zakodb_io_t* io, const char* str)
    int zakodb_io_write_int8(zakodb_io_t* io, int8_t num)
    int zakodb_io_write_uint8(zakodb_io_t* io, uint8_t num)
    int zakodb_io_write_int16(zakodb_io_t* io, int16_t num)
    int zakodb_io_write_uint16(zakodb_io_t* io, uint16_t num)
    int zakodb_io_write_int32(zakodb_io_t* io, int32_t num)
    int zakodb_io_write_uint32(zakodb_io_t* io, uint32_t num)
    int zakodb_io_write_int64(zakodb_io_t* io, int64_t num)
    int zakodb_io_write_uint64(zakodb_io_t* io, uint64_t num)
    int zakodb_io_write_float32(zakodb_io_t* io, float num)
    int zakodb_io_write_float64(zakodb_io_t* io, double num)

    int zakodb_io_seek(zakodb_io_t* io, long off, int whence)
    long zakodb_io_tell(zakodb_io_t* io)

    void zakodb_io_free_buf(void* ptr)


# cache type enum to increase speed
cdef uint8_t ZAKODB_TYPE_INT8 = <uint8_t>ZakoDbType.INT8
cdef uint8_t ZAKODB_TYPE_UINT8 = <uint8_t>ZakoDbType.UINT8
cdef uint8_t ZAKODB_TYPE_INT16 = <uint8_t>ZakoDbType.INT16
cdef uint8_t ZAKODB_TYPE_UINT16 = <uint8_t>ZakoDbType.UINT16
cdef uint8_t ZAKODB_TYPE_INT32 = <uint8_t>ZakoDbType.INT32
cdef uint8_t ZAKODB_TYPE_UINT32 = <uint8_t>ZakoDbType.UINT32
cdef uint8_t ZAKODB_TYPE_INT64 = <uint8_t>ZakoDbType.INT64
cdef uint8_t ZAKODB_TYPE_UINT64 = <uint8_t>ZakoDbType.UINT64
cdef uint8_t ZAKODB_TYPE_BYTES = <uint8_t>ZakoDbType.BYTES
cdef uint8_t ZAKODB_TYPE_HASHED_BYTES = <uint8_t>ZakoDbType.HASHED_BYTES
cdef uint8_t ZAKODB_TYPE_FLOAT32 = <uint8_t>ZakoDbType.FLOAT32
cdef uint8_t ZAKODB_TYPE_FLOAT64 = <uint8_t>ZakoDbType.FLOAT64


cdef void _zakodb_io_check_retval(int retval):
    if retval == ZAKODB_IO_SUCCESS:
        return
    if retval == ZAKODB_IO_FAILURE:
        raise RuntimeError
    if retval == ZAKODB_IO_EOF:
        raise EOFError


cdef class ZakoDbPrimitiveIO:
    cdef zakodb_io_t* c_io

    def __init__(self, path: Any):
        if isinstance(path, PathLike):
            path = path.__fspath__()
        if isinstance(path, str):
            path = path.encode()
        elif not isinstance(path, bytes):
            raise TypeError

        _zakodb_io_check_retval(zakodb_io_open(<const char*>path, &self.c_io))

    def __dealloc__(self):
        self.close()

    def close(self):
        zakodb_io_close(self.c_io)
        self.c_io = NULL

    def read_raw(self, size_t n):
        cdef char* buf = <char*>malloc(n)
        cdef bytes py_bytes

        try:
            _zakodb_io_check_retval(zakodb_io_read_raw(self.c_io, n, <void*>buf))
            py_bytes = buf[:n]
        finally:
            free(<void*>buf)

        return py_bytes

    def read_bytes(self):
        cdef uint16_t n
        cdef uint8_t* data
        cdef bytes py_bytes

        _zakodb_io_check_retval(zakodb_io_read_bytes(self.c_io, &n, &data))

        try:
            py_bytes = (<char*>data)[:n]
        finally:
            zakodb_io_free_buf(<void*>data)

        return py_bytes

    def read_cstr(self):
        cdef char* string
        cdef str py_str

        _zakodb_io_check_retval(zakodb_io_read_cstr(self.c_io, &string))

        try:
            py_str = (<bytes>string).decode("ascii")
        finally:
            zakodb_io_free_buf(<void*>string)

        return py_str

    def read_int8(self):
        cdef int8_t num

        _zakodb_io_check_retval(zakodb_io_read_int8(self.c_io, &num))
        return num

    def read_uint8(self):
        cdef uint8_t num

        _zakodb_io_check_retval(zakodb_io_read_uint8(self.c_io, &num))
        return num

    def read_int16(self):
        cdef int16_t num

        _zakodb_io_check_retval(zakodb_io_read_int16(self.c_io, &num))
        return num

    def read_uint16(self):
        cdef uint16_t num

        _zakodb_io_check_retval(zakodb_io_read_uint16(self.c_io, &num))
        return num

    def read_int32(self):
        cdef int32_t num

        _zakodb_io_check_retval(zakodb_io_read_int32(self.c_io, &num))
        return num

    def read_uint32(self):
        cdef uint32_t num

        _zakodb_io_check_retval(zakodb_io_read_uint32(self.c_io, &num))
        return num

    def read_int64(self):
        cdef int64_t num

        _zakodb_io_check_retval(zakodb_io_read_int64(self.c_io, &num))
        return num

    def read_uint64(self):
        cdef uint64_t num

        _zakodb_io_check_retval(zakodb_io_read_uint64(self.c_io, &num))
        return num

    def read_float32(self):
        cdef float num

        _zakodb_io_check_retval(zakodb_io_read_float32(self.c_io, &num))
        return num

    def read_float64(self):
        cdef double num

        _zakodb_io_check_retval(zakodb_io_read_float64(self.c_io, &num))
        return num

    def write_raw(self, bytes data):
        _zakodb_io_check_retval(
            zakodb_io_write_raw(self.c_io, <size_t>len(data), <void*><const char*>data)
        )

    def write_bytes(self, bytes data):
        n = len(data)

        if n > 2 ** 16:
            raise OverflowError

        _zakodb_io_check_retval(
            zakodb_io_write_bytes(self.c_io, <uint16_t>n, <const uint8_t*><const char*>data)
        )

    def write_cstr(self, str data):
        encoded = data.encode("ascii")
        _zakodb_io_check_retval(zakodb_io_write_cstr(self.c_io, <const char*>encoded))

    def write_int8(self, int8_t num):
        _zakodb_io_check_retval(zakodb_io_write_int8(self.c_io, num))

    def write_uint8(self, uint8_t num):
        _zakodb_io_check_retval(zakodb_io_write_uint8(self.c_io, num))

    def write_int16(self, int16_t num):
        _zakodb_io_check_retval(zakodb_io_write_int16(self.c_io, num))

    def write_uint16(self, uint16_t num):
        _zakodb_io_check_retval(zakodb_io_write_uint16(self.c_io, num))

    def write_int32(self, int32_t num):
        _zakodb_io_check_retval(zakodb_io_write_int32(self.c_io, num))

    def write_uint32(self, uint32_t num):
        _zakodb_io_check_retval(zakodb_io_write_uint32(self.c_io, num))

    def write_int64(self, int64_t num):
        _zakodb_io_check_retval(zakodb_io_write_int64(self.c_io, num))

    def write_uint64(self, uint64_t num):
        _zakodb_io_check_retval(zakodb_io_write_uint64(self.c_io, num))

    def write_float32(self, float num):
        _zakodb_io_check_retval(zakodb_io_write_float32(self.c_io, num))

    def write_float64(self, double num):
        _zakodb_io_check_retval(zakodb_io_write_float64(self.c_io, num))

    def seek(self, long off, int whence):
        _zakodb_io_check_retval(zakodb_io_seek(self.c_io, off, whence))

    def tell(self):
        ret = zakodb_io_tell(self.c_io)

        if ret == -1:
            raise RuntimeError

        return ret


cdef class ZakoDbIO:
    cdef ZakoDbPrimitiveIO io

    def __init__(self, path: Any):
        self.io = ZakoDbPrimitiveIO(path)

    @property
    def underlying(self):
        return self.io

    def close(self):
        self.io.close()

    def expect_magic(self):
        try:
            read = self.io.read_raw(<size_t>len(ZAKODB_MAGIC))
        except EOFError:
            raise NotAZakoDbError

        if read != ZAKODB_MAGIC:
            raise NotAZakoDbError

    def write_magic(self):
        self.io.write_raw(ZAKODB_MAGIC)

    def write_hashed_bytes(self, obj: ZakoDbHashedBytes):
        self.io.write_uint64(<uint64_t>obj.hash)
        self.io.write_bytes(obj.content)

    def read_hashed_bytes(self):
        hash = self.io.read_uint64()
        content = self.io.read_bytes()
        obj = ZakoDbHashedBytes(hash=hash, content=content)
        return obj

    def read_metadata(self):
        version = self.io.read_uint8()

        if version != 1:
            raise NotImplementedError

        hash_method = ZakoDbHashMethod(self.io.read_uint8())
        field_count = self.io.read_uint8()
        field_props: list[ZakoDbFieldProperty] = []

        for _ in range(field_count):
            field_name = self.io.read_cstr()
            field_type = ZakoDbType(self.io.read_uint8())
            field_prop = ZakoDbFieldProperty(name=field_name, type=field_type)
            field_props.append(field_prop)

        extra_field_count = self.io.read_uint8()
        extra_fields: dict[str, ZakoDbTypedValue] = {}

        for _ in range(extra_field_count):
            extra_field_name = self.io.read_cstr()
            extra_field_type = ZakoDbType(self.io.read_uint8())
            extra_field_value = self.read_value(extra_field_type)
            typed_extra_field = ZakoDbTypedValue(type=extra_field_type, value=extra_field_value)
            extra_fields[extra_field_name] = typed_extra_field

        metadata = ZakoDbMetadata(
            version=version,
            hash_method=hash_method,
            field_props=tuple(field_props),
            extra_fields=extra_fields,
        )

        return metadata

    def write_metadata(self, metadata: ZakoDbMetadata):
        self.io.write_uint8(<uint8_t>metadata.version)
        self.io.write_uint8(<uint8_t>metadata.hash_method)

        field_count = len(metadata.field_props)
        self.io.write_uint8(<uint8_t>field_count)

        for field_prop in metadata.field_props:
            self.io.write_cstr(field_prop.name)
            self.write_type(field_prop.type)

        extra_field_count = len(metadata.extra_fields)
        self.io.write_uint8(<uint8_t>extra_field_count)

        for extra_field_name, extra_field_value in metadata.extra_fields.items():
            self.io.write_cstr(extra_field_name)
            self.write_typed_value(extra_field_value)

    @staticmethod
    def check_type(value: ZakoDbPythonType, typ: ZakoDbType):
        if not isinstance(value, ZAKODB_PYTHON_TYPES[typ]):
            raise TypeError
        return value

    def write_type(self, typ: ZakoDbType):
        self.io.write_uint8(<uint8_t>typ)

    def write_value(self, value: ZakoDbPythonType, typ: ZakoDbType):
        self.check_type(value, typ)
        self.write_value_unchecked(value, typ)

    def write_value_unchecked(self, value: Any, typ: ZakoDbType):
        if typ == ZAKODB_TYPE_INT8:
            return self.io.write_int8(value)
        if typ == ZAKODB_TYPE_UINT8:
            return self.io.write_uint8(value)
        if typ == ZAKODB_TYPE_INT16:
            return self.io.write_int16(value)
        if typ == ZAKODB_TYPE_UINT16:
            return self.io.write_uint16(value)
        if typ == ZAKODB_TYPE_INT32:
            return self.io.write_int32(value)
        if typ == ZAKODB_TYPE_UINT32:
            return self.io.write_uint32(value)
        if typ == ZAKODB_TYPE_INT64:
            return self.io.write_int64(value)
        if typ == ZAKODB_TYPE_UINT64:
            return self.io.write_uint64(value)
        if typ == ZAKODB_TYPE_BYTES:
            return self.io.write_bytes(value)
        if typ == ZAKODB_TYPE_HASHED_BYTES:
            return self.write_hashed_bytes(value)
        if typ == ZAKODB_TYPE_FLOAT32:
            return self.io.write_float32(value)
        if typ == ZAKODB_TYPE_FLOAT64:
            return self.io.write_float64(value)

        raise NotImplementedError

    def write_typed_value(self, value: ZakoDbTypedValue):
        self.check_type(value.value, value.type)
        self.write_type(value.type)
        self.write_value(value.value, value.type)

    def write_typed_value_unchecked(self, value: ZakoDbTypedValue):
        self.write_type(value.type)
        self.write_value(value.value, value.type)

    def read_value(self, typ: ZakoDbType):
        if typ == ZAKODB_TYPE_INT8:
            return self.io.read_int8()
        if typ == ZAKODB_TYPE_UINT8:
            return self.io.read_uint8()
        if typ == ZAKODB_TYPE_INT16:
            return self.io.read_int16()
        if typ == ZAKODB_TYPE_UINT16:
            return self.io.read_uint16()
        if typ == ZAKODB_TYPE_INT32:
            return self.io.read_int32()
        if typ == ZAKODB_TYPE_UINT32:
            return self.io.read_uint32()
        if typ == ZAKODB_TYPE_INT64:
            return self.io.read_int64()
        if typ == ZAKODB_TYPE_UINT64:
            return self.io.read_uint64()
        if typ == ZAKODB_TYPE_BYTES:
            return self.io.read_bytes()
        if typ == ZAKODB_TYPE_HASHED_BYTES:
            return self.read_hashed_bytes()
        if typ == ZAKODB_TYPE_FLOAT32:
            return self.io.read_float32()
        if typ == ZAKODB_TYPE_FLOAT64:
            return self.io.read_float64()

        raise NotImplementedError

    def write_entry(
        self, entry: ZakoDbEntry, field_props: tuple[ZakoDbFieldProperty, ...]
    ):
        fields_with_types: list[tuple[ZakoDbPythonType, ZakoDbType]] = []

        for field_prop in field_props:
            fields_with_types.append(
                (
                    self.check_type(entry[field_prop.name], field_prop.type),
                    field_prop.type,
                )
            )

        for value, typ in fields_with_types:
            self.write_value_unchecked(value, typ)

    def read_entry(self, field_props: tuple[ZakoDbFieldProperty, ...]):
        fields: ZakoDbEntry = {}

        for field_prop in field_props:
            fields[field_prop.name] = self.read_value(field_prop.type)

        return fields
