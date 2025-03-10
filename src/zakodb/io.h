#ifndef _ZAKODB_IO_H
#define _ZAKODB_IO_H

#include <stdint.h>
#include <stddef.h>

typedef struct zakodb_io_t zakodb_io_t;

#define ZAKODB_IO_SUCCESS 0
#define ZAKODB_IO_FAILURE -1
#define ZAKODB_IO_EOF -2

#define ZAKODB_IO_CSTR_INIT_BUFSIZE 16
#define ZAKODB_IO_READ_BUFSIZE 4096

int zakodb_io_open(const char* path, zakodb_io_t** io);
void zakodb_io_close(zakodb_io_t* io);

int zakodb_io_read_raw(zakodb_io_t* io, size_t n, void* ptr);
int zakodb_io_read_bytes(zakodb_io_t* io, uint16_t* n, uint8_t** bytes);
int zakodb_io_read_cstr(zakodb_io_t* io, char** str);
int zakodb_io_read_int8(zakodb_io_t* io, int8_t* num);
int zakodb_io_read_uint8(zakodb_io_t* io, uint8_t* num);
int zakodb_io_read_int16(zakodb_io_t* io, int16_t* num);
int zakodb_io_read_uint16(zakodb_io_t* io, uint16_t* num);
int zakodb_io_read_int32(zakodb_io_t* io, int32_t* num);
int zakodb_io_read_uint32(zakodb_io_t* io, uint32_t* num);
int zakodb_io_read_int64(zakodb_io_t* io, int64_t* num);
int zakodb_io_read_uint64(zakodb_io_t* io, uint64_t* num);
int zakodb_io_read_float32(zakodb_io_t* io, float* num);
int zakodb_io_read_float64(zakodb_io_t* io, double* num);

int zakodb_io_write_raw(zakodb_io_t* io, size_t n, const void* ptr);
int zakodb_io_write_bytes(zakodb_io_t* io, uint16_t n, const uint8_t* bytes);
int zakodb_io_write_cstr(zakodb_io_t* io, const char* str);
int zakodb_io_write_int8(zakodb_io_t* io, int8_t num);
int zakodb_io_write_uint8(zakodb_io_t* io, uint8_t num);
int zakodb_io_write_int16(zakodb_io_t* io, int16_t num);
int zakodb_io_write_uint16(zakodb_io_t* io, uint16_t num);
int zakodb_io_write_int32(zakodb_io_t* io, int32_t num);
int zakodb_io_write_uint32(zakodb_io_t* io, uint32_t num);
int zakodb_io_write_int64(zakodb_io_t* io, int64_t num);
int zakodb_io_write_uint64(zakodb_io_t* io, uint64_t num);
int zakodb_io_write_float32(zakodb_io_t* io, float num);
int zakodb_io_write_float64(zakodb_io_t* io, double num);

int zakodb_io_seek(zakodb_io_t* io, long off, int whence);
long zakodb_io_tell(zakodb_io_t* io);

void zakodb_io_free_buf(void* ptr);

#endif
