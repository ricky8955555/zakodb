#ifndef __STDC_IEC_559__
#error The following Programm only supports float operations using the IEEE 754 Standard
#endif

#include "io.h"

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct zakodb_io_t {
    FILE* fp;
    char  buf[BUFSIZ];
};

int zakodb_io_open(const char* path, zakodb_io_t** io) {
    FILE* fp;

    fp = fopen(path, "r+");
    if (fp == NULL) fp = fopen(path, "w+");
    if (fp == NULL) return ZAKODB_IO_FAILURE;

    *io = malloc(sizeof(zakodb_io_t));
    (*io)->fp = fp;
    setbuf(fp, (*io)->buf);

    return ZAKODB_IO_SUCCESS;
}

void zakodb_io_close(zakodb_io_t* io) {
    if (io == NULL) return;
    if (io->fp != NULL) fclose(io->fp);
    free(io);
}

int zakodb_io_read_raw(zakodb_io_t* io, size_t n, void* ptr) {
    size_t nread;

    nread = fread(ptr, 1, n, io->fp);
    if (nread != n) return ZAKODB_IO_EOF;

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_read_bytes(zakodb_io_t* io, uint16_t* n, uint8_t** bytes) {
    int      ret;
    uint8_t* buf;

    ret = zakodb_io_read_uint16(io, n);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

    buf = malloc(*n);
    if (buf == NULL) return ZAKODB_IO_FAILURE;

    ret = zakodb_io_read_raw(io, *n, buf);
    if (ret != ZAKODB_IO_SUCCESS) {
        free(buf);
        return ret;
    }

    *bytes = buf;

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_read_cstr(zakodb_io_t* io, char** str) {
    char*  buf;
    int    ch;

    size_t len;
    size_t cap;

    len = 0;
    cap = ZAKODB_IO_CSTR_INIT_BUFSIZE;

    buf = malloc(sizeof(char) * cap);
    if (buf == NULL) return ZAKODB_IO_FAILURE;

    while (1) {
        ch = fgetc(io->fp);

        if (ch == EOF) {
            *str = buf;
            return ZAKODB_IO_EOF;
        }

        if (len == cap) {
            cap = cap / 2 * 3;
            buf = realloc(buf, sizeof(char) * cap);

            if (buf == NULL) return ZAKODB_IO_FAILURE;
        }

        buf[len++] = ch;

        if (ch == '\0') {
            *str = buf;
            return ZAKODB_IO_SUCCESS;
        }
    }
}

int zakodb_io_read_int8(zakodb_io_t* io, int8_t* num) {
    int     ret;
    uint8_t raw;

    ret = zakodb_io_read_uint8(io, &raw);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

    *num = (int8_t)raw;

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_read_uint8(zakodb_io_t* io, uint8_t* num) {
    int     ret;
    uint8_t bytes[1];

    ret = zakodb_io_read_raw(io, 1, bytes);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

    *num = bytes[0];

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_read_int16(zakodb_io_t* io, int16_t* num) {
    int      ret;
    uint16_t raw;

    ret = zakodb_io_read_uint16(io, &raw);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

    *num = (int16_t)raw;

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_read_uint16(zakodb_io_t* io, uint16_t* num) {
    int     ret;
    uint8_t bytes[2];

    ret = zakodb_io_read_raw(io, 2, bytes);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

    *num = ((uint16_t)bytes[0] << 8) + (uint16_t)bytes[1];

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_read_int32(zakodb_io_t* io, int32_t* num) {
    int      ret;
    uint32_t raw;

    ret = zakodb_io_read_uint32(io, &raw);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

    *num = (int32_t)raw;

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_read_uint32(zakodb_io_t* io, uint32_t* num) {
    int     ret;
    uint8_t bytes[4];

    ret = zakodb_io_read_raw(io, 4, bytes);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

    *num = ((uint32_t)bytes[0] << 24) + ((uint32_t)bytes[1] << 16) +
           ((uint32_t)bytes[2] << 8) + (uint32_t)bytes[3];

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_read_int64(zakodb_io_t* io, int64_t* num) {
    int      ret;
    uint64_t raw;

    ret = zakodb_io_read_uint64(io, &raw);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

    *num = (int64_t)raw;

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_read_uint64(zakodb_io_t* io, uint64_t* num) {
    int     ret;
    uint8_t bytes[8];

    ret = zakodb_io_read_raw(io, 8, bytes);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

    *num = ((uint64_t)bytes[0] << 56) + ((uint64_t)bytes[1] << 48) +
           ((uint64_t)bytes[2] << 40) + ((uint64_t)bytes[3] << 32) +
           ((uint64_t)bytes[4] << 24) + ((uint64_t)bytes[5] << 16) +
           ((uint64_t)bytes[6] << 8) + (uint64_t)bytes[7];

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_read_float32(zakodb_io_t* io, float* num) {
    int ret;

    union {
        float   num;
        uint8_t bytes[4];
    } data;

    uint8_t* bytes;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    uint8_t revbytes[4];
    bytes = revbytes;
#else
    bytes = data.bytes;
#endif

    ret = zakodb_io_read_raw(io, 4, bytes);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    data.bytes[0] = revbytes[3];
    data.bytes[1] = revbytes[2];
    data.bytes[2] = revbytes[1];
    data.bytes[3] = revbytes[0];
#endif

    *num = data.num;

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_read_float64(zakodb_io_t* io, double* num) {
    int ret;

    union {
        double  num;
        uint8_t bytes[8];
    } data;

    uint8_t* bytes;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    uint8_t revbytes[8];
    bytes = revbytes;
#else
    bytes = data.bytes;
#endif

    ret = zakodb_io_read_raw(io, 8, bytes);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    data.bytes[0] = revbytes[7];
    data.bytes[1] = revbytes[6];
    data.bytes[2] = revbytes[5];
    data.bytes[3] = revbytes[4];
    data.bytes[4] = revbytes[3];
    data.bytes[5] = revbytes[2];
    data.bytes[6] = revbytes[1];
    data.bytes[7] = revbytes[0];
#endif

    *num = data.num;

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_write_raw(zakodb_io_t* io, size_t n, const void* ptr) {
    size_t nwrite;

    nwrite = fwrite(ptr, sizeof(char), n, io->fp);
    if (nwrite != n) return ZAKODB_IO_FAILURE;

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_write_bytes(zakodb_io_t* io, uint16_t n, const uint8_t* bytes) {
    int ret;

    ret = zakodb_io_write_uint16(io, n);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

    ret = zakodb_io_write_raw(io, n, bytes);
    if (ret != ZAKODB_IO_SUCCESS) return ret;

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_write_cstr(zakodb_io_t* io, const char* str) {
    size_t len;
    size_t ret;

    len = strlen(str);
    ret = fwrite(str, sizeof(char), len + 1, io->fp);

    if (ret != len + 1) return ZAKODB_IO_FAILURE;

    return ZAKODB_IO_SUCCESS;
}

int zakodb_io_write_int8(zakodb_io_t* io, int8_t num) {
    return zakodb_io_write_uint8(io, (uint8_t)num);
}

int zakodb_io_write_uint8(zakodb_io_t* io, uint8_t num) {
    uint8_t bytes[1];

    bytes[0] = num;

    return zakodb_io_write_raw(io, 1, bytes);
}

int zakodb_io_write_int16(zakodb_io_t* io, int16_t num) {
    return zakodb_io_write_uint16(io, (uint16_t)num);
}

int zakodb_io_write_uint16(zakodb_io_t* io, uint16_t num) {
    uint8_t bytes[2];

    bytes[1] = (uint8_t)(num & 0xff);
    bytes[0] = (uint8_t)(num >> 8);

    return zakodb_io_write_raw(io, 2, bytes);
}

int zakodb_io_write_int32(zakodb_io_t* io, int32_t num) {
    return zakodb_io_write_uint32(io, (uint32_t)num);
}

int zakodb_io_write_uint32(zakodb_io_t* io, uint32_t num) {
    uint8_t bytes[4];

    bytes[3] = (uint8_t)(num & 0xff);
    bytes[2] = (uint8_t)((num >> 8) & 0xff);
    bytes[1] = (uint8_t)((num >> 16) & 0xff);
    bytes[0] = (uint8_t)(num >> 24);

    return zakodb_io_write_raw(io, 4, bytes);
}

int zakodb_io_write_int64(zakodb_io_t* io, int64_t num) {
    return zakodb_io_write_uint64(io, (uint64_t)num);
}

int zakodb_io_write_uint64(zakodb_io_t* io, uint64_t num) {
    uint8_t bytes[8];

    bytes[7] = (uint8_t)(num & 0xff);
    bytes[6] = (uint8_t)((num >> 8) & 0xff);
    bytes[5] = (uint8_t)((num >> 16) & 0xff);
    bytes[4] = (uint8_t)((num >> 24) & 0xff);
    bytes[3] = (uint8_t)((num >> 32) & 0xff);
    bytes[2] = (uint8_t)((num >> 40) & 0xff);
    bytes[1] = (uint8_t)((num >> 48) & 0xff);
    bytes[0] = (uint8_t)(num >> 56);

    return zakodb_io_write_raw(io, 8, bytes);
}

int zakodb_io_write_float32(zakodb_io_t* io, float num) {
    union {
        float   num;
        uint8_t bytes[4];
    } data;

    uint8_t* bytes;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    uint8_t revbytes[4];
#endif

    data.num = num;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    revbytes[0] = data.bytes[3];
    revbytes[1] = data.bytes[2];
    revbytes[2] = data.bytes[1];
    revbytes[3] = data.bytes[0];
    bytes = revbytes;
#else
    bytes = data.bytes;
#endif

    return zakodb_io_write_raw(io, 4, bytes);
}

int zakodb_io_write_float64(zakodb_io_t* io, double num) {
    union {
        double  num;
        uint8_t bytes[8];
    } data;

    uint8_t* bytes;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    uint8_t revbytes[8];
#endif

    data.num = num;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    revbytes[0] = data.bytes[7];
    revbytes[1] = data.bytes[6];
    revbytes[2] = data.bytes[5];
    revbytes[3] = data.bytes[4];
    revbytes[4] = data.bytes[3];
    revbytes[5] = data.bytes[2];
    revbytes[6] = data.bytes[1];
    revbytes[7] = data.bytes[0];
    bytes = revbytes;
#else
    bytes = data.bytes;
#endif

    return zakodb_io_write_raw(io, 8, bytes);
}

int zakodb_io_seek(zakodb_io_t* io, long off, int whence) {
    int ret;

    ret = fseek(io->fp, off, whence);
    if (ret != 0) return ZAKODB_IO_FAILURE;

    return ZAKODB_IO_SUCCESS;
}

long zakodb_io_tell(zakodb_io_t* io) { return ftell(io->fp); }

void zakodb_io_free_buf(void* ptr) {
    if (ptr != NULL) free(ptr);
}
