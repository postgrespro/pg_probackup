#ifndef __FILE__H__
#define __FILE__H__

#include <stdio.h>

typedef enum
{
	FIO_OPEN_NEW,
	FIO_OPEN_EXISTED,
	FIO_CLOSE,
	FIO_WRITE,
	FIO_RENAME,
	FIO_UNLINK,
	FIO_MKDIR,
	FIO_CHMOD,
	FIO_SEEK,
	FIO_TRUNCATE,
	FIO_READ
} fio_operations;

typedef enum
{
	FIO_DB_HOST,
	FIO_BACKUP_HOST
} fio_location;

#define FIO_FDMAX 64

#define SYS_CHECK(cmd) do if ((cmd) < 0) { perror(#cmd); exit(EXIT_FAILURE); } while (0)
#define IO_CHECK(cmd, size) do { int _rc = (cmd); if (_rc != (size)) { fprintf(stderr, "Receive %d bytes instead of %d\n", _rc, (int)(size)); exit(EXIT_FAILURE); } } while (0)

typedef struct {
	unsigned cop    : 4;
	unsigned handle : 8;
	unsigned size   : 20;
	unsigned arg;
} fio_header;

extern void   fio_redirect(int in, int out);
extern void   fio_communicate(int in, int out);

extern FILE*  fio_open(char const* name, char const* mode, fio_location location);
extern int    fio_close(FILE* f);
extern size_t fio_write(FILE* f, void const* buf, size_t size);
extern int    fio_printf(FILE* f, char const* arg, ...) __attribute__((format(printf, 2, 3)));
extern int    fio_flush(FILE* f);
extern int    fio_seek(FILE* f, off_t offs);
extern int    fio_truncate(FILE* f, off_t size);
extern int    fio_rename(char const* old_path, char const* new_path, fio_location location);
extern int    fio_unlink(char const* path, fio_location location);
extern int    fio_mkdir(char const* path, int mode, fio_location location);
extern int    fio_chmod(char const* path, int mode, fio_location location);

extern FILE*  fio_open_stream(char const* name, fio_location location);
extern int    fio_close_stream(FILE* f);

#endif

