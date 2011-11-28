/*-------------------------------------------------------------------------
 *
 * file.c:
 *
 * Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"

#include "storage/block.h"
#include "storage/bufpage.h"

#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

/* ----------------------------------------------------------------
 *	Reader and Writer
 * ----------------------------------------------------------------
 */
typedef struct Reader	Reader;
typedef size_t (*ReaderClose)(Reader *self);
typedef size_t (*ReaderRead)(Reader *self, void *buf, size_t len);

struct Reader
{
	ReaderClose		close;	/* close and returns physical read size */
	ReaderRead		read;
};

typedef struct Writer	Writer;
typedef size_t (*WriterClose)(Writer *self, pg_crc32 *crc);
typedef void (*WriterWrite)(Writer *self, const void *buf, size_t len);

struct Writer
{
	WriterClose		close;	/* close and returns physical written size */
	WriterWrite		write;
};

static Reader *FileReader(const char *path);
static Writer *FileWriter(const char *path);
static Reader *ZlibReader(Reader *inner);
static Writer *ZlibWriter(Writer *inner);
static Reader *DataReader(Reader *inner);
static Reader *BackupReader(Reader *inner);

#define inner_read(self, buf, len) \
	((self)->inner->read((self)->inner, (buf), (len)))
#define inner_write(self, buf, len) \
	((self)->inner->write((self)->inner, (buf), (len)))

/* ----------------------------------------------------------------
 *	PostgreSQL data files
 * ----------------------------------------------------------------
 */
#define PG_PAGE_LAYOUT_VERSION_v81		3	/* 8.1 - 8.2 */
#define PG_PAGE_LAYOUT_VERSION_v83		4	/* 8.3 - */

/* 80000 <= PG_VERSION_NUM < 80300 */
typedef struct PageHeaderData_v80
{
	XLogRecPtr		pd_lsn;
	TimeLineID		pd_tli;
	LocationIndex	pd_lower;
	LocationIndex	pd_upper;
	LocationIndex	pd_special;
	uint16			pd_pagesize_version;
	ItemIdData		pd_linp[1];
} PageHeaderData_v80;

#define PageGetPageSize_v80(page) \
	((Size) ((page)->pd_pagesize_version & (uint16) 0xFF00))
#define PageGetPageLayoutVersion_v80(page) \
	((page)->pd_pagesize_version & 0x00FF)
#define SizeOfPageHeaderData_v80	(offsetof(PageHeaderData_v80, pd_linp))

/* 80300 <= PG_VERSION_NUM */
typedef struct PageHeaderData_v83
{
	XLogRecPtr		pd_lsn;
	uint16			pd_tli;
	uint16			pd_flags;
	LocationIndex	pd_lower;
	LocationIndex	pd_upper;
	LocationIndex	pd_special;
	uint16			pd_pagesize_version;
	TransactionId	pd_prune_xid;
	ItemIdData		pd_linp[1];
} PageHeaderData_v83;

#define PageGetPageSize_v83(page) \
	((Size) ((page)->pd_pagesize_version & (uint16) 0xFF00))
#define PageGetPageLayoutVersion_v83(page) \
	((page)->pd_pagesize_version & 0x00FF)
#define SizeOfPageHeaderData_v83	(offsetof(PageHeaderData_v83, pd_linp))
#define PD_VALID_FLAG_BITS_v83		0x0007

typedef union DataPage
{
	XLogRecPtr			pd_lsn;
	PageHeaderData_v80	v80;	/* 8.0 - 8.2 */
	PageHeaderData_v83	v83;	/* 8.3 - */
	char				data[1];
} DataPage;

static void do_copy(pgFile *file, Reader *in, Writer *out);

/*
 * Backup a file.
 */
void
pgFile_backup(pgFile *file, const char *from, const char *to)
{
	Reader	   *in;
	Writer	   *out;
	char		path[MAXPGPATH];

	Assert(file);
	Assert(from);
	Assert(to);

	/* Reader */
	join_path_components(path, from, file->name);
	if ((in = FileReader(path)) == NULL)
	{
		file->mode = MISSING_FILE;
		return;	/* have been deleted, ignore this file */
	}
	if (file->flags & PGFILE_DATA)
		in = DataReader(in);

	/* Writer */
	join_path_components(path, to, file->name);
	out = FileWriter(path);
	if (file->flags & PGFILE_ZLIB)
		out = ZlibWriter(out);

	do_copy(file, in, out);

	elog(LOG, "backup file: %s (%.2f%% of %lu bytes)",
		file->name, 100.0 * file->written_size / file->size,
		(unsigned long) file->size);
}

/*
 * Restore a file.
 */
void
pgFile_restore(pgFile *file, const char *from, const char *to)
{
	Reader	   *in;
	Writer	   *out;
	char		path[MAXPGPATH];

	Assert(file);
	Assert(from);
	Assert(to);

	/* Reader */
	join_path_components(path, from, file->name);
	if ((in = FileReader(path)) == NULL)
		return;	/* have been deleted, ignore this file */
	if (file->flags & PGFILE_ZLIB)
		in = ZlibReader(in);
	if (file->flags & PGFILE_DATA)
		in = BackupReader(in);

	/* Writer */
	join_path_components(path, to, file->name);
	out = FileWriter(path);

	do_copy(file, in, out);

	/* update file permission */
	if (chmod(path, file->mode) != 0)
		ereport(ERROR,
			(errcode_errno(),
			 errmsg("could not change mode of \"%s\": ", path)));

	elog(LOG, "restore file: %s (%.2f%% of %lu bytes)",
		file->name, 100.0 * file->written_size / file->size,
		(unsigned long) file->size);

	/* TODO: restore other attributes, including mtime. */
}

static void
do_copy(pgFile *file, Reader *in, Writer *out)
{
	void	   *buffer;
	size_t		buflen;
	size_t		len;
	pg_crc32	crc;

	Assert(block_size > 0);
	Assert(wal_block_size > 0);

	buflen = Max(block_size, wal_block_size);
	buffer = pgut_malloc(buflen);	/* use malloc for memroy alignment */

	/* copy contents */
	while ((len = in->read(in, buffer, buflen)) > 0)
	{
		CHECK_FOR_INTERRUPTS();
		out->write(out, buffer, len);
	}

	/* close in and out */
	file->read_size = in->close(in);
	file->written_size = out->close(out, &crc);
	file->crc = crc;
	file->flags |= PGFILE_CRC;

	free(buffer);
}

/* ----------------------------------------------------------------
 *	File Reader
 * ----------------------------------------------------------------
 */
typedef struct FReader
{
	Reader		base;
	FILE	   *fp;
	size_t		done;
} FReader;

static size_t FReader_close(FReader *self);
static size_t FReader_read(FReader *self, void *buf, size_t len);

/* returns NULL if file not found */
static Reader *
FileReader(const char *path)
{
	FReader	   *self;
	FILE	   *fp;

	if ((fp = pgut_fopen(path, "R")) == NULL)
		return NULL;

	self = pgut_new(FReader);
	self->base.close = (ReaderClose) FReader_close;
	self->base.read = (ReaderRead) FReader_read;
	self->fp = fp;
	self->done = 0;

	return (Reader *) self;
}

static size_t
FReader_close(FReader *self)
{
	size_t	done = self->done;

	fclose(self->fp);
	free(self);

	return done;
}

static size_t
FReader_read(FReader *self, void *buf, size_t len)
{
	errno = 0;
	len = fread(buf, 1, len, self->fp);
	if (errno != 0)
		ereport(ERROR,
			(errcode_errno(),
			 errmsg("could not read file: ")));
	self->done += len;
	return len;
}

/* ----------------------------------------------------------------
 *	File Writer
 * ----------------------------------------------------------------
 */
typedef struct FWriter
{
	Writer		base;
	FILE	   *fp;
	size_t		done;
	pg_crc32	crc;
} FWriter;

static size_t FWriter_close(FWriter *self, pg_crc32 *crc);
static void FWriter_write(FWriter *self, const void *buf, size_t len);

static Writer *
FileWriter(const char *path)
{
	FWriter	   *self;
	FILE	   *fp;

	fp = pgut_fopen(path, "w");

	self = pgut_new(FWriter);
	self->base.close = (WriterClose) FWriter_close;
	self->base.write = (WriterWrite) FWriter_write;
	self->fp = fp;
	self->done = 0;
	INIT_CRC32(self->crc);

	return (Writer *) self;
}

static size_t
FWriter_close(FWriter *self, pg_crc32 *crc)
{
	size_t		done = self->done;

	if (crc)
	{
		FIN_CRC32(self->crc);
		*crc = self->crc;
	}
	fclose(self->fp);
	free(self);

	return done;
}

static void
FWriter_write(FWriter *self, const void *buf, size_t len)
{
	if (fwrite(buf, 1, len, self->fp) != len)
		ereport(ERROR,
			(errcode_errno(),
			 errmsg("could not write file: ")));
	self->done += len;
	COMP_CRC32(self->crc, buf, len);
}

#ifdef HAVE_LIBZ

#define Z_BUFSIZE	(64 * 1024)	/* 64KB */

/* ----------------------------------------------------------------
 *	LibZ Reader
 * ----------------------------------------------------------------
 */
typedef struct ZReader
{
	Reader		base;
	Reader	   *inner;
	z_stream	z;
	Byte		buf[Z_BUFSIZE];
} ZReader;

static size_t ZReader_close(ZReader *self);
static size_t ZReader_read(ZReader *self, void *buf, size_t len);

static Reader *
ZlibReader(Reader *inner)
{
	ZReader    *self = pgut_new(ZReader);

	memset(self, 0, sizeof(ZReader));
	self->base.close = (ReaderClose) ZReader_close;
	self->base.read = (ReaderRead) ZReader_read;
	self->inner = inner;
	if (inflateInit(&self->z) != Z_OK)
		elog(ERROR, "could not create z_stream: %s", self->z.msg);

	return (Reader *) self;
}

static size_t
ZReader_close(ZReader *self)
{
	size_t	done;

	if (inflateEnd(&self->z) != Z_OK)
		elog(ERROR, "could not close z_stream: %s", self->z.msg);
	done = self->inner->close(self->inner);
	free(self);

	return done;
}

static size_t
ZReader_read(ZReader *self, void *buf, size_t len)
{
	self->z.next_out = buf;
	self->z.avail_out = len;

	while (self->z.avail_out > 0)
	{
		/* fill input buffer if empty */
		if (self->z.avail_in == 0)
		{
			size_t	sz = inner_read(self, self->buf, Z_BUFSIZE);
			if (sz == 0)
				break;

			self->z.next_in = self->buf;
			self->z.avail_in = sz;
		}

		/* inflate into output buffer */
		switch (inflate(&self->z, Z_NO_FLUSH))
		{
			case Z_STREAM_END:
			case Z_OK:
				continue;	/* ok, go next */
			default:
				elog(ERROR, "could not inflate z_stream: %s", self->z.msg);
		}
	}

	return len - self->z.avail_out;
}

/* ----------------------------------------------------------------
 *	LibZ Writer
 * ----------------------------------------------------------------
 */
typedef struct ZWriter
{
	Writer		base;
	Writer	   *inner;
	z_stream	z;
	Byte		buf[Z_BUFSIZE];
} ZWriter;

static size_t ZWriter_close(ZWriter *self, pg_crc32 *crc);
static void ZWriter_write(ZWriter *self, const void *buf, size_t len);

static Writer *
ZlibWriter(Writer *inner)
{
	ZWriter    *self = pgut_new(ZWriter);

	memset(self, 0, sizeof(ZWriter));
	self->base.close = (WriterClose) ZWriter_close;
	self->base.write = (WriterWrite) ZWriter_write;
	self->inner = inner;
	if (deflateInit(&self->z, Z_DEFAULT_COMPRESSION) != Z_OK)
		elog(ERROR, "could not create z_stream: %s", self->z.msg);
	self->z.next_out = self->buf;
	self->z.avail_out = Z_BUFSIZE;

	return (Writer *) self;
}

static size_t
ZWriter_close(ZWriter *self, pg_crc32 *crc)
{
	size_t		done;

	/* finish zstream */
	self->z.next_in = NULL;
	self->z.avail_in = 0;
	if (deflate(&self->z, Z_FINISH) < 0)
		elog(ERROR, "could not finish z_stream: %s", self->z.msg);
	inner_write(self, self->buf, Z_BUFSIZE - self->z.avail_out);

	if (deflateEnd(&self->z) != Z_OK)
		elog(ERROR, "could not close z_stream: %s", self->z.msg);
	done = self->inner->close(self->inner, crc);
	free(self);

	return done;
}

static void
ZWriter_write(ZWriter *self, const void *buf, size_t len)
{
	self->z.next_in = (void *) buf;
	self->z.avail_in = len;

	/* compresses until an input buffer becomes empty. */
	while (self->z.avail_in > 0)
	{
		if (deflate(&self->z, Z_NO_FLUSH) < 0)
			elog(ERROR, "could not deflate z_stream: %s", self->z.msg);

		if (self->z.avail_out == 0)
		{
			inner_write(self, self->buf, Z_BUFSIZE);
			self->z.next_out = self->buf;
			self->z.avail_out = Z_BUFSIZE;
		}
	}
}

#else /* HAVE_LIBZ */

static Reader *
ZlibReader(Reader *inner)
{
	elog(ERROR, "zlib is unavailable");
}

static Writer *
ZlibWriter(Writer *inner)
{
	elog(ERROR, "zlib is unavailable");
}

#endif /* HAVE_LIBZ */

/* ----------------------------------------------------------------
 * Reader for backup and restore data files
 * ----------------------------------------------------------------
 */
typedef struct DReader
{
	Reader		base;
	Reader	   *inner;
} DReader;

static size_t DReader_close(DReader *self);
static size_t Data_read(DReader *self, char *buf, size_t len);
static size_t Backup_read(DReader *self, DataPage *buf, size_t len);
static bool parse_header(const DataPage *page, uint16 *lower, uint16 *upper);

/* 
 * DataReader - Data file compresser.
 *
 * Unused free space is removed. If lsn is not NULL, only modified pages
 * after the lsn will be copied.
 */
static Reader *
DataReader(Reader *inner)
{
	DReader *self = pgut_new(DReader);

	self->base.close = (ReaderClose) DReader_close;
	self->base.read = (ReaderRead) Data_read;
	self->inner = inner;

	return (Reader *) self;
}

/* 
 * BackupReader - Data file decompresser.
 */
static Reader *
BackupReader(Reader *inner)
{
	DReader *self = pgut_new(DReader);

	self->base.close = (ReaderClose) DReader_close;
	self->base.read = (ReaderRead) Backup_read;
	self->inner = inner;

	return (Reader *) self;
}

static size_t
DReader_close(DReader *self)
{
	size_t		done;

	done = self->inner->close(self->inner);
	free(self);

	return done;
}

static size_t
Data_read(DReader *self, char *buf, size_t len)
{
	DataPage	page;
	size_t		done;
	uint16		pd_lower;
	uint16		pd_upper;
	int			upper_length;

	/* read a page at once */
	done = 0;
	while (done < block_size)
	{
		size_t	sz = inner_read(self, page.data + done, block_size - done);

		if (sz == 0)
			break;
		done += sz;
	}
	if (done == 0)
		return 0;	/* eof */
	if (done != block_size || !parse_header(&page, &pd_lower, &pd_upper))
		goto bad_file;

	/* XXX: repair fragmentation with PageRepairFragmentation? */

#if 0
	/* if the page has not been modified since last backup, skip it */
	if (lsn && !XLogRecPtrIsInvalid(page.pd_lsn) && XLByteLT(page.pd_lsn, *lsn))
		goto retry;
#endif

	upper_length = block_size - pd_upper;
	if (len > pd_lower + upper_length)
		elog(ERROR, "buffer too small");

	/* remove hole of the page */
	memcpy(buf, page.data, pd_lower);
	memcpy(buf + pd_lower, page.data + pd_upper, upper_length);

	return pd_lower + upper_length;

bad_file:
	/* TODO: If a invalid data page was found, fallback to simple copy. */
	elog(ERROR, "not a data file");
	return 0;
}

static size_t
Backup_read(DReader *self, DataPage *buf, size_t len)
{
	size_t		sz;
	uint16		pd_lower;
	uint16		pd_upper;
	int			lower_remain;
	int			upper_length;
	int			header_size;

	Assert(len >= block_size);

	if (server_version < 80300)
		header_size = SizeOfPageHeaderData_v80;
	else
		header_size = SizeOfPageHeaderData_v83;

	/* read each page and write the page excluding hole */
	if ((sz = inner_read(self, buf, header_size)) == 0)
		return 0;
	if (sz != header_size ||
		!parse_header(buf, &pd_lower, &pd_upper))
		goto bad_file;

	lower_remain = pd_lower - header_size;
	upper_length = block_size - pd_upper;

	/* read remain lower and upper, and fill the hole with zero. */
	if (inner_read(self, buf + header_size, lower_remain) != lower_remain ||
		inner_read(self, buf + pd_upper, upper_length) != upper_length)
		goto bad_file;
	memset(buf + pd_lower, 0, pd_upper - pd_lower);

	return block_size;

bad_file:
	elog(ERROR, "not a data file");
	return 0;
}

static bool
parse_header(const DataPage *page, uint16 *lower, uint16 *upper)
{
	uint16		page_layout_version;

	/* Determine page layout version */
	if (server_version < 80300)
		page_layout_version = PG_PAGE_LAYOUT_VERSION_v81;
	else
		page_layout_version = PG_PAGE_LAYOUT_VERSION_v83;

	/* Check normal case */
	if (server_version < 80300)
	{
		const PageHeaderData_v80 *v80 = &page->v80;

		if (PageGetPageSize_v80(v80) == block_size &&
			PageGetPageLayoutVersion_v80(v80) == page_layout_version &&
			v80->pd_lower >= SizeOfPageHeaderData_v80 &&
			v80->pd_lower <= v80->pd_upper &&
			v80->pd_upper <= v80->pd_special &&
			v80->pd_special <= block_size &&
			v80->pd_special == MAXALIGN(v80->pd_special) &&
			!XLogRecPtrIsInvalid(v80->pd_lsn))
		{
			*lower = v80->pd_lower;
			*upper = v80->pd_upper;
			return true;
		}
	}
	else
	{
		const PageHeaderData_v83 *v83 = &page->v83;

		if (PageGetPageSize_v83(v83) == block_size &&
			PageGetPageLayoutVersion_v83(v83) == page_layout_version &&
			(v83->pd_flags & ~PD_VALID_FLAG_BITS_v83) == 0 &&
			v83->pd_lower >= SizeOfPageHeaderData_v83 &&
			v83->pd_lower <= v83->pd_upper &&
			v83->pd_upper <= v83->pd_special &&
			v83->pd_special <= block_size &&
			v83->pd_special == MAXALIGN(v83->pd_special) &&
			!XLogRecPtrIsInvalid(v83->pd_lsn))
		{
			*lower = v83->pd_lower;
			*upper = v83->pd_upper;
			return true;
		}
	}
	
	*lower = *upper = 0;
	return false;
}
