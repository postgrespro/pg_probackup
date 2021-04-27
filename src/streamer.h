#include "libpq-fe.h"
#include "receivelog.h"

typedef struct XlogStreamCtl
{
	XLogRecPtr	startpos;		/* Start position for streaming */
	TimeLineID	timeline;		/* Timeline to stream data from */
	char	   *sysidentifier;	/* Validate this system identifier and
								 * timeline */
	int			standby_message_timeout;	/* Send status messages this often */
	bool		synchronous;	/* Flush immediately WAL data on write */
	bool		mark_done;		/* Mark segment as done in generated archive */
	bool		do_sync;		/* Flush to disk to ensure consistent state of
								 * data */

	stream_stop_callback stream_stop;	/* Stop streaming when returns true */

	pgsocket	stop_socket;	/* if valid, watch for input on this socket
								 * and check stream_stop() when there is any */

//	WalWriteMethod *walmethod;	/* How to write the WAL */
	char	   *partial_suffix; /* Suffix appended to partially received files */
	char	   *replication_slot;	/* Replication slot to use, or NULL */
    // new stuff
    char	   *basedir;
    int			compress_algo;
    int         compress_level;
    // Access Method
    pbkFile     open_for_write()
    // (*open_for_write) (const char *pathname, const char *temp_suffix, size_t pad_to_size);
} XlogStreamCtl;

//typedef struct StreamCtl
//{
//	XLogRecPtr	startpos;		/* Start position for streaming */
//	TimeLineID	timeline;		/* Timeline to stream data from */
//	char	   *sysidentifier;	/* Validate this system identifier and
//								 * timeline */
//	int			standby_message_timeout;	/* Send status messages this often */
//	bool		synchronous;	/* Flush immediately WAL data on write */
//	bool		mark_done;		/* Mark segment as done in generated archive */
//	bool		do_sync;		/* Flush to disk to ensure consistent state of
//								 * data */
//
//	stream_stop_callback stream_stop;	/* Stop streaming when returns true */
//
//	pgsocket	stop_socket;	/* if valid, watch for input on this socket
//								 * and check stream_stop() when there is any */
//
//	WalWriteMethod *walmethod;	/* How to write the WAL */
//	char	   *partial_suffix; /* Suffix appended to partially received files */
//	char	   *replication_slot;	/* Replication slot to use, or NULL */
//} StreamCtl;

//typedef struct WalWriteMethod WalWriteMethod;
//struct WalWriteMethod
//{
//	/*
//	 * Open a target file. Returns Walfile, or NULL if open failed. If a temp
//	 * suffix is specified, a file with that name will be opened, and then
//	 * automatically renamed in close(). If pad_to_size is specified, the file
//	 * will be padded with NUL up to that size, if supported by the Walmethod.
//	 */
//	Walfile		(*open_for_write) (const char *pathname, const char *temp_suffix, size_t pad_to_size);
//
//	/*
//	 * Close an open Walfile, using one or more methods for handling automatic
//	 * unlinking etc. Returns 0 on success, other values for error.
//	 */
//	int			(*close) (Walfile f, WalCloseMethod method);
//
//	/* Check if a file exist */
//	bool		(*existsfile) (const char *pathname);
//
//	/* Return the size of a file, or -1 on failure. */
//	ssize_t		(*get_file_size) (const char *pathname);
//
//	/*
//	 * Write count number of bytes to the file, and return the number of bytes
//	 * actually written or -1 for error.
//	 */
//	ssize_t		(*write) (Walfile f, const void *buf, size_t count);
//
//	/* Return the current position in a file or -1 on error */
//	off_t		(*get_current_pos) (Walfile f);
//
//	/*
//	 * fsync the contents of the specified file. Returns 0 on success.
//	 */
//	int			(*sync) (Walfile f);
//
//	/*
//	 * Clean up the Walmethod, closing any shared resources. For methods like
//	 * tar, this includes writing updated headers. Returns true if the
//	 * close/write/sync of shared resources succeeded, otherwise returns false
//	 * (but the resources are still closed).
//	 */
//	bool		(*finish) (void);
//
//	/* Return a text for the last error in this Walfile */
//	const char *(*getlasterror) (void);
//};

//typedef struct DirectoryMethodData
//{
//	char	   *basedir;
//	int			compression;
//	bool		sync;
//} DirectoryMethodData;
//static DirectoryMethodData *dir_data = NULL;

/*
 * Local file handle
 */
//typedef struct DirectoryMethodFile
//{
//	int			fd;
//	off_t		currpos;
//	char	   *pathname;
//	char	   *fullpath;
//	char	   *temp_suffix;
//#ifdef HAVE_LIBZ
//	gzFile		gzfp;
//#endif
//} DirectoryMethodFile;

extern void RunStream(PGconn *conn, StreamCtl *stream);
