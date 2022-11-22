#ifndef FILE_COMPAT_H
#define FILE_COMPAT_H

#include <sys/stat.h>
#include "datatype/timestamp.h"


#if PG_VERSION_NUM >= 120000
#include "common/logging.h"
#else
#include "logging.h"
#endif


extern int fsync_parent_path_compat(const char* fname);
extern int fsync_fname_compat(const char* fname, bool isdir);
extern int durable_rename_compat(const char* oldfile, const char* newfile);


#if PG_VERSION_NUM < 110000
#include "file_compat10.h"
#else
#include "common/file_perm.h"
#include "access/xlog_internal.h"
#endif



#endif							/* FILE_COMPAT_H */



