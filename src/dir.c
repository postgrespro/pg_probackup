/*-------------------------------------------------------------------------
 *
 * dir.c: directory operation utility.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2022, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include <assert.h>
#include "pg_probackup.h"
#include "utils/file.h"


#if PG_VERSION_NUM < 110000
#include "catalog/catalog.h"
#endif
#include "catalog/pg_tablespace.h"

#include <unistd.h>
#include <dirent.h>

#include "utils/configuration.h"

/*
 * The contents of these directories are removed or recreated during server
 * start so they are not included in backups.  The directories themselves are
 * kept and included as empty to preserve access permissions.
 */
static const char *pgdata_exclude_dir[] =
{
	PG_XLOG_DIR,
	/*
	 * Skip temporary statistics files. PG_STAT_TMP_DIR must be skipped even
	 * when stats_temp_directory is set because PGSS_TEXT_FILE is always created
	 * there.
	 */
	"pg_stat_tmp",
	"pgsql_tmp",

	/*
	 * It is generally not useful to backup the contents of this directory even
	 * if the intention is to restore to another master. See backup.sgml for a
	 * more detailed description.
	 */
	"pg_replslot",

	/* Contents removed on startup, see dsm_cleanup_for_mmap(). */
	"pg_dynshmem",

	/* Contents removed on startup, see AsyncShmemInit(). */
	"pg_notify",

	/*
	 * Old contents are loaded for possible debugging but are not required for
	 * normal operation, see OldSerXidInit().
	 */
	"pg_serial",

	/* Contents removed on startup, see DeleteAllExportedSnapshotFiles(). */
	"pg_snapshots",

	/* Contents zeroed on startup, see StartupSUBTRANS(). */
	"pg_subtrans",

	/* end of list */
	NULL,				/* pg_log will be set later */
	NULL
};

static char *pgdata_exclude_files[] =
{
	/* Skip auto conf temporary file. */
	"postgresql.auto.conf.tmp",

	/* Skip current log file temporary file */
	"current_logfiles.tmp",
	"recovery.conf",
	"postmaster.pid",
	"postmaster.opts",
	"probackup_recovery.conf",
	"recovery.signal",
	"standby.signal",

	/*skip in non-exclusive backup */
	"backup_label",
	"tablespace_map",
	NULL
};

/* Tablespace mapping structures */

typedef struct TablespaceListCell
{
	struct TablespaceListCell *next;
	char		old_dir[MAXPGPATH];
	char		new_dir[MAXPGPATH];
} TablespaceListCell;

typedef struct TablespaceList
{
	TablespaceListCell *head;
	TablespaceListCell *tail;
} TablespaceList;

typedef struct TablespaceCreatedListCell
{
	struct TablespaceCreatedListCell *next;
	char		link_name[MAXPGPATH];
	char		linked_dir[MAXPGPATH];
} TablespaceCreatedListCell;

typedef struct TablespaceCreatedList
{
	TablespaceCreatedListCell *head;
	TablespaceCreatedListCell *tail;
} TablespaceCreatedList;

typedef struct exclude_cb_ctx {
    bool	backup_logs;
	size_t	pref_len;
	char	exclude_dir_content_pref[MAXPGPATH];
} exclude_cb_ctx;

static char dir_check_file(pgFile *file, bool backup_logs);

static void dir_list_file_internal(parray *files, pgFile *parent, const char *parent_dir,
								   bool handle_tablespaces, bool follow_symlink, bool backup_logs,
								   bool skip_hidden, int external_dir_num, fio_location location);
static void opt_path_map(ConfigOption *opt, const char *arg,
						 TablespaceList *list, const char *type);
static void cleanup_tablespace(const char *path);

static void control_string_bad_format(const char* str);

static bool exclude_files_cb(void *value, void *exclude_args);

/* Tablespace mapping */
static TablespaceList tablespace_dirs = {NULL, NULL};
/* Extra directories mapping */
static TablespaceList external_remap_list = {NULL, NULL};

pgFile *
pgFileNew(const char *path, const char *rel_path, bool follow_symlink,
		  int external_dir_num, fio_location location)
{
	struct stat		st;
	pgFile		   *file;

	/* stat the file */
	if (fio_stat(location, path, &st, follow_symlink) < 0)
	{
		/* file not found is not an error case */
		if (errno == ENOENT)
			return NULL;
		elog(ERROR, "cannot stat file \"%s\": %s", path,
			strerror(errno));
	}

	file = pgFileInit(rel_path);
	file->size = st.st_size;
	file->mode = st.st_mode;
	file->mtime = st.st_mtime;
	file->external_dir_num = external_dir_num;

	return file;
}

pgFile *
pgFileInit(const char *rel_path)
{
	pgFile	   *file;
	char	   *file_name = NULL;

	file = (pgFile *) pgut_malloc(sizeof(pgFile));
	MemSet(file, 0, sizeof(pgFile));

	file->rel_path = pgut_strdup(rel_path);
	canonicalize_path(file->rel_path);

	/* Get file name from the path */
	file_name = last_dir_separator(file->rel_path);

	if (file_name == NULL)
		file->name = file->rel_path;
	else
	{
		file_name++;
		file->name = file_name;
	}

	/* Number of blocks readed during backup */
	file->n_blocks = BLOCKNUM_INVALID;

	/* Number of blocks backed up during backup */
	file->n_headers = 0;

	// May be add?
	// pg_atomic_clear_flag(file->lock);
	file->excluded = false;
	return file;
}

/*
 * Read the local file to compute its CRC.
 * We cannot make decision about file decompression because
 * user may ask to backup already compressed files and we should be
 * obvious about it.
 */
pg_crc32
pgFileGetCRC32C(const char *file_path, bool missing_ok)
{
	FILE	   *fp;
	pg_crc32	crc = 0;
	char	   *buf;
	size_t		len = 0;

	INIT_CRC32C(crc);

	/* open file in binary read mode */
	fp = fopen(file_path, PG_BINARY_R);
	if (fp == NULL)
	{
		if (missing_ok && errno == ENOENT)
		{
			FIN_CRC32C(crc);
			return crc;
		}

		elog(ERROR, "Cannot open file \"%s\": %s",
			file_path, strerror(errno));
	}

	/* disable stdio buffering */
	setvbuf(fp, NULL, _IONBF, BUFSIZ);
	buf = pgut_malloc(STDIO_BUFSIZE);

	/* calc CRC of file */
	do
	{
		if (interrupted)
			elog(ERROR, "interrupted during CRC calculation");

		len = fread(buf, 1, STDIO_BUFSIZE, fp);

		if (ferror(fp))
			elog(ERROR, "Cannot read \"%s\": %s", file_path, strerror(errno));

		COMP_CRC32C(crc, buf, len);
	}
	while (!feof(fp));

	FIN_CRC32C(crc);
	fclose(fp);
	pg_free(buf);

	return crc;
}

#if PG_VERSION_NUM < 120000
/*
 * Read the local file to compute its CRC using traditional algorithm.
 * (*_TRADITIONAL_CRC32 macros)
 * This was used only in version 2.0.22--2.0.24
 * And never used for PG >= 12
 * To be removed with end of PG-11 support
 */
pg_crc32
pgFileGetCRC32(const char *file_path, bool missing_ok)
{
	FILE	   *fp;
	pg_crc32	crc = 0;
	char	   *buf;
	size_t		len = 0;

	INIT_TRADITIONAL_CRC32(crc);

	/* open file in binary read mode */
	fp = fopen(file_path, PG_BINARY_R);
	if (fp == NULL)
	{
		if (missing_ok && errno == ENOENT)
		{
			FIN_TRADITIONAL_CRC32(crc);
			return crc;
		}

		elog(ERROR, "Cannot open file \"%s\": %s",
			file_path, strerror(errno));
	}

	/* disable stdio buffering */
	setvbuf(fp, NULL, _IONBF, BUFSIZ);
	buf = pgut_malloc(STDIO_BUFSIZE);

	/* calc CRC of file */
	do
	{
		if (interrupted)
			elog(ERROR, "interrupted during CRC calculation");

		len = fread(buf, 1, STDIO_BUFSIZE, fp);

		if (ferror(fp))
			elog(ERROR, "Cannot read \"%s\": %s", file_path, strerror(errno));

		COMP_TRADITIONAL_CRC32(crc, buf, len);
	}
	while (!feof(fp));

	FIN_TRADITIONAL_CRC32(crc);
	fclose(fp);
	pg_free(buf);

	return crc;
}
#endif /* PG_VERSION_NUM < 120000 */

/*
 * Read the local file to compute its CRC.
 * We cannot make decision about file decompression because
 * user may ask to backup already compressed files and we should be
 * obvious about it.
 */
pg_crc32
pgFileGetCRC32Cgz(const char *file_path, bool missing_ok)
{
	gzFile    fp;
	pg_crc32  crc = 0;
	int       len = 0;
	int       err;
	char	 *buf;

	INIT_CRC32C(crc);

	/* open file in binary read mode */
	fp = gzopen(file_path, PG_BINARY_R);
	if (fp == NULL)
	{
		if (missing_ok && errno == ENOENT)
		{
			FIN_CRC32C(crc);
			return crc;
		}

		elog(ERROR, "Cannot open file \"%s\": %s",
			file_path, strerror(errno));
	}

	buf = pgut_malloc(STDIO_BUFSIZE);

	/* calc CRC of file */
	for (;;)
	{
		if (interrupted)
			elog(ERROR, "interrupted during CRC calculation");

		len = gzread(fp, buf, STDIO_BUFSIZE);

		if (len <= 0)
		{
			/* we either run into eof or error */
			if (gzeof(fp))
				break;
			else
			{
				const char *err_str = NULL;

				err_str = gzerror(fp, &err);
				elog(ERROR, "Cannot read from compressed file %s", err_str);
			}
		}

		/* update CRC */
		COMP_CRC32C(crc, buf, len);
	}

	FIN_CRC32C(crc);
	gzclose(fp);
	pg_free(buf);

	return crc;
}

void
pgFileFree(void *file)
{
	pgFile	   *file_ptr;

	if (file == NULL)
		return;

	file_ptr = (pgFile *) file;

	pfree(file_ptr->linked);
	pfree(file_ptr->rel_path);

	pfree(file);
}

/* Compare two pgFile with their path in ascending order of ASCII code. */
int
pgFileMapComparePath(const void *f1, const void *f2)
{
	page_map_entry *f1p = *(page_map_entry **)f1;
	page_map_entry *f2p = *(page_map_entry **)f2;

	return strcmp(f1p->path, f2p->path);
}

/* Compare two pgFile with their name in ascending order of ASCII code. */
int
pgFileCompareName(const void *f1, const void *f2)
{
	pgFile *f1p = *(pgFile **)f1;
	pgFile *f2p = *(pgFile **)f2;

	return strcmp(f1p->name, f2p->name);
}

/* Compare pgFile->name with string in ascending order of ASCII code. */
int
pgFileCompareNameWithString(const void *f1, const void *f2)
{
	pgFile *f1p = *(pgFile **)f1;
	char *f2s = *(char **)f2;

	return strcmp(f1p->name, f2s);
}

/* Compare pgFile->rel_path with string in ascending order of ASCII code. */
int
pgFileCompareRelPathWithString(const void *f1, const void *f2)
{
	pgFile *f1p = *(pgFile **)f1;
	char *f2s = *(char **)f2;

	return strcmp(f1p->rel_path, f2s);
}

/*
 * Compare two pgFile with their relative path and external_dir_num in ascending
 * order of ASСII code.
 */
int
pgFileCompareRelPathWithExternal(const void *f1, const void *f2)
{
	pgFile *f1p = *(pgFile **)f1;
	pgFile *f2p = *(pgFile **)f2;
	int 		res;

	res = strcmp(f1p->rel_path, f2p->rel_path);
	if (res == 0)
	{
		if (f1p->external_dir_num > f2p->external_dir_num)
			return 1;
		else if (f1p->external_dir_num < f2p->external_dir_num)
			return -1;
		else
			return 0;
	}
	return res;
}

/*
 * Compare two pgFile with their rel_path and external_dir_num
 * in descending order of ASCII code.
 */
int
pgFileCompareRelPathWithExternalDesc(const void *f1, const void *f2)
{
	return -pgFileCompareRelPathWithExternal(f1, f2);
}

/* Compare two pgFile with their linked directory path. */
int
pgFileCompareLinked(const void *f1, const void *f2)
{
	pgFile	   *f1p = *(pgFile **)f1;
	pgFile	   *f2p = *(pgFile **)f2;

	return strcmp(f1p->linked, f2p->linked);
}

/* Compare two pgFile with their size */
int
pgFileCompareSize(const void *f1, const void *f2)
{
	pgFile *f1p = *(pgFile **)f1;
	pgFile *f2p = *(pgFile **)f2;

	if (f1p->size > f2p->size)
		return 1;
	else if (f1p->size < f2p->size)
		return -1;
	else
		return 0;
}

/* Compare two pgFile with their size in descending order */
int
pgFileCompareSizeDesc(const void *f1, const void *f2)
{
	return -1 * pgFileCompareSize(f1, f2);
}

int
pgCompareString(const void *str1, const void *str2)
{
	return strcmp(*(char **) str1, *(char **) str2);
}

/*
 * From bsearch(3): "The compar routine is expected to have two argu‐
 * ments  which  point  to  the key object and to an array member, in that order"
 * But in practice this is opposite, so we took strlen from second string (search key)
 * This is checked by tests.catchup.CatchupTest.test_catchup_with_exclude_path
 */
int
pgPrefixCompareString(const void *str1, const void *str2)
{
	const char *s1 = *(char **) str1;
	const char *s2 = *(char **) str2;
	return strncmp(s1, s2, strlen(s2));
}

/* Compare two Oids */
int
pgCompareOid(const void *f1, const void *f2)
{
	Oid *v1 = *(Oid **) f1;
	Oid *v2 = *(Oid **) f2;

	if (*v1 > *v2)
		return 1;
	else if (*v1 < *v2)
		return -1;
	else
		return 0;}


void
db_map_entry_free(void *entry)
{
	db_map_entry *m = (db_map_entry *) entry;

	free(m->datname);
	free(entry);
}

/*
 * List files, symbolic links and directories in the directory "root" and add
 * pgFile objects to "files".  We add "root" to "files" if add_root is true.
 *
 * When follow_symlink is true, symbolic link is ignored and only file or
 * directory linked to will be listed.
 *
 * TODO: make it strictly local
 */
void
dir_list_file(parray *files, const char *root, bool handle_tablespaces, bool follow_symlink,
			  bool backup_logs, bool skip_hidden, int external_dir_num, fio_location location)
{
	pgFile	   *file;

	file = pgFileNew(root, "", follow_symlink, external_dir_num, location);
	if (file == NULL)
	{
		/* For external directory this is not ok */
		if (external_dir_num > 0)
			elog(ERROR, "External directory is not found: \"%s\"", root);
		else
			return;
	}

	if (!S_ISDIR(file->mode))
	{
		if (external_dir_num > 0)
			elog(ERROR, " --external-dirs option \"%s\": directory or symbolic link expected",
					root);
		else
			elog(WARNING, "Skip \"%s\": unexpected file format", root);
		return;
	}

	dir_list_file_internal(files, file, root, handle_tablespaces, follow_symlink,
						   backup_logs, skip_hidden, external_dir_num, location);

	pgFileFree(file);
}

#define CHECK_FALSE				0
#define CHECK_TRUE				1
#define CHECK_EXCLUDE_FALSE		2

/*
 * Check file or directory.
 *
 * Check for exclude.
 * Extract information about the file parsing its name.
 * Skip files:
 * - skip temp tables files
 * - skip unlogged tables files
 * Skip recursive tablespace content
 * Set flags for:
 * - database directories
 * - datafiles
 */
static char
dir_check_file(pgFile *file, bool backup_logs)
{
	int			i;
	int			sscanf_res;
	bool		in_tablespace = false;

	in_tablespace = path_is_prefix_of_path(PG_TBLSPC_DIR, file->rel_path);

	/* Check if we need to exclude file by name */
	if (S_ISREG(file->mode))
	{
		for (i = 0; pgdata_exclude_files[i]; i++)
			if (strcmp(file->rel_path, pgdata_exclude_files[i]) == 0)
			{
				/* Skip */
				elog(LOG, "Excluding file: %s", file->name);
				return CHECK_FALSE;
			}
	}
	/*
	 * If the directory name is in the exclude list, do not list the
	 * contents.
	 */
	else if (S_ISDIR(file->mode) && !in_tablespace && file->external_dir_num == 0)
	{
		/*
		 * If the item in the exclude list starts with '/', compare to
		 * the absolute path of the directory. Otherwise compare to the
		 * directory name portion.
		 */
		for (i = 0; pgdata_exclude_dir[i]; i++)
		{
			/* exclude by dirname */
			if (strcmp(file->name, pgdata_exclude_dir[i]) == 0)
			{
				elog(LOG, "Excluding directory content: %s", file->rel_path);
				return CHECK_EXCLUDE_FALSE;
			}
		}

		if (!backup_logs)
		{
			if (strcmp(file->rel_path, PG_LOG_DIR) == 0)
			{
				/* Skip */
				elog(LOG, "Excluding directory content: %s", file->rel_path);
				return CHECK_EXCLUDE_FALSE;
			}
		}
	}

	/* Do not backup ptrack_init files */
	if (S_ISREG(file->mode) && strcmp(file->name, "ptrack_init") == 0)
		return CHECK_FALSE;

	/*
	 * Check files located inside database directories including directory
	 * 'global'
	 */
	if (S_ISREG(file->mode) && file->tblspcOid != 0 &&
		file->name && file->name[0])
	{
		if (strcmp(file->name, "pg_internal.init") == 0)
			return CHECK_FALSE;
		/* Do not backup ptrack2.x temp map files */
//		else if (strcmp(file->name, "ptrack.map") == 0)
//			return CHECK_FALSE;
		else if (strcmp(file->name, "ptrack.map.mmap") == 0)
			return CHECK_FALSE;
		else if (strcmp(file->name, "ptrack.map.tmp") == 0)
			return CHECK_FALSE;
		/* Do not backup temp files */
		else if (file->name[0] == 't' && isdigit(file->name[1]))
			return CHECK_FALSE;
		else if (isdigit(file->name[0]))
		{
			set_forkname(file);

			if (file->forkName == ptrack) /* Compatibility with left-overs from ptrack1 */
				return CHECK_FALSE;
		}
	}

	return CHECK_TRUE;
}

/*
 * Excluding default files from the files list.
 * Input:
 *  parray *files - an array of pgFile* to filter.
 *  croterion_fn - a callback that filters things out
 * Output:
 *  true - if the file must be deleted from the list
 *  false - otherwise
 */

static bool
exclude_files_cb(void *value, void *exclude_args) {
    pgFile *file = (pgFile*) value;
    exclude_cb_ctx *ex_ctx = (exclude_cb_ctx*) exclude_args;

	/*
	 * Check the file relative path for previously excluded dir prefix. These files
	 * should not be in the list, only their empty parent directory, see dir_check_file.
	 *
	 * Assuming that the excluded dir is ALWAYS followed by its content like this:
	 * 		pref/dir/
	 *		pref/dir/file1
	 *		pref/dir/file2
	 *		pref/dir/file3
	 *		...
	 * we can make prefix checks only for files that subsequently follow the excluded dir
	 * and avoid unnecessary checks for the rest of the files. So we store the prefix length,
	 * update it and the prefix itself once we've got a CHECK_EXCLUDE_FALSE status code,
	 * keep doing prefix checks while there are files in that directory and set prefix length
	 * to 0 once they are gone.
	 */
	if(ex_ctx->pref_len > 0
		&& strncmp(ex_ctx->exclude_dir_content_pref, file->rel_path, ex_ctx->pref_len) == 0) {
		return true;
	} else {
		memset(ex_ctx->exclude_dir_content_pref, 0, ex_ctx->pref_len);
		ex_ctx->pref_len = 0;
	}

    int check_res = dir_check_file(file, ex_ctx->backup_logs);

	switch(check_res) {
		case CHECK_FALSE:
			return true;
			break;
		case CHECK_TRUE:;
			return false;
			break;
		case CHECK_EXCLUDE_FALSE:
			// since the excluded dir always goes before its contents, memorize it
			// and use it for further files filtering.
			strcpy(ex_ctx->exclude_dir_content_pref, file->rel_path);
			ex_ctx->pref_len = strlen(file->rel_path);
			return false;
			break;
		default:
			// Should not get there normally.
			assert(false);
			return false;
			break;
	}

	// Should not get there as well.
	return false;
}

void exclude_files(parray *files, bool backup_logs) {
    exclude_cb_ctx ctx = {
		.pref_len = 0,
        .backup_logs = backup_logs,
		.exclude_dir_content_pref = "\0",
    };

    parray_remove_if(files, exclude_files_cb, (void*)&ctx, pgFileFree);
}

/*
 * List files in parent->path directory.
 * If "handle_tablespaces" is true, handle recursive tablespaces
 * and the ones located inside pgdata.
 * If "follow_symlink" is true, follow symlinks so that the
 * fio_stat call fetches the info from the file pointed to by the
 * symlink, not from the symlink itself.
 *
 * TODO: should we check for interrupt here ?
 */
static void
dir_list_file_internal(parray *files, pgFile *parent, const char *parent_dir,
					   bool handle_tablespaces, bool follow_symlink, bool backup_logs,
					   bool skip_hidden, int external_dir_num, fio_location location)
{
	DIR			  *dir;
	struct dirent *dent;
	bool in_tablespace = false;

	if (!S_ISDIR(parent->mode))
		elog(ERROR, "\"%s\" is not a directory", parent_dir);

	in_tablespace = path_is_prefix_of_path(PG_TBLSPC_DIR, parent->rel_path);

	/* Open directory and list contents */
	dir = fio_opendir(location, parent_dir);
	if (dir == NULL)
	{
		if (errno == ENOENT)
		{
			/* Maybe the directory was removed */
			return;
		}
		elog(ERROR, "Cannot open directory \"%s\": %s",
				parent_dir, strerror(errno));
	}

	errno = 0;
	while ((dent = fio_readdir(dir)))
	{
		pgFile	   *file;
		char		child[MAXPGPATH];
		char		rel_child[MAXPGPATH];

		join_path_components(child, parent_dir, dent->d_name);
		join_path_components(rel_child, parent->rel_path, dent->d_name);

		file = pgFileNew(child, rel_child, follow_symlink,
					external_dir_num, location);
		if (file == NULL)
			continue;

		/* Skip entries point current dir or parent dir */
		if (S_ISDIR(file->mode) &&
			(strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0))
		{
			pgFileFree(file);
			continue;
		}

		/* skip hidden files and directories */
		if (skip_hidden && file->name[0] == '.') {
			elog(WARNING, "Skip hidden file: '%s'", child);
			pgFileFree(file);
			continue;
		}

		/*
		 * Add only files, directories and links. Skip sockets and other
		 * unexpected file formats.
		 */
		if (!S_ISDIR(file->mode) && !S_ISREG(file->mode))
		{
			elog(WARNING, "Skip '%s': unexpected file format", child);
			pgFileFree(file);
			continue;
		}

		if(handle_tablespaces) {
			/*
			 * Do not copy tablespaces twice. It may happen if the tablespace is located
			 * inside the PGDATA.
			 */
			if (S_ISDIR(file->mode) &&
				strcmp(file->name, TABLESPACE_VERSION_DIRECTORY) == 0)
			{
				Oid			tblspcOid;
				char		tmp_rel_path[MAXPGPATH];
				int			sscanf_res;
			
				/*
				 * Valid path for the tablespace is
				 * pg_tblspc/tblsOid/TABLESPACE_VERSION_DIRECTORY
				 */
				if (!path_is_prefix_of_path(PG_TBLSPC_DIR, file->rel_path))
					continue;
				sscanf_res = sscanf(file->rel_path, PG_TBLSPC_DIR "/%u/%s",
							&tblspcOid, tmp_rel_path);
				if (sscanf_res == 0)
					continue;
			}
	
			if (in_tablespace) {
	            char        tmp_rel_path[MAXPGPATH];
				ssize_t      sscanf_res;
	        
	            sscanf_res = sscanf(file->rel_path, PG_TBLSPC_DIR "/%u/%[^/]/%u/",
	                                &(file->tblspcOid), tmp_rel_path,
	                                &(file->dbOid));
	        
	            /*
	             * We should skip other files and directories rather than
	             * TABLESPACE_VERSION_DIRECTORY, if this is recursive tablespace.
	             */
	            if (sscanf_res == 2 && strcmp(tmp_rel_path, TABLESPACE_VERSION_DIRECTORY) != 0)
	                continue;
	        } else if (path_is_prefix_of_path("global", file->rel_path)) {
	            file->tblspcOid = GLOBALTABLESPACE_OID;
	        } else if (path_is_prefix_of_path("base", file->rel_path)) {
	            file->tblspcOid = DEFAULTTABLESPACE_OID;
	            sscanf(file->rel_path, "base/%u/", &(file->dbOid));
	        }
		}

		parray_append(files, file);

		/*
		 * If the entry is a directory call dir_list_file_internal()
		 * recursively.
		 */
		if (S_ISDIR(file->mode))
			dir_list_file_internal(files, file, child, handle_tablespaces, follow_symlink,
								   backup_logs, skip_hidden, external_dir_num, location);
	}

	if (errno && errno != ENOENT)
	{
		int			errno_tmp = errno;
		fio_closedir(dir);
		elog(ERROR, "Cannot read directory \"%s\": %s",
				parent_dir, strerror(errno_tmp));
	}
	fio_closedir(dir);
}

/*
 * Retrieve tablespace path, either relocated or original depending on whether
 * -T was passed or not.
 *
 * Copy of function get_tablespace_mapping() from pg_basebackup.c.
 */
const char *
get_tablespace_mapping(const char *dir)
{
	TablespaceListCell *cell;

	for (cell = tablespace_dirs.head; cell; cell = cell->next)
		if (strcmp(dir, cell->old_dir) == 0)
			return cell->new_dir;

	return dir;
}

/*
 * Split argument into old_dir and new_dir and append to mapping
 * list.
 *
 * Copy of function tablespace_list_append() from pg_basebackup.c.
 */
static void
opt_path_map(ConfigOption *opt, const char *arg, TablespaceList *list,
			 const char *type)
{
	TablespaceListCell *cell = pgut_new(TablespaceListCell);
	char	   *dst;
	char	   *dst_ptr;
	const char *arg_ptr;

	memset(cell, 0, sizeof(TablespaceListCell));
	dst_ptr = dst = cell->old_dir;
	for (arg_ptr = arg; *arg_ptr; arg_ptr++)
	{
		if (dst_ptr - dst >= MAXPGPATH)
			elog(ERROR, "directory name too long");

		if (*arg_ptr == '\\' && *(arg_ptr + 1) == '=')
			;					/* skip backslash escaping = */
		else if (*arg_ptr == '=' && (arg_ptr == arg || *(arg_ptr - 1) != '\\'))
		{
			if (*cell->new_dir)
				elog(ERROR, "multiple \"=\" signs in %s mapping\n", type);
			else
				dst = dst_ptr = cell->new_dir;
		}
		else
			*dst_ptr++ = *arg_ptr;
	}

	if (!*cell->old_dir || !*cell->new_dir)
		elog(ERROR, "invalid %s mapping format \"%s\", "
			 "must be \"OLDDIR=NEWDIR\"", type, arg);
	canonicalize_path(cell->old_dir);
	canonicalize_path(cell->new_dir);

	/*
	 * This check isn't absolutely necessary.  But all tablespaces are created
	 * with absolute directories, so specifying a non-absolute path here would
	 * just never match, possibly confusing users.  It's also good to be
	 * consistent with the new_dir check.
	 */
	if (!is_absolute_path(cell->old_dir))
		elog(ERROR, "old directory is not an absolute path in %s mapping: %s\n",
			 type, cell->old_dir);

	if (!is_absolute_path(cell->new_dir))
		elog(ERROR, "new directory is not an absolute path in %s mapping: %s\n",
			 type, cell->new_dir);

	if (list->tail)
		list->tail->next = cell;
	else
		list->head = cell;
	list->tail = cell;
}

/* Parse tablespace mapping */
void
opt_tablespace_map(ConfigOption *opt, const char *arg)
{
	opt_path_map(opt, arg, &tablespace_dirs, "tablespace");
}

/* Parse external directories mapping */
void
opt_externaldir_map(ConfigOption *opt, const char *arg)
{
	opt_path_map(opt, arg, &external_remap_list, "external directory");
}

/*
 * Create directories from **dest_files** in **data_dir**.
 *
 * If **extract_tablespaces** is true then try to extract tablespace data
 * directories into their initial path using tablespace_map file.
 * Use **backup_dir** for tablespace_map extracting.
 *
 * Enforce permissions from backup_content.control. The only
 * problem now is with PGDATA itself.
 * TODO: we must preserve PGDATA permissions somewhere. Is it actually a problem?
 * Shouldn`t starting postgres force correct permissions on PGDATA?
 *
 * TODO: symlink handling. If user located symlink in PG_TBLSPC_DIR, it will
 * be restored as directory.
 */
void
create_data_directories(parray *dest_files, const char *data_dir, const char *backup_dir,
						bool extract_tablespaces, bool incremental, fio_location location, 
						const char* waldir_path)
{
	int			i;
	parray		*links = NULL;
	mode_t		pg_tablespace_mode = DIR_PERMISSION;
	char		to_path[MAXPGPATH];

	if (waldir_path && !dir_is_empty(waldir_path, location))
	{
		elog(ERROR, "WAL directory location is not empty: \"%s\"", waldir_path);
	}


	/* get tablespace map */
	if (extract_tablespaces)
	{
		links = parray_new();
		read_tablespace_map(links, backup_dir);
		/* Sort links by a link name */
		parray_qsort(links, pgFileCompareName);
	}

	/*
	 * We have no idea about tablespace permission
	 * For PG < 11 we can just force default permissions.
	 */
#if PG_VERSION_NUM >= 110000
	if (links)
	{
		/* For PG>=11 we use temp kludge: trust permissions on 'pg_tblspc'
		 * and force them on every tablespace.
		 * TODO: remove kludge and ask data_directory_mode
		 * at the start of backup.
		 */
		for (i = 0; i < parray_num(dest_files); i++)
		{
			pgFile	   *file = (pgFile *) parray_get(dest_files, i);

			if (!S_ISDIR(file->mode))
				continue;

			/* skip external directory content */
			if (file->external_dir_num != 0)
				continue;

			/* look for 'pg_tblspc' directory  */
			if (strcmp(file->rel_path, PG_TBLSPC_DIR) == 0)
			{
				pg_tablespace_mode = file->mode;
				break;
			}
		}
	}
#endif

	/*
	 * We iterate over dest_files and for every directory with parent 'pg_tblspc'
	 * we must lookup this directory name in tablespace map.
	 * If we got a match, we treat this directory as tablespace.
	 * It means that we create directory specified in tablespace_map and
	 * original directory created as symlink to it.
	 */

	elog(LOG, "Restore directories and symlinks...");

	/* create directories */
	for (i = 0; i < parray_num(dest_files); i++)
	{
		char parent_dir[MAXPGPATH];
		pgFile	   *dir = (pgFile *) parray_get(dest_files, i);

		if (!S_ISDIR(dir->mode))
			continue;

		/* skip external directory content */
		if (dir->external_dir_num != 0)
			continue;
		/* Create WAL directory and symlink if waldir_path is setting */
		if (waldir_path && strcmp(dir->rel_path, PG_XLOG_DIR) == 0) {
			/* get full path to PG_XLOG_DIR */

			join_path_components(to_path, data_dir, PG_XLOG_DIR);

			elog(VERBOSE, "Create directory \"%s\" and symbolic link \"%s\"",
				waldir_path, to_path);

			/* create tablespace directory from waldir_path*/
			fio_mkdir(location, waldir_path, pg_tablespace_mode, false);

			/* create link to linked_path */
			if (fio_symlink(location, waldir_path, to_path, incremental) < 0)
				elog(ERROR, "Could not create symbolic link \"%s\": %s",
					to_path, strerror(errno));

			continue;


		}

		/* tablespace_map exists */
		if (links)
		{
			/* get parent dir of rel_path */
			strncpy(parent_dir, dir->rel_path, MAXPGPATH);
			get_parent_directory(parent_dir);

			/* check if directory is actually link to tablespace */
			if (strcmp(parent_dir, PG_TBLSPC_DIR) == 0)
			{
				/* this directory located in pg_tblspc
				 * check it against tablespace map
				 */
				pgFile **link = (pgFile **) parray_bsearch(links, dir, pgFileCompareName);

				/* got match */
				if (link)
				{
					const char *linked_path = get_tablespace_mapping((*link)->linked);

					if (!is_absolute_path(linked_path))
							elog(ERROR, "Tablespace directory path must be an absolute path: %s\n",
								 linked_path);

					join_path_components(to_path, data_dir, dir->rel_path);

					elog(LOG, "Create directory \"%s\" and symbolic link \"%s\"",
							 linked_path, to_path);

					/* create tablespace directory */
					fio_mkdir(location, linked_path, pg_tablespace_mode, false);

					/* create link to linked_path */
					if (fio_symlink(location, linked_path, to_path, incremental) < 0)
						elog(ERROR, "Could not create symbolic link \"%s\": %s",
							 to_path, strerror(errno));

					continue;
				}
			}
		}

		/* This is not symlink, create directory */
		elog(LOG, "Create directory \"%s\"", dir->rel_path);

		join_path_components(to_path, data_dir, dir->rel_path);

		// TODO check exit code
		fio_mkdir(location, to_path, dir->mode, false);
	}

	if (extract_tablespaces)
	{
		parray_walk(links, pgFileFree);
		parray_free(links);
	}
}

/*
 * Read names of symbolic names of tablespaces with links to directories from
 * tablespace_map or tablespace_map.txt.
 */
void
read_tablespace_map(parray *links, const char *backup_dir)
{
	FILE	   *fp;
	char		db_path[MAXPGPATH],
				map_path[MAXPGPATH];
	char		buf[MAXPGPATH * 2];

	join_path_components(db_path, backup_dir, DATABASE_DIR);
	join_path_components(map_path, db_path, PG_TABLESPACE_MAP_FILE);

	fp = fio_open_stream(FIO_BACKUP_HOST, map_path);
	if (fp == NULL)
		elog(ERROR, "Cannot open tablespace map file \"%s\": %s", map_path, strerror(errno));

	while (fgets(buf, lengthof(buf), fp))
	{
		char        link_name[MAXPGPATH];
		char       *path;
		int         n = 0;
		pgFile     *file;
		int         i = 0;

		if (sscanf(buf, "%s %n", link_name, &n) != 1)
			elog(ERROR, "invalid format found in \"%s\"", map_path);

		path = buf + n;

		/* Remove newline character at the end of string if any  */
		i = strcspn(path, "\n");
		if (strlen(path) > i)
			path[i] = '\0';

		file = pgut_new(pgFile);
		memset(file, 0, sizeof(pgFile));

		/* follow the convention for pgFileFree */
		file->name = pgut_strdup(link_name);
		file->linked = pgut_strdup(path);
		canonicalize_path(file->linked);

		parray_append(links, file);
	}

	if (ferror(fp))
			elog(ERROR, "Failed to read from file: \"%s\"", map_path);

	fio_close_stream(fp);
}

/*
 * Check that all tablespace mapping entries have correct linked directory
 * paths. Linked directories must be empty or do not exist, unless
 * we are running incremental restore, then linked directories can be nonempty.
 *
 * If tablespace-mapping option is supplied, all OLDDIR entries must have
 * entries in tablespace_map file.
 *
 * When running incremental restore with tablespace remapping, then
 * new tablespace directory MUST be empty, because there is no way
 * we can be sure, that files laying there belong to our instance.
 * But "force" flag allows to ignore this condition, by wiping out
 * the current content on the directory.
 *
 * Exit codes:
 *  1. backup has no tablespaces
 *  2. backup has tablespaces and they are empty
 *  3. backup has tablespaces and some of them are not empty
 */
int
check_tablespace_mapping(pgBackup *backup, bool incremental, bool force, bool pgdata_is_empty, bool no_validate)
{
	parray	   *links = parray_new();
	size_t		i;
	TablespaceListCell *cell;
	pgFile	   *tmp_file = pgut_new(pgFile);
	bool        tblspaces_are_empty = true;

	elog(LOG, "Checking tablespace directories of backup %s",
			base36enc(backup->start_time));

	/* validate tablespace map,
	 * if there are no tablespaces, then there is nothing left to do
	 */
	if (!validate_tablespace_map(backup, no_validate))
	{
		/*
		 * Sanity check
		 * If there is no tablespaces in backup,
		 * then using the '--tablespace-mapping' option is a mistake.
		 */
		if (tablespace_dirs.head != NULL)
			elog(ERROR, "Backup %s has no tablespaceses, nothing to remap "
					"via \"--tablespace-mapping\" option", base36enc(backup->backup_id));
		return NoTblspc;
	}

	read_tablespace_map(links, backup->root_dir);
	/* Sort links by the path of a linked file*/
	parray_qsort(links, pgFileCompareLinked);

	/* 1 - each OLDDIR must have an entry in tablespace_map file (links) */
	for (cell = tablespace_dirs.head; cell; cell = cell->next)
	{
		tmp_file->linked = cell->old_dir;

		if (parray_bsearch(links, tmp_file, pgFileCompareLinked) == NULL)
			elog(ERROR, "--tablespace-mapping option's old directory "
				 "doesn't have an entry in tablespace_map file: \"%s\"",
				 cell->old_dir);
	}

	/*
	 * There is difference between incremental restore of already existing
	 * tablespaceses and remapped tablespaceses.
	 * Former are allowed to be not empty, because we treat them like an
	 * extension of PGDATA.
	 * The latter are not, unless "--force" flag is used.
	 * in which case the remapped directory is nuked - just to be safe,
	 * because it is hard to be sure that there are no some tricky corner
	 * cases of pages from different systems having the same crc.
	 * This is a strict approach.
	 *
	 * Why can`t we not nuke it and just let it roll ?
	 * What if user just wants to rerun failed restore with the same
	 * parameters? Nuking is bad for this case.
	 *
	 * Consider the example of existing PGDATA:
	 * ....
	 * 	pg_tablespace
	 * 		100500-> /somedirectory
	 * ....
	 *
	 * We want to remap it during restore like that:
	 * ....
	 * 	pg_tablespace
	 * 		100500-> /somedirectory1
	 * ....
	 *
	 * Usually it is required for "/somedirectory1" to be empty, but
	 * in case of incremental restore with 'force' flag, which required
	 * of us to drop already existing content of "/somedirectory1".
	 *
	 * TODO: Ideally in case of incremental restore we must also
	 * drop the "/somedirectory" directory first, but currently
	 * we don`t do that.
	 */

	/* 2 - all linked directories must be empty */
	for (i = 0; i < parray_num(links); i++)
	{
		pgFile	   *link = (pgFile *) parray_get(links, i);
		const char *linked_path = link->linked;
		TablespaceListCell *cell;
		bool remapped = false;

		for (cell = tablespace_dirs.head; cell; cell = cell->next)
		{
			if (strcmp(link->linked, cell->old_dir) == 0)
			{
				linked_path = cell->new_dir;
				remapped = true;
				break;
			}
		}

		if (remapped)
			elog(INFO, "Tablespace %s will be remapped from \"%s\" to \"%s\"", link->name, cell->old_dir, cell->new_dir);
		else
			elog(INFO, "Tablespace %s will be restored using old path \"%s\"", link->name, linked_path);

		if (!is_absolute_path(linked_path))
			elog(ERROR, "Tablespace directory path must be an absolute path: %s\n",
				 linked_path);

		if (!dir_is_empty(linked_path, FIO_DB_HOST))
		{

			if (!incremental)
				elog(ERROR, "Restore tablespace destination is not empty: \"%s\"", linked_path);

			else if (remapped && !force)
				elog(ERROR, "Remapped tablespace destination is not empty: \"%s\". "
							"Use \"--force\" flag if you want to automatically clean up the "
							"content of new tablespace destination",
						linked_path);

			else if (pgdata_is_empty && !force)
				elog(ERROR, "PGDATA is empty, but tablespace destination is not: \"%s\". "
							"Use \"--force\" flag is you want to automatically clean up the "
							"content of tablespace destination",
						linked_path);

			/*
			 * TODO: compile the list of tblspc Oids to delete later,
			 * similar to what we do with database_map.
			 */
			else if (force && (pgdata_is_empty || remapped))
			{
				elog(WARNING, "Cleaning up the content of %s directory: \"%s\"",
						remapped ? "remapped tablespace" : "tablespace", linked_path);
				cleanup_tablespace(linked_path);
				continue;
			}

			tblspaces_are_empty = false;
		}
	}

	free(tmp_file);
	parray_walk(links, pgFileFree);
	parray_free(links);

	if (tblspaces_are_empty)
		return EmptyTblspc;

	return NotEmptyTblspc;
}

/* TODO: Make it consistent with check_tablespace_mapping */
void
check_external_dir_mapping(pgBackup *backup, bool incremental)
{
	TablespaceListCell *cell;
	parray *external_dirs_to_restore;
	int		i;

	elog(LOG, "check external directories of backup %s",
			base36enc(backup->start_time));

	if (!backup->external_dir_str)
	{
	 	if (external_remap_list.head)
			elog(ERROR, "--external-mapping option's old directory doesn't "
				 "have an entry in list of external directories of current "
				 "backup: \"%s\"", external_remap_list.head->old_dir);
		return;
	}

	external_dirs_to_restore = make_external_directory_list(
													backup->external_dir_str,
													false);
	/* 1 - each OLDDIR must have an entry in external_dirs_to_restore */
	for (cell = external_remap_list.head; cell; cell = cell->next)
	{
		bool		found = false;

		for (i = 0; i < parray_num(external_dirs_to_restore); i++)
		{
			char	    *external_dir = parray_get(external_dirs_to_restore, i);

			if (strcmp(cell->old_dir, external_dir) == 0)
			{
				/* Swap new dir name with old one, it is used by 2-nd step */
				parray_set(external_dirs_to_restore, i,
						   pgut_strdup(cell->new_dir));
				pfree(external_dir);

				found = true;
				break;
			}
		}
		if (!found)
			elog(ERROR, "--external-mapping option's old directory doesn't "
				 "have an entry in list of external directories of current "
				 "backup: \"%s\"", cell->old_dir);
	}

	/* 2 - all linked directories must be empty */
	for (i = 0; i < parray_num(external_dirs_to_restore); i++)
	{
		char	    *external_dir = (char *) parray_get(external_dirs_to_restore,
														i);

		if (!incremental && !dir_is_empty(external_dir, FIO_DB_HOST))
			elog(ERROR, "External directory is not empty: \"%s\"",
				 external_dir);
	}

	free_dir_list(external_dirs_to_restore);
}

char *
get_external_remap(char *current_dir)
{
	TablespaceListCell *cell;

	for (cell = external_remap_list.head; cell; cell = cell->next)
	{
		char *old_dir = cell->old_dir;

		if (strcmp(old_dir, current_dir) == 0)
			return cell->new_dir;
	}
	return current_dir;
}

/* Parsing states for get_control_value_str() */
#define CONTROL_WAIT_NAME			1
#define CONTROL_INNAME				2
#define CONTROL_WAIT_COLON			3
#define CONTROL_WAIT_VALUE			4
#define CONTROL_INVALUE				5
#define CONTROL_WAIT_NEXT_NAME		6

/*
 * Get value from json-like line "str" of backup_content.control file.
 *
 * The line has the following format:
 *   {"name1":"value1", "name2":"value2"}
 *
 * The value will be returned in "value_int64" as int64.
 *
 * Returns true if the value was found in the line and parsed.
 */
bool
get_control_value_int64(const char *str, const char *name, int64 *value_int64, bool is_mandatory)
{

	char buf_int64[32];

	assert(value_int64);

    /* Set default value */
    *value_int64 = 0;

	if (!get_control_value_str(str, name, buf_int64, sizeof(buf_int64), is_mandatory))
		return false;

	if (!parse_int64(buf_int64, value_int64, 0))
	{
		/* We assume that too big value is -1 */
		if (errno == ERANGE)
			*value_int64 = BYTES_INVALID;
		else
			control_string_bad_format(str);
        return false;
	}

	return true;
}

/*
 * Get value from json-like line "str" of backup_content.control file.
 *
 * The line has the following format:
 *   {"name1":"value1", "name2":"value2"}
 *
 * The value will be returned to "value_str" as string.
 *
 * Returns true if the value was found in the line.
 */

bool
get_control_value_str(const char *str, const char *name,
                      char *value_str, size_t value_str_size, bool is_mandatory)
{
	int			state = CONTROL_WAIT_NAME;
	char	   *name_ptr = (char *) name;
	char	   *buf = (char *) str;
	char 	   *const value_str_start = value_str;

	assert(value_str);
	assert(value_str_size > 0);

	/* Set default value */
	*value_str = '\0';

	while (*buf)
	{
		switch (state)
		{
			case CONTROL_WAIT_NAME:
				if (*buf == '"')
					state = CONTROL_INNAME;
				else if (IsAlpha(*buf))
					control_string_bad_format(str);
				break;
			case CONTROL_INNAME:
				/* Found target field. Parse value. */
				if (*buf == '"')
					state = CONTROL_WAIT_COLON;
				/* Check next field */
				else if (*buf != *name_ptr)
				{
					name_ptr = (char *) name;
					state = CONTROL_WAIT_NEXT_NAME;
				}
				else
					name_ptr++;
				break;
			case CONTROL_WAIT_COLON:
				if (*buf == ':')
					state = CONTROL_WAIT_VALUE;
				else if (!IsSpace(*buf))
					control_string_bad_format(str);
				break;
			case CONTROL_WAIT_VALUE:
				if (*buf == '"')
				{
					state = CONTROL_INVALUE;
				}
				else if (IsAlpha(*buf))
					control_string_bad_format(str);
				break;
			case CONTROL_INVALUE:
				/* Value was parsed, exit */
				if (*buf == '"')
				{
					*value_str = '\0';
					return true;
				}
				else
				{
					/* verify if value_str not exceeds value_str_size limits */
					if (value_str - value_str_start >= value_str_size - 1) {
						elog(ERROR, "field \"%s\" is out of range in the line %s of the file %s",
							 name, str, DATABASE_FILE_LIST);
					}
					*value_str = *buf;
					value_str++;
				}
				break;
			case CONTROL_WAIT_NEXT_NAME:
				if (*buf == ',')
					state = CONTROL_WAIT_NAME;
				break;
			default:
				/* Should not happen */
				break;
		}

		buf++;
	}

	/* There is no close quotes */
	if (state == CONTROL_INNAME || state == CONTROL_INVALUE)
		control_string_bad_format(str);

	/* Did not find target field */
	if (is_mandatory)
		elog(ERROR, "field \"%s\" is not found in the line %s of the file %s",
			 name, str, DATABASE_FILE_LIST);
	return false;
}

static void
control_string_bad_format(const char* str)
{
    elog(ERROR, "%s file has invalid format in line %s",
         DATABASE_FILE_LIST, str);
}

/*
 * Check if directory empty.
 */
bool
dir_is_empty(const char *path, fio_location location)
{
	DIR		   *dir;
	struct dirent *dir_ent;

	dir = fio_opendir(location, path);
	if (dir == NULL)
	{
		/* Directory in path doesn't exist */
		if (errno == ENOENT)
			return true;
		elog(ERROR, "cannot open directory \"%s\": %s", path, strerror(errno));
	}

	errno = 0;
	while ((dir_ent = fio_readdir(dir)))
	{
		/* Skip entries point current dir or parent dir */
		if (strcmp(dir_ent->d_name, ".") == 0 ||
			strcmp(dir_ent->d_name, "..") == 0)
			continue;

		/* Directory is not empty */
		fio_closedir(dir);
		return false;
	}
	if (errno)
		elog(ERROR, "cannot read directory \"%s\": %s", path, strerror(errno));

	fio_closedir(dir);

	return true;
}

/*
 * Return true if the path is a existing regular file.
 */
bool
fileExists(const char *path, fio_location location)
{
	struct stat buf;

	if (fio_stat(location, path, &buf, true) == -1 && errno == ENOENT)
		return false;
	else if (!S_ISREG(buf.st_mode))
		return false;
	else
		return true;
}

/*
 * Construct parray containing remapped external directories paths
 * from string like /path1:/path2
 */
parray *
make_external_directory_list(const char *colon_separated_dirs, bool remap)
{
	char	   *p;
	parray	   *list = parray_new();
	char	   *tmp = pg_strdup(colon_separated_dirs);

#ifndef WIN32
#define EXTERNAL_DIRECTORY_DELIMITER ":"
#else
#define EXTERNAL_DIRECTORY_DELIMITER ";"
#endif

	p = strtok(tmp, EXTERNAL_DIRECTORY_DELIMITER);
	while(p!=NULL)
	{
		char	   *external_path = pg_strdup(p);

		canonicalize_path(external_path);
		if (is_absolute_path(external_path))
		{
			if (remap)
			{
				char	   *full_path = get_external_remap(external_path);

				if (full_path != external_path)
				{
					full_path = pg_strdup(full_path);
					pfree(external_path);
					external_path = full_path;
				}
			}
			parray_append(list, external_path);
		}
		else
			elog(ERROR, "External directory \"%s\" is not an absolute path",
				 external_path);

		p = strtok(NULL, EXTERNAL_DIRECTORY_DELIMITER);
	}
	pfree(tmp);
	parray_qsort(list, pgCompareString);
	return list;
}

/* Free memory of parray containing strings */
void
free_dir_list(parray *list)
{
	parray_walk(list, pfree);
	parray_free(list);
}

/* Append to string "path_prefix" int "dir_num" */
void
makeExternalDirPathByNum(char *ret_path, const char *path_prefix, const int dir_num)
{
	sprintf(ret_path, "%s%d", path_prefix, dir_num);
}

/* Check if "dir" presents in "dirs_list" */
bool
backup_contains_external(const char *dir, parray *dirs_list)
{
	void *search_result;

	if (!dirs_list) /* There is no external dirs in backup */
		return false;
	search_result = parray_bsearch(dirs_list, dir, pgCompareString);
	return search_result != NULL;
}

/*
 * Print database_map
 */
void
print_database_map(FILE *out, parray *database_map)
{
	int i;

	for (i = 0; i < parray_num(database_map); i++)
	{
		db_map_entry *db_entry = (db_map_entry *) parray_get(database_map, i);

		fio_fprintf(out, "{\"dbOid\":\"%u\", \"datname\":\"%s\"}\n",
				db_entry->dbOid, db_entry->datname);
	}

}

/*
 * Create file 'database_map' and add its meta to backup_files_list
 * NULL check for database_map must be done by the caller.
 */
void
write_database_map(pgBackup *backup, parray *database_map, parray *backup_files_list)
{
	FILE		*fp;
	pgFile		*file;
	char		database_dir[MAXPGPATH];
	char		database_map_path[MAXPGPATH];

	join_path_components(database_dir, backup->root_dir, DATABASE_DIR);
	join_path_components(database_map_path, database_dir, DATABASE_MAP);

	fp = fio_fopen(FIO_BACKUP_HOST, database_map_path, PG_BINARY_W);
	if (fp == NULL)
		elog(ERROR, "Cannot open database map \"%s\": %s", database_map_path,
			 strerror(errno));

	print_database_map(fp, database_map);
	if (fio_fflush(fp) || fio_fclose(fp))
	{
		int save_errno = errno;
		if (fio_remove(FIO_BACKUP_HOST, database_map_path, false) != 0)
			elog(WARNING, "Cannot cleanup database map \"%s\": %s", database_map_path, strerror(errno));
		elog(ERROR, "Cannot write database map \"%s\": %s",
			 database_map_path, strerror(save_errno));
	}

	/* Add metadata to backup_content.control */
	file = pgFileNew(database_map_path, DATABASE_MAP, true, 0,
								 FIO_BACKUP_HOST);
	file->crc = pgFileGetCRC32C(database_map_path, false);
	file->write_size = file->size;
	file->uncompressed_size = file->read_size;

	parray_append(backup_files_list, file);
}

/*
 * read database map, return NULL if database_map in empty or missing
 */
parray *
read_database_map(pgBackup *backup)
{
	FILE		*fp;
	parray 		*database_map;
	char		buf[MAXPGPATH];
	char		path[MAXPGPATH];
	char		database_map_path[MAXPGPATH];

	join_path_components(path, backup->root_dir, DATABASE_DIR);
	join_path_components(database_map_path, path, DATABASE_MAP);

	fp = fio_open_stream(FIO_BACKUP_HOST, database_map_path);
	if (fp == NULL)
	{
		/* It is NOT ok for database_map to be missing at this point, so
		 * we should error here.
		 * It`s a job of the caller to error if database_map is not empty.
		 */
		elog(ERROR, "Cannot open \"%s\": %s", database_map_path, strerror(errno));
	}

	database_map = parray_new();

	while (fgets(buf, lengthof(buf), fp))
	{
		char datname[MAXPGPATH];
		int64 dbOid;

		db_map_entry *db_entry = (db_map_entry *) pgut_malloc(sizeof(db_map_entry));

        get_control_value_int64(buf, "dbOid", &dbOid, true);
        get_control_value_str(buf, "datname", datname, sizeof(datname), true);

		db_entry->dbOid = dbOid;
		db_entry->datname = pgut_strdup(datname);

		parray_append(database_map, db_entry);
	}

	if (ferror(fp))
			elog(ERROR, "Failed to read from file: \"%s\"", database_map_path);

	fio_close_stream(fp);

	/* Return NULL if file is empty */
	if (parray_num(database_map) == 0)
	{
		parray_free(database_map);
		return NULL;
	}

	return database_map;
}

/*
 * Use it to cleanup tablespaces
 * TODO: Current algorihtm is not very efficient in remote mode,
 * due to round-trip to delete every file.
 */
void
cleanup_tablespace(const char *path)
{
	pioDrive_i drive = pioDriveForLocation(FIO_BACKUP_HOST);
	$i(pioRemoveDir, drive, .root = path, .root_as_well = false);
}

/*
 * Clear the synchronisation locks in a parray of (pgFile *)'s
 */
void
pfilearray_clear_locks(parray *file_list)
{
	int i;
	for (i = 0; i < parray_num(file_list); i++)
	{
		pgFile *file = (pgFile *) parray_get(file_list, i);
		pg_atomic_clear_flag(&file->lock);
	}
}

static inline bool
is_forkname(char *name, size_t *pos, const char *forkname)
{
	size_t fnlen = strlen(forkname);
	if (strncmp(name + *pos, forkname, fnlen) != 0)
		return false;
	*pos += fnlen;
	return true;
}

#define OIDCHARS 10

/* Set forkName if possible */
bool
set_forkname(pgFile *file)
{
	size_t i = 0;
	uint64_t oid = 0; /* use 64bit to not check for overflow in a loop */

	/* pretend it is not relation file */
	file->relOid = 0;
	file->forkName = none;
	file->is_datafile = false;

	for (i = 0; isdigit(file->name[i]); i++)
	{
		if (i == 0 && file->name[i] == '0')
			return false;
		oid = oid * 10 + file->name[i] - '0';
	}
	if (i == 0 || i > OIDCHARS || oid > UINT32_MAX)
		return false;

	/* usual fork name */
	/* /^\d+_(vm|fsm|init|ptrack)$/ */
	if (is_forkname(file->name, &i, "_vm"))
		file->forkName = vm;
	else if (is_forkname(file->name, &i, "_fsm"))
		file->forkName = fsm;
	else if (is_forkname(file->name, &i, "_init"))
		file->forkName = init;
	else if (is_forkname(file->name, &i, "_ptrack"))
		file->forkName = ptrack;

	/* segment number */
	/* /^\d+(_(vm|fsm|init|ptrack))?\.\d+$/ */
	if (file->name[i] == '.' && isdigit(file->name[i+1]))
	{
		for (i++; isdigit(file->name[i]); i++)
			;
	}

	/* CFS "fork name" */
	if (file->forkName == none &&
		is_forkname(file->name, &i, ".cfm"))
	{
		/* /^\d+(\.\d+)?.cfm$/ */
		file->forkName = cfm;
	}

	/* If there are excess characters, it is not relation file */
	if (file->name[i] != 0)
	{
		file->forkName = none;
		return false;
	}

	file->relOid = oid;
	file->is_datafile = file->forkName == none;
	return true;
}
