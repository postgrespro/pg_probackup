/*-------------------------------------------------------------------------
 *
 * dir.c: directory operation utility.
 *
 * This file contains:
 *		- pgFile functions;
 *		- functions to walk directory and collect files list
 *		- functions to exclude files from backup;
 *		- postgres specific parsing rules for filenames;
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include "utils/file.h"


#if PG_VERSION_NUM < 110000
#include "catalog/catalog.h"
#endif
#include "catalog/pg_tablespace.h"

#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

#include "utils/configuration.h"

/*
 * The contents of these directories are removed or recreated during server
 * start so they are not included in backups.  The directories themselves are
 * kept and included as empty to preserve access permissions.
 */
const char *pgdata_exclude_dir[] =
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
	NULL
};

static char *pgdata_exclude_files_non_exclusive[] =
{
	/*skip in non-exclusive backup */
	"backup_label",
	"tablespace_map",
	NULL
};


static int pgCompareString(const void *str1, const void *str2);

static char dir_check_file(pgFile *file, bool backup_logs);

static void dir_list_file_internal(parray *files, pgFile *parent, const char *parent_dir,
								   bool exclude, bool follow_symlink, bool backup_logs,
								   bool skip_hidden, int external_dir_num, fio_location location);


/*
 * Create directory, also create parent directories if necessary.
 */
int
dir_create_dir(const char *dir, mode_t mode)
{
	char		parent[MAXPGPATH];

	strncpy(parent, dir, MAXPGPATH);
	get_parent_directory(parent);

	/* Create parent first */
	if (access(parent, F_OK) == -1)
		dir_create_dir(parent, mode);

	/* Create directory */
	if (mkdir(dir, mode) == -1)
	{
		if (errno == EEXIST)	/* already exist */
			return 0;
		elog(ERROR, "cannot create directory \"%s\": %s", dir, strerror(errno));
	}

	return 0;
}

pgFile *
pgFileNew(const char *path, const char *rel_path, bool follow_symlink,
		  int external_dir_num, fio_location location)
{
	struct stat		st;
	pgFile		   *file;

	/* stat the file */
	if (fio_stat(path, &st, follow_symlink, location) < 0)
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

	return file;
}

/*
 * Delete file pointed by the pgFile.
 * If the pgFile points directory, the directory must be empty.
 */
void
pgFileDelete(mode_t mode, const char *full_path)
{
	if (S_ISDIR(mode))
	{
		if (rmdir(full_path) == -1)
		{
			if (errno == ENOENT)
				return;
			else if (errno == ENOTDIR)	/* could be symbolic link */
				goto delete_file;

			elog(ERROR, "Cannot remove directory \"%s\": %s",
				full_path, strerror(errno));
		}
		return;
	}

delete_file:
	if (remove(full_path) == -1)
	{
		if (errno == ENOENT)
			return;
		elog(ERROR, "Cannot remove file \"%s\": %s", full_path,
			strerror(errno));
	}
}

/*
 * Read the local file to compute its CRC.
 * We cannot make decision about file decompression because
 * user may ask to backup already compressed files and we should be
 * obvious about it.
 */
pg_crc32
pgFileGetCRC(const char *file_path, bool use_crc32c, bool missing_ok)
{
	FILE	   *fp;
	pg_crc32	crc = 0;
	char	   *buf;
	size_t		len = 0;

	INIT_FILE_CRC32(use_crc32c, crc);

	/* open file in binary read mode */
	fp = fopen(file_path, PG_BINARY_R);
	if (fp == NULL)
	{
		if (errno == ENOENT)
		{
			if (missing_ok)
			{
				FIN_FILE_CRC32(use_crc32c, crc);
				return crc;
			}
		}

		elog(ERROR, "Cannot open file \"%s\": %s",
			file_path, strerror(errno));
	}

	/* disable stdio buffering */
	setvbuf(fp, NULL, _IONBF, BUFSIZ);
	buf = pgut_malloc(STDIO_BUFSIZE);

	/* calc CRC of file */
	for (;;)
	{
		if (interrupted)
			elog(ERROR, "interrupted during CRC calculation");

		len = fread(buf, 1, STDIO_BUFSIZE, fp);

		if (ferror(fp))
			elog(ERROR, "Cannot read \"%s\": %s", file_path, strerror(errno));

		/* update CRC */
		COMP_FILE_CRC32(use_crc32c, crc, buf, len);

		if (feof(fp))
			break;
	}

	FIN_FILE_CRC32(use_crc32c, crc);
	fclose(fp);
	pg_free(buf);

	return crc;
}

/*
 * Read the local file to compute its CRC.
 * We cannot make decision about file decompression because
 * user may ask to backup already compressed files and we should be
 * obvious about it.
 */
pg_crc32
pgFileGetCRCgz(const char *file_path, bool use_crc32c, bool missing_ok)
{
	gzFile    fp;
	pg_crc32  crc = 0;
	int       len = 0;
	int       err;
	char	 *buf;

	INIT_FILE_CRC32(use_crc32c, crc);

	/* open file in binary read mode */
	fp = gzopen(file_path, PG_BINARY_R);
	if (fp == NULL)
	{
		if (errno == ENOENT)
		{
			if (missing_ok)
			{
				FIN_FILE_CRC32(use_crc32c, crc);
				return crc;
			}
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
		COMP_FILE_CRC32(use_crc32c, crc, buf, len);
	}

	FIN_FILE_CRC32(use_crc32c, crc);
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

static int
pgCompareString(const void *str1, const void *str2)
{
	return strcmp(*(char **) str1, *(char **) str2);
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

/*
 * List files, symbolic links and directories in the directory "root" and add
 * pgFile objects to "files".  We add "root" to "files" if add_root is true.
 *
 * When follow_symlink is true, symbolic link is ignored and only file or
 * directory linked to will be listed.
 */
void
dir_list_file(parray *files, const char *root, bool exclude, bool follow_symlink,
			  bool add_root, bool backup_logs, bool skip_hidden, int external_dir_num,
			  fio_location location)
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
	if (add_root)
		parray_append(files, file);

	dir_list_file_internal(files, file, root, exclude, follow_symlink,
						   backup_logs, skip_hidden, external_dir_num, location);

	if (!add_root)
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
		if (!exclusive_backup)
		{
			for (i = 0; pgdata_exclude_files_non_exclusive[i]; i++)
				if (strcmp(file->rel_path,
						   pgdata_exclude_files_non_exclusive[i]) == 0)
				{
					/* Skip */
					elog(VERBOSE, "Excluding file: %s", file->name);
					return CHECK_FALSE;
				}
		}

		for (i = 0; pgdata_exclude_files[i]; i++)
			if (strcmp(file->rel_path, pgdata_exclude_files[i]) == 0)
			{
				/* Skip */
				elog(VERBOSE, "Excluding file: %s", file->name);
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
				elog(VERBOSE, "Excluding directory content: %s", file->rel_path);
				return CHECK_EXCLUDE_FALSE;
			}
		}

		if (!backup_logs)
		{
			if (strcmp(file->rel_path, PG_LOG_DIR) == 0)
			{
				/* Skip */
				elog(VERBOSE, "Excluding directory content: %s", file->rel_path);
				return CHECK_EXCLUDE_FALSE;
			}
		}
	}

	/*
	 * Do not copy tablespaces twice. It may happen if the tablespace is located
	 * inside the PGDATA.
	 */
	if (S_ISDIR(file->mode) &&
		strcmp(file->name, TABLESPACE_VERSION_DIRECTORY) == 0)
	{
		Oid			tblspcOid;
		char		tmp_rel_path[MAXPGPATH];

		/*
		 * Valid path for the tablespace is
		 * pg_tblspc/tblsOid/TABLESPACE_VERSION_DIRECTORY
		 */
		if (!path_is_prefix_of_path(PG_TBLSPC_DIR, file->rel_path))
			return CHECK_FALSE;
		sscanf_res = sscanf(file->rel_path, PG_TBLSPC_DIR "/%u/%s",
							&tblspcOid, tmp_rel_path);
		if (sscanf_res == 0)
			return CHECK_FALSE;
	}

	if (in_tablespace)
	{
		char		tmp_rel_path[MAXPGPATH];

		sscanf_res = sscanf(file->rel_path, PG_TBLSPC_DIR "/%u/%[^/]/%u/",
							&(file->tblspcOid), tmp_rel_path,
							&(file->dbOid));

		/*
		 * We should skip other files and directories rather than
		 * TABLESPACE_VERSION_DIRECTORY, if this is recursive tablespace.
		 */
		if (sscanf_res == 2 && strcmp(tmp_rel_path, TABLESPACE_VERSION_DIRECTORY) != 0)
			return CHECK_FALSE;

		if (sscanf_res == 3 && S_ISDIR(file->mode) &&
			strcmp(tmp_rel_path, TABLESPACE_VERSION_DIRECTORY) == 0)
			file->is_database = true;
	}
	else if (path_is_prefix_of_path("global", file->rel_path))
	{
		file->tblspcOid = GLOBALTABLESPACE_OID;

		if (S_ISDIR(file->mode) && strcmp(file->name, "global") == 0)
			file->is_database = true;
	}
	else if (path_is_prefix_of_path("base", file->rel_path))
	{
		file->tblspcOid = DEFAULTTABLESPACE_OID;

		sscanf(file->rel_path, "base/%u/", &(file->dbOid));

		if (S_ISDIR(file->mode) && strcmp(file->name, "base") != 0)
			file->is_database = true;
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
			char	   *fork_name;
			int			len;
			char		suffix[MAXPGPATH];

			fork_name = strstr(file->name, "_");
			if (fork_name)
			{
				/* Auxiliary fork of the relfile */
				if (strcmp(fork_name, "vm") == 0)
					file->forkName = vm;

				else if (strcmp(fork_name, "fsm") == 0)
					file->forkName = fsm;

				else if (strcmp(fork_name, "cfm") == 0)
					file->forkName = cfm;

				else if (strcmp(fork_name, "ptrack") == 0)
					file->forkName = ptrack;

				else if (strcmp(fork_name, "init") == 0)
					file->forkName = init;

				/* Do not backup ptrack files */
				if (file->forkName == ptrack)
					return CHECK_FALSE;
			}
			else
			{
               /*
                * snapfs files:
                * RELFILENODE.BLOCKNO.snapmap.SNAPID
                * RELFILENODE.BLOCKNO.snap.SNAPID
                */
               if (strstr(file->name, "snap") != NULL)
                       return true;

				len = strlen(file->name);
				/* reloid.cfm */
				if (len > 3 && strcmp(file->name + len - 3, "cfm") == 0)
					return CHECK_TRUE;

				sscanf_res = sscanf(file->name, "%u.%d.%s", &(file->relOid),
									&(file->segno), suffix);
				if (sscanf_res == 0)
					elog(ERROR, "Cannot parse file name \"%s\"", file->name);
				else if (sscanf_res == 1 || sscanf_res == 2)
					file->is_datafile = true;
			}
		}
	}

	return CHECK_TRUE;
}

/*
 * List files in parent->path directory.  If "exclude" is true do not add into
 * "files" files from pgdata_exclude_files and directories from
 * pgdata_exclude_dir.
 */
static void
dir_list_file_internal(parray *files, pgFile *parent, const char *parent_dir,
					   bool exclude, bool follow_symlink, bool backup_logs,
					   bool skip_hidden, int external_dir_num, fio_location location)
{
	DIR			  *dir;
	struct dirent *dent;

	if (!S_ISDIR(parent->mode))
		elog(ERROR, "\"%s\" is not a directory", parent_dir);

	/* Open directory and list contents */
	dir = fio_opendir(parent_dir, location);
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
		char		check_res;

		join_path_components(child, parent_dir, dent->d_name);
		join_path_components(rel_child, parent->rel_path, dent->d_name);

		file = pgFileNew(child, rel_child, follow_symlink, external_dir_num,
						 location);
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
		if (skip_hidden && file->name[0] == '.')
		{
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

		if (exclude)
		{
			check_res = dir_check_file(file, backup_logs);
			if (check_res == CHECK_FALSE)
			{
				/* Skip */
				pgFileFree(file);
				continue;
			}
			else if (check_res == CHECK_EXCLUDE_FALSE)
			{
				/* We add the directory itself which content was excluded */
				parray_append(files, file);
				continue;
			}
		}

		parray_append(files, file);

		/*
		 * If the entry is a directory call dir_list_file_internal()
		 * recursively.
		 */
		if (S_ISDIR(file->mode))
			dir_list_file_internal(files, file, child, exclude, follow_symlink,
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
 *
 * TODO move tablespace handling into a separate funtions to make code more readable
 */
void
create_data_directories(parray *dest_files, const char *data_dir, const char *backup_dir,
						bool extract_tablespaces, bool incremental, fio_location location,
						TablespaceList *tablespace_dirs)
{
	int			i;
	parray		*links = NULL;
	mode_t		pg_tablespace_mode = DIR_PERMISSION;
	char		to_path[MAXPGPATH];

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
					const char *linked_path = get_tablespace_mapping((*link)->linked, *tablespace_dirs);

					if (!is_absolute_path(linked_path))
							elog(ERROR, "Tablespace directory is not an absolute path: %s\n",
								 linked_path);

					join_path_components(to_path, data_dir, dir->rel_path);

					elog(VERBOSE, "Create directory \"%s\" and symbolic link \"%s\"",
							 linked_path, to_path);

					/* create tablespace directory */
					fio_mkdir(linked_path, pg_tablespace_mode, location);

					/* create link to linked_path */
					if (fio_symlink(linked_path, to_path, incremental, location) < 0)
						elog(ERROR, "Could not create symbolic link \"%s\": %s",
							 to_path, strerror(errno));

					continue;
				}
			}
		}

		/* This is not symlink, create directory */
		elog(VERBOSE, "Create directory \"%s\"", dir->rel_path);

		join_path_components(to_path, data_dir, dir->rel_path);
		fio_mkdir(to_path, dir->mode, location);
	}

	if (extract_tablespaces)
	{
		parray_walk(links, pgFileFree);
		parray_free(links);
	}
}

/*
 * Check if directory empty.
 */
bool
dir_is_empty(const char *path, fio_location location)
{
	DIR		   *dir;
	struct dirent *dir_ent;

	dir = fio_opendir(path, location);
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

	if (fio_stat(path, &buf, true, location) == -1 && errno == ENOENT)
		return false;
	else if (!S_ISREG(buf.st_mode))
		return false;
	else
		return true;
}

size_t
pgFileSize(const char *path)
{
	struct stat buf;

	if (stat(path, &buf) == -1)
		elog(ERROR, "Cannot stat file \"%s\": %s", path, strerror(errno));

	return buf.st_size;
}

/*
 * Construct parray containing remapped external directories paths
 * from string like /path1:/path2
 */
parray *
make_external_directory_list(const char *colon_separated_dirs,
							 TablespaceList *external_remap_list)
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
			if (external_remap_list)
			{
				char	   *full_path = get_external_remap(external_path,
														  *external_remap_list);

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


/* create backup directory in $BACKUP_PATH */
int
pgBackupCreateDir(pgBackup *backup)
{
	int		i;
	char	path[MAXPGPATH];
	parray *subdirs = parray_new();

	parray_append(subdirs, pg_strdup(DATABASE_DIR));

	/* Add external dirs containers */
	if (backup->external_dir_str)
	{
		parray *external_list;

		external_list = make_external_directory_list(backup->external_dir_str,
													 NULL);
		for (i = 0; i < parray_num(external_list); i++)
		{
			char		temp[MAXPGPATH];
			/* Numeration of externaldirs starts with 1 */
			makeExternalDirPathByNum(temp, EXTERNAL_DIR, i+1);
			parray_append(subdirs, pg_strdup(temp));
		}
		free_dir_list(external_list);
	}

	if (!dir_is_empty(backup->root_dir, FIO_BACKUP_HOST))
		elog(ERROR, "backup destination is not empty \"%s\"", path);

	fio_mkdir(backup->root_dir, DIR_PERMISSION, FIO_BACKUP_HOST);

	/* block header map */
	init_header_map(backup);

	/* create directories for actual backup files */
	for (i = 0; i < parray_num(subdirs); i++)
	{
		join_path_components(path, backup->root_dir, parray_get(subdirs, i));
		fio_mkdir(path, DIR_PERMISSION, FIO_BACKUP_HOST);
	}

	free_dir_list(subdirs);
	return 0;
}
