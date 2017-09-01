/*-------------------------------------------------------------------------
 *
 * dir.c: directory operation utility.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <libgen.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <time.h>

#include "datapagemap.h"

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
	NULL
};

pgFile *pgFileNew(const char *path, bool omit_symlink);
static int BlackListCompare(const void *str1, const void *str2);

static void dir_list_file_internal(parray *files, const char *root,
								   bool exclude, bool omit_symlink,
								   bool add_root, parray *black_list);

/*
 * Create directory, also create parent directories if necessary.
 */
int
dir_create_dir(const char *dir, mode_t mode)
{
	char		copy[MAXPGPATH];
	char		parent[MAXPGPATH];

	strncpy(copy, dir, MAXPGPATH);
	strncpy(parent, dirname(copy), MAXPGPATH);

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
pgFileNew(const char *path, bool omit_symlink)
{
	struct stat		st;
	pgFile		   *file;

	/* stat the file */
	if ((omit_symlink ? stat(path, &st) : lstat(path, &st)) == -1)
	{
		/* file not found is not an error case */
		if (errno == ENOENT)
			return NULL;
		elog(ERROR, "cannot stat file \"%s\": %s", path,
			strerror(errno));
	}

	file = pgFileInit(path);
	file->size = st.st_size;
	file->mode = st.st_mode;

	return file;
}

pgFile *
pgFileInit(const char *path)
{
	pgFile		   *file;
	file = (pgFile *) pgut_malloc(sizeof(pgFile));

	file->size = 0;
	file->mode = 0;
	file->read_size = 0;
	file->write_size = 0;
	file->crc = 0;
	file->is_datafile = false;
	file->linked = NULL;
	file->pagemap.bitmap = NULL;
	file->pagemap.bitmapsize = 0;
	file->ptrack_path = NULL;
	file->segno = 0;
	file->path = pgut_malloc(strlen(path) + 1);
	strcpy(file->path, path);		/* enough buffer size guaranteed */
	file->is_cfs = false;
	file->generation = 0;
	file->is_partial_copy = false;
	file->compress_alg = NOT_DEFINED_COMPRESS;
	return file;
}

/*
 * Delete file pointed by the pgFile.
 * If the pgFile points directory, the directory must be empty.
 */
void
pgFileDelete(pgFile *file)
{
	if (S_ISDIR(file->mode))
	{
		if (rmdir(file->path) == -1)
		{
			if (errno == ENOENT)
				return;
			else if (errno == ENOTDIR)	/* could be symbolic link */
				goto delete_file;

			elog(ERROR, "cannot remove directory \"%s\": %s",
				file->path, strerror(errno));
		}
		return;
	}

delete_file:
	if (remove(file->path) == -1)
	{
		if (errno == ENOENT)
			return;
		elog(ERROR, "cannot remove file \"%s\": %s", file->path,
			strerror(errno));
	}
}

pg_crc32
pgFileGetCRC(pgFile *file)
{
	FILE	   *fp;
	pg_crc32	crc = 0;
	char		buf[1024];
	size_t		len;
	int			errno_tmp;

	/* open file in binary read mode */
	fp = fopen(file->path, "r");
	if (fp == NULL)
		elog(ERROR, "cannot open file \"%s\": %s",
			file->path, strerror(errno));

	/* calc CRC of backup file */
	INIT_CRC32C(crc);
	while ((len = fread(buf, 1, sizeof(buf), fp)) == sizeof(buf))
	{
		if (interrupted)
			elog(ERROR, "interrupted during CRC calculation");
		COMP_CRC32C(crc, buf, len);
	}
	errno_tmp = errno;
	if (!feof(fp))
		elog(WARNING, "cannot read \"%s\": %s", file->path,
			strerror(errno_tmp));
	if (len > 0)
		COMP_CRC32C(crc, buf, len);
	FIN_CRC32C(crc);

	fclose(fp);

	return crc;
}

void
pgFileFree(void *file)
{
	pgFile	   *file_ptr;

	if (file == NULL)
		return;

	file_ptr = (pgFile *) file;

	if (file_ptr->linked)
		free(file_ptr->linked);
	free(file_ptr->path);
	if (file_ptr->ptrack_path != NULL)
		free(file_ptr->ptrack_path);
	free(file);
}

/* Compare two pgFile with their path in ascending order of ASCII code. */
int
pgFileComparePath(const void *f1, const void *f2)
{
	pgFile *f1p = *(pgFile **)f1;
	pgFile *f2p = *(pgFile **)f2;

	return strcmp(f1p->path, f2p->path);
}

/* Compare two pgFile with their path in descending order of ASCII code. */
int
pgFileComparePathDesc(const void *f1, const void *f2)
{
	return -pgFileComparePath(f1, f2);
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
BlackListCompare(const void *str1, const void *str2)
{
	return strcmp(*(char **) str1, *(char **) str2);
}

/*
 * List files, symbolic links and directories in the directory "root" and add
 * pgFile objects to "files".  We add "root" to "files" if add_root is true.
 *
 * When omit_symlink is true, symbolic link is ignored and only file or
 * directory llnked to will be listed.
 */
void
dir_list_file(parray *files, const char *root, bool exclude, bool omit_symlink,
			  bool add_root)
{
	parray	   *black_list = NULL;
	char		path[MAXPGPATH];

	join_path_components(path, backup_instance_path, PG_BLACK_LIST);
	/* List files with black list */
	if (root && pgdata && strcmp(root, pgdata) == 0 && fileExists(path))
	{
		FILE	   *black_list_file = NULL;
		char		buf[MAXPGPATH * 2];
		char		black_item[MAXPGPATH * 2];

		black_list = parray_new();
		black_list_file = fopen(path, "r");

		if (black_list_file == NULL)
			elog(ERROR, "cannot open black_list: %s", strerror(errno));

		while (fgets(buf, lengthof(buf), black_list_file) != NULL)
		{
			join_path_components(black_item, pgdata, buf);

			if (black_item[strlen(black_item) - 1] == '\n')
				black_item[strlen(black_item) - 1] = '\0';

			if (black_item[0] == '#' || black_item[0] == '\0')
				continue;

			parray_append(black_list, black_item);
		}

		fclose(black_list_file);
		parray_qsort(black_list, BlackListCompare);
	}

	dir_list_file_internal(files, root, exclude, omit_symlink, add_root,
						   black_list);
	parray_qsort(files, pgFileComparePath);
}

/*
 * TODO Add comment
 */
static void
dir_list_file_internal(parray *files, const char *root, bool exclude,
					   bool omit_symlink, bool add_root, parray *black_list)
{
	pgFile *file;

	file = pgFileNew(root, omit_symlink);
	if (file == NULL)
		return;

	/* skip if the file is in black_list defined by user */
	if (black_list && parray_bsearch(black_list, root, BlackListCompare))
	{
		elog(LOG, "Skip file \"%s\": file is in the user's black list", file->path);
		return;
	}

	/*
	 * Add to files list only files, links and directories. Skip sockets and
	 * other unexpected file formats.
	 */
	if (!S_ISDIR(file->mode) && !S_ISLNK(file->mode) &&	!S_ISREG(file->mode))
	{
		elog(WARNING, "Skip file \"%s\": unexpected file format", file->path);
		return;
	}

	if (add_root)
	{
		/* Skip files */
		if (!S_ISDIR(file->mode) && exclude)
		{
			char	    *file_name;
			int			i;

			/* Extract file name */
			file_name = strrchr(file->path, '/');
			if (file_name == NULL)
				file_name = file->path;
			else
				file_name++;

			/* Check if we need to exclude file by name */
			for (i = 0; pgdata_exclude_files[i]; i++)
				if (strcmp(file_name, pgdata_exclude_files[i]) == 0)
					/* Skip */
					return;
		}

		parray_append(files, file);
	}

	/* chase symbolic link chain and find regular file or directory */
	while (S_ISLNK(file->mode))
	{
		ssize_t	len;
		char	linked[MAXPGPATH];

		len = readlink(file->path, linked, sizeof(linked));
		if (len < 0)
			elog(ERROR, "cannot read link \"%s\": %s", file->path,
				strerror(errno));
		if (len >= sizeof(linked))
			elog(ERROR, "symbolic link \"%s\" target is too long\n",
				 file->path);

		linked[len] = '\0';
		file->linked = pgut_strdup(linked);

		/* make absolute path to read linked file */
		if (linked[0] != '/')
		{
			char	dname[MAXPGPATH];
			char	absolute[MAXPGPATH];

			strncpy(dname, file->path, lengthof(dname));
			join_path_components(absolute, dname, linked);
			file = pgFileNew(absolute, omit_symlink);
		}
		else
			file = pgFileNew(file->linked, omit_symlink);

		/* linked file is not found, stop following link chain */
		if (file == NULL)
			return;

		parray_append(files, file);
	}

	/*
	 * If the entry was a directory, add it to the list and add call this
	 * function recursivelly.
	 * If the directory name is in the exclude list, do not list the contents.
	 */
	while (S_ISDIR(file->mode))
	{
		bool			skip = false;
		DIR			    *dir;
		struct dirent   *dent;

		if (exclude)
		{
			int			i;
			char	    *dirname;

			/* skip entry which matches exclude list */
			dirname = strrchr(file->path, '/');
			if (dirname == NULL)
				dirname = file->path;
			else
				dirname++;

			/*
			 * If the item in the exclude list starts with '/', compare to the
			 * absolute path of the directory. Otherwise compare to the directory
			 * name portion.
			 */
			for (i = 0; exclude && pgdata_exclude_dir[i]; i++)
			{
				if (pgdata_exclude_dir[i][0] == '/')
				{
					if (strcmp(file->path, pgdata_exclude_dir[i]) == 0)
					{
						skip = true;
						break;
					}
				}
				else if (strcmp(dirname, pgdata_exclude_dir[i]) == 0)
				{
					skip = true;
					break;
				}
			}
			if (skip)
				break;
		}

		/* open directory and list contents */
		dir = opendir(file->path);
		if (dir == NULL)
		{
			if (errno == ENOENT)
			{
				/* maybe the direcotry was removed */
				return;
			}
			elog(ERROR, "cannot open directory \"%s\": %s",
				file->path, strerror(errno));
		}

		errno = 0;
		while ((dent = readdir(dir)))
		{
			char child[MAXPGPATH];

			/* skip entries point current dir or parent dir */
			if (strcmp(dent->d_name, ".") == 0 ||
				strcmp(dent->d_name, "..") == 0)
				continue;

			join_path_components(child, file->path, dent->d_name);
			dir_list_file_internal(files, child, exclude, omit_symlink, true, black_list);
		}
		if (errno && errno != ENOENT)
		{
			int errno_tmp = errno;
			closedir(dir);
			elog(ERROR, "cannot read directory \"%s\": %s",
				file->path, strerror(errno_tmp));
		}
		closedir(dir);

		break;	/* pseudo loop */
	}
}

/*
 * List data directories excluding directories from
 * pgdata_exclude_dir array.
 *
 * **is_root** is a little bit hack. We exclude only first level of directories
 * and on the first level we check all files and directories.
 */
void
list_data_directories(parray *files, const char *path, bool is_root,
					  bool exclude)
{
	DIR		   *dir;
	struct dirent *dent;
	int			prev_errno;
	bool		has_child_dirs = false;

	/* open directory and list contents */
	dir = opendir(path);
	if (dir == NULL)
		elog(ERROR, "cannot open directory \"%s\": %s", path, strerror(errno));

	errno = 0;
	while ((dent = readdir(dir)))
	{
		char		child[MAXPGPATH];
		bool		skip = false;
		struct stat	st;

		/* skip entries point current dir or parent dir */
		if (strcmp(dent->d_name, ".") == 0 ||
			strcmp(dent->d_name, "..") == 0)
			continue;

		join_path_components(child, path, dent->d_name);

		if (lstat(child, &st) == -1)
			elog(ERROR, "cannot stat file \"%s\": %s", child, strerror(errno));

		if (!S_ISDIR(st.st_mode))
			continue;

		/* Check for exclude for the first level of listing */
		if (is_root && exclude)
		{
			int			i;

			for (i = 0; pgdata_exclude_dir[i]; i++)
			{
				if (strcmp(dent->d_name, pgdata_exclude_dir[i]) == 0)
				{
					skip = true;
					break;
				}
			}
		}
		if (skip)
			continue;

		has_child_dirs = true;
		list_data_directories(files, child, false, exclude);
	}

	/* List only full and last directories */
	if (!is_root && !has_child_dirs)
	{
		pgFile	   *dir;

		dir = pgFileNew(path, false);
		parray_append(files, dir);
	}

	prev_errno = errno;
	closedir(dir);

	if (prev_errno && prev_errno != ENOENT)
		elog(ERROR, "cannot read directory \"%s\": %s",
			 path, strerror(prev_errno));
}

/*
 * Read names of symbolik names of tablespaces with links to directories from
 * tablespace_map or tablespace_map.txt.
 */
void
read_tablespace_map(parray *files, const char *backup_dir)
{
	FILE	   *fp;
	char		db_path[MAXPGPATH],
				map_path[MAXPGPATH];
	char		buf[MAXPGPATH * 2];

	join_path_components(db_path, backup_dir, DATABASE_DIR);
	join_path_components(map_path, db_path, PG_TABLESPACE_MAP_FILE);

	/* Exit if database/tablespace_map doesn't exist */
	if (!fileExists(map_path))
	{
		elog(LOG, "there is no file tablespace_map");
		return;
	}

	fp = fopen(map_path, "rt");
	if (fp == NULL)
		elog(ERROR, "cannot open \"%s\": %s", map_path, strerror(errno));

	while (fgets(buf, lengthof(buf), fp))
	{
		char		link_name[MAXPGPATH],
					path[MAXPGPATH];
		pgFile	   *file;

		if (sscanf(buf, "%1023s %1023s", link_name, path) != 2)
			elog(ERROR, "invalid format found in \"%s\"", map_path);

		file = pgut_new(pgFile);
		memset(file, 0, sizeof(pgFile));

		file->path = pgut_malloc(strlen(link_name) + 1);
		strcpy(file->path, link_name);

		file->linked = pgut_malloc(strlen(path) + 1);
		strcpy(file->linked, path);

		parray_append(files, file);
	}

	parray_qsort(files, pgFileCompareLinked);
	fclose(fp);
}

/*
 * Print backup content list.
 */
void
print_file_list(FILE *out, const parray *files, const char *root)
{
	size_t		i;

	/* print each file in the list */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);
		char	   *path = file->path;

		/* omit root directory portion */
		if (root && strstr(path, root) == path)
			path = GetRelativePath(path, root);

		fprintf(out, "{\"path\":\"%s\", \"size\":\"%lu\",\"mode\":\"%u\","
					 "\"is_datafile\":\"%u\", \"crc\":\"%u\", \"compress_alg\":\"%s\"",
				path, (unsigned long) file->write_size, file->mode,
				file->is_datafile?1:0, file->crc, deparse_compress_alg(file->compress_alg));

		if (file->is_datafile)
			fprintf(out, ",\"segno\":\"%d\"", file->segno);

		if (S_ISLNK(file->mode))
			fprintf(out, ",\"linked\":\"%s\"", file->linked);

#ifdef PGPRO_EE
		if (file->is_cfs)
			fprintf(out, ",\"is_cfs\":\"%u\" ,\"CFS_generation\":\"" UINT64_FORMAT "\","
					"\"is_partial_copy\":\"%u\"",
					file->is_cfs?1:0, file->generation, file->is_partial_copy?1:0);
#endif
		fprintf(out, "}\n");
	}
}

/* Parsing states for get_control_value() */
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
 * The value will be returned to "value_str" as string if it is not NULL. If it
 * is NULL the value will be returned to "value_ulong" as unsigned long.
 */
static void
get_control_value(const char *str, const char *name,
				  char *value_str, uint64 *value_uint64, bool is_mandatory)
{
	int			state = CONTROL_WAIT_NAME;
	char	   *name_ptr = (char *) name;
	char	   *buf = (char *) str;
	char		buf_uint64[32],	/* Buffer for "value_uint64" */
			   *buf_uint64_ptr = buf_uint64;

	/* Set default values */
	if (value_str)
		*value_str = '\0';
	else if (value_uint64)
		*value_uint64 = 0;

	while (*buf)
	{
		switch (state)
		{
			case CONTROL_WAIT_NAME:
				if (*buf == '"')
					state = CONTROL_INNAME;
				else if (IsAlpha(*buf))
					goto bad_format;
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
					goto bad_format;
				break;
			case CONTROL_WAIT_VALUE:
				if (*buf == '"')
				{
					state = CONTROL_INVALUE;
					buf_uint64_ptr = buf_uint64;
				}
				else if (IsAlpha(*buf))
					goto bad_format;
				break;
			case CONTROL_INVALUE:
				/* Value was parsed, exit */
				if (*buf == '"')
				{
					if (value_str)
					{
						*value_str = '\0';
					}
					else if (value_uint64)
					{
						/* Length of buf_uint64 should not be greater than 31 */
						if (buf_uint64_ptr - buf_uint64 >= 32)
							elog(ERROR, "field \"%s\" is out of range in the line %s of the file %s",
								 name, str, DATABASE_FILE_LIST);

						*buf_uint64_ptr = '\0';
						if (!parse_uint64(buf_uint64, value_uint64))
							goto bad_format;
					}

					return;
				}
				else
				{
					if (value_str)
					{
						*value_str = *buf;
						value_str++;
					}
					else
					{
						*buf_uint64_ptr = *buf;
						buf_uint64_ptr++;
					}
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
		goto bad_format;

	/* Did not find target field */
	if (is_mandatory)
		elog(ERROR, "field \"%s\" is not found in the line %s of the file %s",
			 name, str, DATABASE_FILE_LIST);
	return;

bad_format:
	elog(ERROR, "%s file has invalid format in line %s",
		 DATABASE_FILE_LIST, str);
}

/*
 * Construct parray of pgFile from the backup content list.
 * If root is not NULL, path will be absolute path.
 */
parray *
dir_read_file_list(const char *root, const char *file_txt)
{
	FILE   *fp;
	parray *files;
	char	buf[MAXPGPATH * 2];

	fp = fopen(file_txt, "rt");
	if (fp == NULL)
		elog(errno == ENOENT ? ERROR : ERROR,
			"cannot open \"%s\": %s", file_txt, strerror(errno));

	files = parray_new();

	while (fgets(buf, lengthof(buf), fp))
	{
		char		path[MAXPGPATH];
		char		filepath[MAXPGPATH];
		char		linked[MAXPGPATH];
		char		compress_alg_string[MAXPGPATH];
		uint64		write_size,
					mode,		/* bit length of mode_t depends on platforms */
					is_datafile,
					crc,
					segno;
#ifdef PGPRO_EE
		uint64		generation,
					is_partial_copy,
					is_cfs;
#endif
		pgFile	   *file;

		get_control_value(buf, "path", path, NULL, true);
		get_control_value(buf, "size", NULL, &write_size, true);
		get_control_value(buf, "mode", NULL, &mode, true);
		get_control_value(buf, "is_datafile", NULL, &is_datafile, true);
		get_control_value(buf, "crc", NULL, &crc, true);

		/* optional fields */
		get_control_value(buf, "linked", linked, NULL, false);
		get_control_value(buf, "segno", NULL, &segno, false);
		get_control_value(buf, "compress_alg", compress_alg_string, NULL, false);

#ifdef PGPRO_EE
		get_control_value(buf, "is_cfs", NULL, &is_cfs, false);
		get_control_value(buf, "CFS_generation", NULL, &generation, false);
		get_control_value(buf, "is_partial_copy", NULL, &is_partial_copy, false);
#endif
		if (root)
			join_path_components(filepath, root, path);
		else
			strcpy(filepath, path);

		file = pgFileInit(filepath);

		file->write_size = (size_t) write_size;
		file->mode = (mode_t) mode;
		file->is_datafile = is_datafile ? true : false;
		file->crc = (pg_crc32) crc;
		file->compress_alg = parse_compress_alg(compress_alg_string);
		if (linked[0])
			file->linked = pgut_strdup(linked);
		file->segno = (int) segno;
#ifdef PGPRO_EE
		file->is_cfs = is_cfs ? true : false;
		file->generation = generation;
		file->is_partial_copy = is_partial_copy ? true : false;
#endif

		parray_append(files, file);
	}

	fclose(fp);
	return files;
}

/*
 * Check if directory empty.
 */
bool
dir_is_empty(const char *path)
{
	DIR		   *dir;
	struct dirent *dir_ent;

	dir = opendir(path);
	if (dir == NULL)
	{
		/* Directory in path doesn't exist */
		if (errno == ENOENT)
			return true;
		elog(ERROR, "cannot open directory \"%s\": %s", path, strerror(errno));
	}

	errno = 0;
	while ((dir_ent = readdir(dir)))
	{
		/* Skip entries point current dir or parent dir */
		if (strcmp(dir_ent->d_name, ".") == 0 ||
			strcmp(dir_ent->d_name, "..") == 0)
			continue;

		/* Directory is not empty */
		closedir(dir);
		return false;
	}
	if (errno)
		elog(ERROR, "cannot read directory \"%s\": %s", path, strerror(errno));

	closedir(dir);

	return true;
}
