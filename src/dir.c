/*-------------------------------------------------------------------------
 *
 * dir.c: directory operation utility.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#if PG_VERSION_NUM < 110000
#include "catalog/catalog.h"
#endif
#include "catalog/pg_tablespace.h"

#include <libgen.h>
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
	NULL
};

static char *pgdata_exclude_files_non_exclusive[] =
{
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

static bool dir_check_file(const char *root, pgFile *file);
static void dir_list_file_internal(parray *files, const char *root,
								   pgFile *parent, bool exclude,
								   bool omit_symlink, parray *black_list, 
								   int extra_dir_num);

static void list_data_directories(parray *files, const char *path, bool is_root,
								  bool exclude);

/* Tablespace mapping */
static TablespaceList tablespace_dirs = {NULL, NULL};
static TablespaceCreatedList tablespace_created_dirs = {NULL, NULL};

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
pgFileNew(const char *path, bool omit_symlink, int extra_dir_num)
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
	file->extra_dir_num = extra_dir_num;

	return file;
}

pgFile *
pgFileInit(const char *path)
{
	pgFile	   *file;
	char	   *file_name;

	file = (pgFile *) pgut_malloc(sizeof(pgFile));

	file->name = NULL;

	file->size = 0;
	file->mode = 0;
	file->read_size = 0;
	file->write_size = 0;
	file->crc = 0;
	file->is_datafile = false;
	file->linked = NULL;
	file->pagemap.bitmap = NULL;
	file->pagemap.bitmapsize = PageBitmapIsEmpty;
	file->pagemap_isabsent = false;
	file->tblspcOid = 0;
	file->dbOid = 0;
	file->relOid = 0;
	file->segno = 0;
	file->is_database = false;
	file->forkName = pgut_malloc(MAXPGPATH);
	file->forkName[0] = '\0';

	file->path = pgut_malloc(strlen(path) + 1);
	strcpy(file->path, path);		/* enough buffer size guaranteed */

	/* Get file name from the path */
	file_name = strrchr(file->path, '/');
	if (file_name == NULL)
		file->name = file->path;
	else
	{
		file_name++;
		file->name = file_name;
	}

	file->is_cfs = false;
	file->exists_in_prev = false;	/* can change only in Incremental backup. */
	/* Number of blocks readed during backup */
	file->n_blocks = BLOCKNUM_INVALID;
	file->compress_alg = NOT_DEFINED_COMPRESS;
	file->extra_dir_num = 0;
	file->extradir = NULL;
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
pgFileGetCRC(const char *file_path, bool use_crc32c, bool raise_on_deleted,
			 size_t *bytes_read)
{
	FILE	   *fp;
	pg_crc32	crc = 0;
	char		buf[1024];
	size_t		len;
	size_t		total = 0;
	int			errno_tmp;

	INIT_FILE_CRC32(use_crc32c, crc);

	/* open file in binary read mode */
	fp = fopen(file_path, PG_BINARY_R);
	if (fp == NULL)
	{
		if (!raise_on_deleted && errno == ENOENT)
		{
			FIN_FILE_CRC32(use_crc32c, crc);
			return crc;
		}
		else
			elog(ERROR, "cannot open file \"%s\": %s",
				file_path, strerror(errno));
	}

	/* calc CRC of file */
	for (;;)
	{
		if (interrupted)
			elog(ERROR, "interrupted during CRC calculation");

		len = fread(buf, 1, sizeof(buf), fp);
		if(len == 0)
			break;
		/* update CRC */
		COMP_FILE_CRC32(use_crc32c, crc, buf, len);
		total += len;
	}

	if (bytes_read)
		*bytes_read = total;

	errno_tmp = errno;
	if (!feof(fp))
		elog(WARNING, "cannot read \"%s\": %s", file_path,
			strerror(errno_tmp));

	FIN_FILE_CRC32(use_crc32c, crc);
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

	if (file_ptr->forkName)
		free(file_ptr->forkName);

	free(file_ptr->path);
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

/* 
 * Compare two pgFile with their path and extra_dir_num
 * in ascending order of ASCII code.
 */
int
pgFileComparePathWithExtra(const void *f1, const void *f2)
{
	pgFile *f1p = *(pgFile **)f1;
	pgFile *f2p = *(pgFile **)f2;
	int 		res;

	res = strcmp(f1p->path, f2p->path);
	if (!res)
	{
		if (f1p->extra_dir_num > f2p->extra_dir_num)
			return 1;
		else if (f1p->extra_dir_num < f2p->extra_dir_num)
			return -1;
		else
			return 0;
	}
	return res;
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

/* Compare two strings */
int
BlackListCompare(const void *str1, const void *str2)
{
	return strcmp(*(char **) str1, *(char **) str2);
}

/*
 * List files, symbolic links and directories in the directory "root" and add
 * pgFile objects to "files".  We add "root" to "files" if add_root is true.
 *
 * When omit_symlink is true, symbolic link is ignored and only file or
 * directory linked to will be listed.
 */
void
dir_list_file(parray *files, const char *root, bool exclude, bool omit_symlink,
			  bool add_root, int extra_dir_num)
{
	pgFile	   *file;
	parray	   *black_list = NULL;
	char		path[MAXPGPATH];

	join_path_components(path, backup_instance_path, PG_BLACK_LIST);
	/* List files with black list */
	if (root && instance_config.pgdata &&
		strcmp(root, instance_config.pgdata) == 0 && fileExists(path))
	{
		FILE	   *black_list_file = NULL;
		char		buf[MAXPGPATH * 2];
		char		black_item[MAXPGPATH * 2];

		black_list = parray_new();
		black_list_file = fopen(path, PG_BINARY_R);

		if (black_list_file == NULL)
			elog(ERROR, "cannot open black_list: %s", strerror(errno));

		while (fgets(buf, lengthof(buf), black_list_file) != NULL)
		{
			join_path_components(black_item, instance_config.pgdata, buf);

			if (black_item[strlen(black_item) - 1] == '\n')
				black_item[strlen(black_item) - 1] = '\0';

			if (black_item[0] == '#' || black_item[0] == '\0')
				continue;

			parray_append(black_list, black_item);
		}

		fclose(black_list_file);
		parray_qsort(black_list, BlackListCompare);
	}

	file = pgFileNew(root, false, extra_dir_num);
	if (file == NULL)
		return;

	if (!S_ISDIR(file->mode))
	{
		elog(WARNING, "Skip \"%s\": unexpected file format", file->path);
		return;
	}
	if (extra_dir_num)
	{
		file->extradir = pgut_strdup(file->path);
		elog(VERBOSE,"Dir_list_file add root Name: %s Path: %s %s",
			 file->name, file->path, file->extradir);
	}
	if (add_root)
		parray_append(files, file);

	dir_list_file_internal(files, root, file, exclude, omit_symlink, black_list,
						   extra_dir_num);

	if (black_list)
		free_dir_list(black_list);
}

/*
 * Check file or directory.
 *
 * Check for exclude.
 * Extract information about the file parsing its name.
 * Skip files:
 * - skip temp tables files
 * - skip unlogged tables files
 * Set flags for:
 * - database directories
 * - datafiles
 */
static bool
dir_check_file(const char *root, pgFile *file)
{
	const char *rel_path;
	int			i;
	int			sscanf_res;

	/* Check if we need to exclude file by name */
	if (S_ISREG(file->mode))
	{
		if (!exclusive_backup)
		{
			for (i = 0; pgdata_exclude_files_non_exclusive[i]; i++)
				if (strcmp(file->name,
						   pgdata_exclude_files_non_exclusive[i]) == 0)
				{
					/* Skip */
					elog(VERBOSE, "Excluding file: %s", file->name);
					return false;
				}
		}

		for (i = 0; pgdata_exclude_files[i]; i++)
			if (strcmp(file->name, pgdata_exclude_files[i]) == 0)
			{
				/* Skip */
				elog(VERBOSE, "Excluding file: %s", file->name);
				return false;
			}
	}
	/*
	 * If the directory name is in the exclude list, do not list the
	 * contents.
	 */
	else if (S_ISDIR(file->mode))
	{
		/*
		 * If the item in the exclude list starts with '/', compare to
		 * the absolute path of the directory. Otherwise compare to the
		 * directory name portion.
		 */
		for (i = 0; pgdata_exclude_dir[i]; i++)
		{
			/* Full-path exclude*/
			if (pgdata_exclude_dir[i][0] == '/')
			{
				if (strcmp(file->path, pgdata_exclude_dir[i]) == 0)
				{
					elog(VERBOSE, "Excluding directory content: %s",
						 file->name);
					return false;
				}
			}
			else if (strcmp(file->name, pgdata_exclude_dir[i]) == 0)
			{
				elog(VERBOSE, "Excluding directory content: %s",
					 file->name);
				return false;
			}
		}
	}

	rel_path = GetRelativePath(file->path, root);

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
		if (!path_is_prefix_of_path(PG_TBLSPC_DIR, rel_path))
			return false;
		sscanf_res = sscanf(rel_path, PG_TBLSPC_DIR "/%u/%s",
							&tblspcOid, tmp_rel_path);
		if (sscanf_res == 0)
			return false;
	}

	if (path_is_prefix_of_path("global", rel_path))
	{
		file->tblspcOid = GLOBALTABLESPACE_OID;

		if (S_ISDIR(file->mode) && strcmp(file->name, "global") == 0)
			file->is_database = true;
	}
	else if (path_is_prefix_of_path("base", rel_path))
	{
		file->tblspcOid = DEFAULTTABLESPACE_OID;

		sscanf(rel_path, "base/%u/", &(file->dbOid));

		if (S_ISDIR(file->mode) && strcmp(file->name, "base") != 0)
			file->is_database = true;
	}
	else if (path_is_prefix_of_path(PG_TBLSPC_DIR, rel_path))
	{
		char		tmp_rel_path[MAXPGPATH];

		sscanf_res = sscanf(rel_path, PG_TBLSPC_DIR "/%u/%[^/]/%u/",
							&(file->tblspcOid), tmp_rel_path,
							&(file->dbOid));

		if (sscanf_res == 3 && S_ISDIR(file->mode) &&
			strcmp(tmp_rel_path, TABLESPACE_VERSION_DIRECTORY) == 0)
			file->is_database = true;
	}

	/* Do not backup ptrack_init files */
	if (S_ISREG(file->mode) && strcmp(file->name, "ptrack_init") == 0)
		return false;

	/*
	 * Check files located inside database directories including directory
	 * 'global'
	 */
	if (S_ISREG(file->mode) && file->tblspcOid != 0 &&
		file->name && file->name[0])
	{
		if (strcmp(file->name, "pg_internal.init") == 0)
			return false;
		/* Do not backup temp files */
		else if (file->name[0] == 't' && isdigit(file->name[1]))
			return false;
		else if (isdigit(file->name[0]))
		{
			char	   *fork_name;
			int			len;
			char		suffix[MAXPGPATH];

			fork_name = strstr(file->name, "_");
			if (fork_name)
			{
				/* Auxiliary fork of the relfile */
				sscanf(file->name, "%u_%s", &(file->relOid), file->forkName);

				/* Do not backup ptrack files */
				if (strcmp(file->forkName, "ptrack") == 0)
					return false;
			}
			else
			{
				len = strlen(file->name);
				/* reloid.cfm */
				if (len > 3 && strcmp(file->name + len - 3, "cfm") == 0)
					return true;

				sscanf_res = sscanf(file->name, "%u.%d.%s", &(file->relOid),
									&(file->segno), suffix);
				if (sscanf_res == 0)
					elog(ERROR, "Cannot parse file name \"%s\"", file->name);
				else if (sscanf_res == 1 || sscanf_res == 2)
					file->is_datafile = true;
			}
		}
	}

	return true;
}

/*
 * List files in "root" directory.  If "exclude" is true do not add into "files"
 * files from pgdata_exclude_files and directories from pgdata_exclude_dir.
 */
static void
dir_list_file_internal(parray *files, const char *root, pgFile *parent,
		bool exclude, bool omit_symlink, parray *black_list, int extra_dir_num)
{
	DIR		    *dir;
	struct dirent *dent;

	if (!S_ISDIR(parent->mode))
		elog(ERROR, "\"%s\" is not a directory", parent->path);

	/* Open directory and list contents */
	dir = opendir(parent->path);
	if (dir == NULL)
	{
		if (errno == ENOENT)
		{
			/* Maybe the directory was removed */
			return;
		}
		elog(ERROR, "cannot open directory \"%s\": %s",
			 parent->path, strerror(errno));
	}

	errno = 0;
	while ((dent = readdir(dir)))
	{
		pgFile	   *file;
		char		child[MAXPGPATH];

		join_path_components(child, parent->path, dent->d_name);

		file = pgFileNew(child, omit_symlink, extra_dir_num);
		if (file == NULL)
			continue;

		/* Skip entries point current dir or parent dir */
		if (S_ISDIR(file->mode) &&
			(strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0))
		{
			pgFileFree(file);
			continue;
		}

		/*
		 * Add only files, directories and links. Skip sockets and other
		 * unexpected file formats.
		 */
		if (!S_ISDIR(file->mode) && !S_ISREG(file->mode))
		{
			elog(WARNING, "Skip \"%s\": unexpected file format", file->path);
			pgFileFree(file);
			continue;
		}

		/* Skip if the directory is in black_list defined by user */
		if (black_list && parray_bsearch(black_list, file->path,
										 BlackListCompare))
		{
			elog(LOG, "Skip \"%s\": it is in the user's black list", file->path);
			pgFileFree(file);
			continue;
		}

		/* If it is extra dir, remember it */
		if (extra_dir_num)
			file->extradir = parent->extradir;

		/* We add the directory anyway */
		if (S_ISDIR(file->mode))
			parray_append(files, file);

		if (exclude && !dir_check_file(root, file))
		{
			if (S_ISREG(file->mode))
				pgFileFree(file);
			/* Skip */
			continue;
		}

		/* At least add the file */
		if (S_ISREG(file->mode))
			parray_append(files, file);

		/*
		 * If the entry is a directory call dir_list_file_internal()
		 * recursively.
		 */
		if (S_ISDIR(file->mode))
			dir_list_file_internal(files, root, file, exclude, omit_symlink,
								   black_list, extra_dir_num);
	}

	if (errno && errno != ENOENT)
	{
		int			errno_tmp = errno;
		closedir(dir);
		elog(ERROR, "cannot read directory \"%s\": %s",
			 parent->path, strerror(errno_tmp));
	}
	closedir(dir);
}

/*
 * List data directories excluding directories from
 * pgdata_exclude_dir array.
 *
 * **is_root** is a little bit hack. We exclude only first level of directories
 * and on the first level we check all files and directories.
 */
static void
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

		dir = pgFileNew(path, false, false);
		parray_append(files, dir);
	}

	prev_errno = errno;
	closedir(dir);

	if (prev_errno && prev_errno != ENOENT)
		elog(ERROR, "cannot read directory \"%s\": %s",
			 path, strerror(prev_errno));
}

/*
 * Save create directory path into memory. We can use it in next page restore to
 * not raise the error "restore tablespace destination is not empty" in
 * create_data_directories().
 */
static void
set_tablespace_created(const char *link, const char *dir)
{
	TablespaceCreatedListCell *cell = pgut_new(TablespaceCreatedListCell);

	strcpy(cell->link_name, link);
	strcpy(cell->linked_dir, dir);
	cell->next = NULL;

	if (tablespace_created_dirs.tail)
		tablespace_created_dirs.tail->next = cell;
	else
		tablespace_created_dirs.head = cell;
	tablespace_created_dirs.tail = cell;
}

/*
 * Retrieve tablespace path, either relocated or original depending on whether
 * -T was passed or not.
 *
 * Copy of function get_tablespace_mapping() from pg_basebackup.c.
 */
static const char *
get_tablespace_mapping(const char *dir)
{
	TablespaceListCell *cell;

	for (cell = tablespace_dirs.head; cell; cell = cell->next)
		if (strcmp(dir, cell->old_dir) == 0)
			return cell->new_dir;

	return dir;
}

/*
 * Is directory was created when symlink was created in restore_directories().
 */
static const char *
get_tablespace_created(const char *link)
{
	TablespaceCreatedListCell *cell;

	for (cell = tablespace_created_dirs.head; cell; cell = cell->next)
		if (strcmp(link, cell->link_name) == 0)
			return cell->linked_dir;

	return NULL;
}

/*
 * Split argument into old_dir and new_dir and append to tablespace mapping
 * list.
 *
 * Copy of function tablespace_list_append() from pg_basebackup.c.
 */
void
opt_tablespace_map(ConfigOption *opt, const char *arg)
{
	TablespaceListCell *cell = pgut_new(TablespaceListCell);
	char	   *dst;
	char	   *dst_ptr;
	const char *arg_ptr;

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
				elog(ERROR, "multiple \"=\" signs in tablespace mapping\n");
			else
				dst = dst_ptr = cell->new_dir;
		}
		else
			*dst_ptr++ = *arg_ptr;
	}

	if (!*cell->old_dir || !*cell->new_dir)
		elog(ERROR, "invalid tablespace mapping format \"%s\", "
			 "must be \"OLDDIR=NEWDIR\"", arg);

	/*
	 * This check isn't absolutely necessary.  But all tablespaces are created
	 * with absolute directories, so specifying a non-absolute path here would
	 * just never match, possibly confusing users.  It's also good to be
	 * consistent with the new_dir check.
	 */
	if (!is_absolute_path(cell->old_dir))
		elog(ERROR, "old directory is not an absolute path in tablespace mapping: %s\n",
			 cell->old_dir);

	if (!is_absolute_path(cell->new_dir))
		elog(ERROR, "new directory is not an absolute path in tablespace mapping: %s\n",
			 cell->new_dir);

	if (tablespace_dirs.tail)
		tablespace_dirs.tail->next = cell;
	else
		tablespace_dirs.head = cell;
	tablespace_dirs.tail = cell;
}

/*
 * Create backup directories from **backup_dir** to **data_dir**. Doesn't raise
 * an error if target directories exist.
 *
 * If **extract_tablespaces** is true then try to extract tablespace data
 * directories into their initial path using tablespace_map file.
 */
void
create_data_directories(const char *data_dir, const char *backup_dir,
						bool extract_tablespaces)
{
	parray	   *dirs,
			   *links = NULL;
	size_t		i;
	char		backup_database_dir[MAXPGPATH],
				to_path[MAXPGPATH];

	dirs = parray_new();
	if (extract_tablespaces)
	{
		links = parray_new();
		read_tablespace_map(links, backup_dir);
		/* Sort links by a link name*/
		parray_qsort(links, pgFileComparePath);
	}

	join_path_components(backup_database_dir, backup_dir, DATABASE_DIR);
	list_data_directories(dirs, backup_database_dir, true, false);

	elog(LOG, "restore directories and symlinks...");

	for (i = 0; i < parray_num(dirs); i++)
	{
		pgFile	   *dir = (pgFile *) parray_get(dirs, i);
		char	   *relative_ptr = GetRelativePath(dir->path, backup_database_dir);

		Assert(S_ISDIR(dir->mode));

		/* Try to create symlink and linked directory if necessary */
		if (extract_tablespaces &&
			path_is_prefix_of_path(PG_TBLSPC_DIR, relative_ptr))
		{
			char	   *link_ptr = GetRelativePath(relative_ptr, PG_TBLSPC_DIR),
					   *link_sep,
					   *tmp_ptr;
			char		link_name[MAXPGPATH];
			pgFile	  **link;

			/* Extract link name from relative path */
			link_sep = first_dir_separator(link_ptr);
			if (link_sep != NULL)
			{
				int			len = link_sep - link_ptr;
				strncpy(link_name, link_ptr, len);
				link_name[len] = '\0';
			}
			else
				goto create_directory;

			tmp_ptr = dir->path;
			dir->path = link_name;
			/* Search only by symlink name without path */
			link = (pgFile **) parray_bsearch(links, dir, pgFileComparePath);
			dir->path = tmp_ptr;

			if (link)
			{
				const char *linked_path = get_tablespace_mapping((*link)->linked);
				const char *dir_created;

				if (!is_absolute_path(linked_path))
					elog(ERROR, "tablespace directory is not an absolute path: %s\n",
						 linked_path);

				/* Check if linked directory was created earlier */
				dir_created = get_tablespace_created(link_name);
				if (dir_created)
				{
					/*
					 * If symlink and linked directory were created do not
					 * create it second time.
					 */
					if (strcmp(dir_created, linked_path) == 0)
					{
						/*
						 * Create rest of directories.
						 * First check is there any directory name after
						 * separator.
						 */
						if (link_sep != NULL && *(link_sep + 1) != '\0')
							goto create_directory;
						else
							continue;
					}
					else
						elog(ERROR, "tablespace directory \"%s\" of page backup does not "
							 "match with previous created tablespace directory \"%s\" of symlink \"%s\"",
							 linked_path, dir_created, link_name);
				}

				if (link_sep)
					elog(LOG, "create directory \"%s\" and symbolic link \"%.*s\"",
						 linked_path,
						 (int) (link_sep -  relative_ptr), relative_ptr);
				else
					elog(LOG, "create directory \"%s\" and symbolic link \"%s\"",
						 linked_path, relative_ptr);

				/* Firstly, create linked directory */
				dir_create_dir(linked_path, DIR_PERMISSION);

				join_path_components(to_path, data_dir, PG_TBLSPC_DIR);
				/* Create pg_tblspc directory just in case */
				dir_create_dir(to_path, DIR_PERMISSION);

				/* Secondly, create link */
				join_path_components(to_path, to_path, link_name);
				if (symlink(linked_path, to_path) < 0)
					elog(ERROR, "could not create symbolic link \"%s\": %s",
						 to_path, strerror(errno));

				/* Save linked directory */
				set_tablespace_created(link_name, linked_path);

				/*
				 * Create rest of directories.
				 * First check is there any directory name after separator.
				 */
				if (link_sep != NULL && *(link_sep + 1) != '\0')
					goto create_directory;

				continue;
			}
		}

create_directory:
		elog(LOG, "create directory \"%s\"", relative_ptr);

		/* This is not symlink, create directory */
		join_path_components(to_path, data_dir, relative_ptr);
		dir_create_dir(to_path, DIR_PERMISSION);
	}

	if (extract_tablespaces)
	{
		parray_walk(links, pgFileFree);
		parray_free(links);
	}

	parray_walk(dirs, pgFileFree);
	parray_free(dirs);
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

	fclose(fp);
}

/*
 * Check that all tablespace mapping entries have correct linked directory
 * paths. Linked directories must be empty or do not exist.
 *
 * If tablespace-mapping option is supplied, all OLDDIR entries must have
 * entries in tablespace_map file.
 */
void
check_tablespace_mapping(pgBackup *backup)
{
	char		this_backup_path[MAXPGPATH];
	parray	   *links;
	size_t		i;
	TablespaceListCell *cell;
	pgFile	   *tmp_file = pgut_new(pgFile);

	links = parray_new();

	pgBackupGetPath(backup, this_backup_path, lengthof(this_backup_path), NULL);
	read_tablespace_map(links, this_backup_path);
	/* Sort links by the path of a linked file*/
	parray_qsort(links, pgFileCompareLinked);

	if (logger_config.log_level_console <= LOG ||
		logger_config.log_level_file <= LOG)
		elog(LOG, "check tablespace directories of backup %s",
			 base36enc(backup->start_time));

	/* 1 - each OLDDIR must have an entry in tablespace_map file (links) */
	for (cell = tablespace_dirs.head; cell; cell = cell->next)
	{
		tmp_file->linked = cell->old_dir;

		if (parray_bsearch(links, tmp_file, pgFileCompareLinked) == NULL)
			elog(ERROR, "--tablespace-mapping option's old directory "
				 "doesn't have an entry in tablespace_map file: \"%s\"",
				 cell->old_dir);
	}

	/* 2 - all linked directories must be empty */
	for (i = 0; i < parray_num(links); i++)
	{
		pgFile	   *link = (pgFile *) parray_get(links, i);
		const char *linked_path = link->linked;
		TablespaceListCell *cell;

		for (cell = tablespace_dirs.head; cell; cell = cell->next)
			if (strcmp(link->linked, cell->old_dir) == 0)
			{
				linked_path = cell->new_dir;
				break;
			}

		if (!is_absolute_path(linked_path))
			elog(ERROR, "tablespace directory is not an absolute path: %s\n",
				 linked_path);

		if (!dir_is_empty(linked_path))
			elog(ERROR, "restore tablespace destination is not empty: \"%s\"",
				 linked_path);
	}

	free(tmp_file);
	parray_walk(links, pgFileFree);
	parray_free(links);
}

/*
 * Print backup content list.
 */
void
print_file_list(FILE *out, const parray *files, const char *root,
				const char *extra_prefix)
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
		else if (file->extra_dir_num)
		{
			if (extra_prefix)
			{
				char extra_root[MAXPGPATH];
				makeExtraDirPathByNum(extra_root, extra_prefix,
									  file->extra_dir_num);
				path = GetRelativePath(path, extra_root);
			}
			else
				path = GetRelativePath(path, file->extradir);
		}

		fprintf(out, "{\"path\":\"%s\", \"size\":\"" INT64_FORMAT "\", "
					 "\"mode\":\"%u\", \"is_datafile\":\"%u\", "
					 "\"is_cfs\":\"%u\", \"crc\":\"%u\", "
					 "\"compress_alg\":\"%s\", \"extra_dir_num\":\"%u\"",
				path, file->write_size, file->mode,
				file->is_datafile ? 1 : 0, file->is_cfs ? 1 : 0, file->crc,
				deparse_compress_alg(file->compress_alg), file->extra_dir_num);

		if (file->extradir)
			fprintf(out, ",\"extradir\":\"%s\"", file->extradir);

		if (file->is_datafile)
			fprintf(out, ",\"segno\":\"%d\"", file->segno);

#ifndef WIN32
		if (S_ISLNK(file->mode))
#else
		if (pgwin32_is_junction(file->path))
#endif
			fprintf(out, ",\"linked\":\"%s\"", file->linked);

		if (file->n_blocks != BLOCKNUM_INVALID)
			fprintf(out, ",\"n_blocks\":\"%i\"", file->n_blocks);

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
 * is NULL the value will be returned to "value_int64" as int64.
 *
 * Returns true if the value was found in the line.
 */
static bool
get_control_value(const char *str, const char *name,
				  char *value_str, int64 *value_int64, bool is_mandatory)
{
	int			state = CONTROL_WAIT_NAME;
	char	   *name_ptr = (char *) name;
	char	   *buf = (char *) str;
	char		buf_int64[32],	/* Buffer for "value_int64" */
			   *buf_int64_ptr = buf_int64;

	/* Set default values */
	if (value_str)
		*value_str = '\0';
	else if (value_int64)
		*value_int64 = 0;

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
					buf_int64_ptr = buf_int64;
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
					else if (value_int64)
					{
						/* Length of buf_uint64 should not be greater than 31 */
						if (buf_int64_ptr - buf_int64 >= 32)
							elog(ERROR, "field \"%s\" is out of range in the line %s of the file %s",
								 name, str, DATABASE_FILE_LIST);

						*buf_int64_ptr = '\0';
						if (!parse_int64(buf_int64, value_int64, 0))
						{
							/* We assume that too big value is -1 */
							if (errno == ERANGE)
								*value_int64 = BYTES_INVALID;
							else
								goto bad_format;
						}
					}

					return true;
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
						*buf_int64_ptr = *buf;
						buf_int64_ptr++;
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
	return false;

bad_format:
	elog(ERROR, "%s file has invalid format in line %s",
		 DATABASE_FILE_LIST, str);
	return false;	/* Make compiler happy */
}

/*
 * Construct parray of pgFile from the backup content list.
 * If root is not NULL, path will be absolute path.
 */
parray *
dir_read_file_list(const char *root, const char *extra_path, const char *file_txt)
{
	FILE   *fp;
	parray *files;
	char	buf[MAXPGPATH * 2];

	fp = fopen(file_txt, "rt");
	if (fp == NULL)
		elog(ERROR, "cannot open \"%s\": %s", file_txt, strerror(errno));

	files = parray_new();

	while (fgets(buf, lengthof(buf), fp))
	{
		char		path[MAXPGPATH];
		char		filepath[MAXPGPATH];
		char		linked[MAXPGPATH];
		char		compress_alg_string[MAXPGPATH];
		char		extradir_str[MAXPGPATH];
		int64		write_size,
					mode,		/* bit length of mode_t depends on platforms */
					is_datafile,
					is_cfs,
					extra_dir_num,
					crc,
					segno,
					n_blocks;
		pgFile	   *file;

		get_control_value(buf, "path", path, NULL, true);
		get_control_value(buf, "size", NULL, &write_size, true);
		get_control_value(buf, "mode", NULL, &mode, true);
		get_control_value(buf, "is_datafile", NULL, &is_datafile, true);
		get_control_value(buf, "is_cfs", NULL, &is_cfs, false);
		get_control_value(buf, "crc", NULL, &crc, true);
		get_control_value(buf, "compress_alg", compress_alg_string, NULL, false);
		get_control_value(buf, "extra_dir_num", NULL, &extra_dir_num, false);

		if (root)
			if (extra_dir_num)
			{
				char temp[MAXPGPATH];

				Assert(extra_path);
				makeExtraDirPathByNum(temp, extra_path, extra_dir_num);
				join_path_components(filepath, temp, path);
			}
			else
				join_path_components(filepath, root, path);
		else
			strcpy(filepath, path);

		file = pgFileInit(filepath);

		if (extra_dir_num)
		{
			get_control_value(buf, "extradir", extradir_str, NULL, true);
			file->extradir = pgut_strdup(extradir_str);
		}
		file->write_size = (int64) write_size;
		file->mode = (mode_t) mode;
		file->is_datafile = is_datafile ? true : false;
		file->is_cfs = is_cfs ? true : false;
		file->crc = (pg_crc32) crc;
		file->compress_alg = parse_compress_alg(compress_alg_string);
		file->extra_dir_num = extra_dir_num;

		/*
		 * Optional fields
		 */

		if (get_control_value(buf, "linked", linked, NULL, false) && linked[0])
			file->linked = pgut_strdup(linked);

		if (get_control_value(buf, "segno", NULL, &segno, false))
			file->segno = (int) segno;

		if (get_control_value(buf, "n_blocks", NULL, &n_blocks, false))
			file->n_blocks = (int) n_blocks;

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

/*
 * Return true if the path is a existing regular file.
 */
bool
fileExists(const char *path)
{
	struct stat buf;

	if (stat(path, &buf) == -1 && errno == ENOENT)
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
 * Construct parray containing extra directories paths
 * from string like /path1:/path2
 */
parray *
make_extra_directory_list(const char *colon_separated_dirs)
{
	char	   *p;
	parray	   *list = parray_new();
	char	   *tmp = palloc(strlen(colon_separated_dirs) + 1);

	/* TODO: Add path validation */
	strcpy(tmp, colon_separated_dirs);
	p = strtok(tmp,":");
	while(p!=NULL)
	{
		char * dir = (char *)palloc(strlen(p) + 1);
		strcpy(dir,p);
		parray_append(list, dir);
		p=strtok(NULL,":");
	}
	pfree(tmp);
	parray_qsort(list, BlackListCompare);
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
makeExtraDirPathByNum(char *ret_path, const char *path_prefix,
					  const int dir_num)
{
	sprintf(ret_path, "%s%d", path_prefix, dir_num);
}

/* Check if "dir" presents in "dirs_list" */
bool
backup_contains_extra(const char *dir, parray *dirs_list)
{
	void *search_result;

	if (!dirs_list) /* There is no extra dirs in backup */
		return false;
	search_result = parray_bsearch(dirs_list, dir, BlackListCompare);
	return search_result != NULL;
}
