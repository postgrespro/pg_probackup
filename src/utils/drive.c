#include <stdio.h>
#include <unistd.h>

#include "pg_probackup.h"
/* sys/stat.h must be included after pg_probackup.h (see problems with compilation for windows described in PGPRO-5750) */
#include <sys/stat.h>

#include "pio_storage.h"
#include "catalog/pg_tablespace_d.h"
#include "storage/checksum.h"

typedef struct pioLocalDrive
{
} pioLocalDrive;

typedef struct pioRemoteDrive
{
} pioRemoteDrive;

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
	NULL
};

static char *pgdata_exclude_files_non_exclusive[] =
{
	/*skip in non-exclusive backup */
	"backup_label",
	"tablespace_map",
	NULL
};

static pioDrive_i localDrive;
static pioDrive_i remoteDrive;

static void dir_list_file_internal_local(parray *files, pgFile *parent, const char *parent_dir,
									bool exclude, bool follow_symlink, bool backup_logs,
									bool skip_hidden, int external_dir_num, pioDrive_i drive);
static char dir_check_file(pgFile *file, bool backup_logs);
static void dir_list_file_internal_remote(parray *files, pgFile *parent, const char *parent_dir,
									bool exclude, bool follow_symlink, bool backup_logs,
									bool skip_hidden, int external_dir_num, pioDrive_i drive);
static pgFile *
pgFileNew_pio(const char *path, const char *rel_path, bool follow_symlink,
		  int external_dir_num, pioDrive_i drive)
{
	struct stat		st;
	err_i			err;
	pgFile		   *file;

	/* stat the file */
	st = $i(pioStat, drive, path, follow_symlink, &err);
	if ($haserr(err)) {
		/* file not found is not an error case */
		if (getErrno(err) == ENOENT)
			return NULL;
		elog(ERROR, "cannot stat file \"%s\": %s", path,
			strerror(getErrno(err)));
	}

	file = pgFileInit(rel_path);
	file->size = st.st_size;
	file->mode = st.st_mode;
	file->mtime = st.st_mtime;
	file->external_dir_num = external_dir_num;

	return file;
}

static DIR*
remote_opendir(const char* path) {
	int i;
	fio_header hdr;
	unsigned long mask;
	
	mask = fio_fdset;
	for (i = 0; (mask & 1) != 0; i++, mask >>= 1);
	if (i == FIO_FDMAX) {
		elog(ERROR, "Descriptor pool for remote files is exhausted, "
				"probably too many remote directories are opened");
	}
	hdr.cop = FIO_OPENDIR;
	hdr.handle = i;
	hdr.size = strlen(path) + 1;
	fio_fdset |= 1 << i;
	
	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);
	
	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
	
	if (hdr.arg != 0) {
		errno = hdr.arg;
		fio_fdset &= ~(1 << hdr.handle);
		return NULL;
	}
	return (DIR*)(size_t)(i + 1);
}

static struct dirent*
remote_readdir(DIR* dir) {
	fio_header hdr;
	static __thread struct dirent entry;
	
	hdr.cop = FIO_READDIR;
	hdr.handle = (size_t)dir - 1;
	hdr.size = 0;
	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	
	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
	Assert(hdr.cop == FIO_SEND);
	if (hdr.size) {
		Assert(hdr.size == sizeof(entry));
		IO_CHECK(fio_read_all(fio_stdin, &entry, sizeof(entry)), sizeof(entry));
	}
	
	return hdr.size ? &entry : NULL;
}

static int
remote_closedir(DIR* dir) {
	fio_header hdr;
	hdr.cop = FIO_CLOSEDIR;
	hdr.handle = (size_t)dir - 1;
	hdr.size = 0;
	fio_fdset &= ~(1 << hdr.handle);
	
	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	return 0;
}

pioDrive_i
pioDriveForLocation(fio_location loc)
{
    if (fio_is_remote(loc))
        return remoteDrive;
    else
        return localDrive;
}

/* LOCAL DRIVE */

static pioFile_i
pioLocalDrive_pioOpen(VSelf, path_t path, int flags,
                      int permissions, err_i *err)
{
    int	fd;
    fobj_reset_err(err);
    fobj_t file;

    if (permissions == 0)
        fd = open(path, flags, FILE_PERMISSION);
    else
        fd = open(path, flags, permissions);
    if (fd < 0)
    {
        *err = $syserr("Cannot open file {path:q}", (path, path));
        return (pioFile_i){NULL};
    }

    file = $alloc(pioLocalFile, .fd = fd,
                  .p = { .path = ft_cstrdup(path), .flags = flags } );
    return bind_pioFile(file);
}

static struct stat
pioLocalDrive_pioStat(VSelf, path_t path, bool follow_symlink, err_i *err)
{
    struct stat	st = {0};
    int	r;
    fobj_reset_err(err);

    r = follow_symlink ? stat(path, &st) : lstat(path, &st);
    if (r < 0)
        *err = $syserr("Cannot stat file {path:q}", (path, path));
    return st;
}

#define pioLocalDrive_pioExists common_pioExists

static err_i
pioLocalDrive_pioRemove(VSelf, path_t path, bool missing_ok)
{
    if (remove_file_or_dir(path) != 0)
    {
        if (!missing_ok || errno != ENOENT)
            return $syserr("Cannot remove {path:q}", (path, path));
    }
    return $noerr();
}

static err_i
pioLocalDrive_pioRename(VSelf, path_t old_path, path_t new_path)
{
    if (rename(old_path, new_path) != 0)
        return $syserr("Cannot rename file {old_path:q} to {new_path:q}",
                       (old_path, old_path), (new_path, new_path));
    return $noerr();
}

static pg_crc32
pioLocalDrive_pioGetCRC32(VSelf, path_t path, bool compressed, err_i *err)
{
    fobj_reset_err(err);
    elog(VERBOSE, "Local Drive calculate crc32 for '%s', compressed=%d",
         path, compressed);
    if (compressed)
        return pgFileGetCRCgz(path, true, true);
    else
        return pgFileGetCRC(path, true, true);
}

static bool
pioLocalDrive_pioIsRemote(VSelf)
{
    return false;
}

void
pioLocalDrive_pioListDir(VSelf, parray *files, const char* root, bool exclude,
						bool follow_symlink, bool add_root, bool backup_logs,
						bool skip_hidden, int external_dir_num)
{
	Self(pioLocalDrive);
	pgFile	   *file;

	file = pgFileNew_pio(root, "", follow_symlink, external_dir_num, bind_pioDrive(self));
	if (file == NULL) {
		/* For external directory this is not ok */
		if (external_dir_num > 0)
			elog(ERROR, "External directory is not found: \"%s\"", root);
		else
			return;
	}

	if (!S_ISDIR(file->mode)) {
		if (external_dir_num > 0)
			elog(ERROR, " --external-dirs option \"%s\": directory or symbolic link expected",
					root);
		else
			elog(WARNING, "Skip \"%s\": unexpected file format", root);
		return;
	}
	if (add_root)
		parray_append(files, file);

	dir_list_file_internal_local(files, file, root, exclude, follow_symlink,
						   backup_logs, skip_hidden, external_dir_num, bind_pioDrive(self));

	if (!add_root)
		pgFileFree(file);
}

#define CHECK_FALSE				0
#define CHECK_TRUE				1
#define CHECK_EXCLUDE_FALSE		2

static void dir_list_file_internal_local(parray *files, pgFile *parent, const char *parent_dir,
									bool exclude, bool follow_symlink, bool backup_logs,
									bool skip_hidden, int external_dir_num, pioDrive_i drive)
{
	DIR			  *dir;
	struct dirent *dent;

	if (!S_ISDIR(parent->mode))
		elog(ERROR, "\"%s\" is not a directory", parent_dir);

	/* Open directory and list contents */
	dir = opendir(parent_dir);
	if (dir == NULL) {
		if (errno == ENOENT) {
			/* Maybe the directory was removed */
			return;
		}
		elog(ERROR, "Cannot open directory \"%s\": %s",
				parent_dir, strerror(errno));
	}

	errno = 0;
	while ((dent = readdir(dir))) {
		pgFile	   *file;
		char		child[MAXPGPATH];
		char		rel_child[MAXPGPATH];
		char		check_res;

		join_path_components(child, parent_dir, dent->d_name);
		join_path_components(rel_child, parent->rel_path, dent->d_name);

		file = pgFileNew_pio(child, rel_child, follow_symlink, external_dir_num, drive);
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

		if (exclude) {
			check_res = dir_check_file(file, backup_logs);
			if (check_res == CHECK_FALSE) {
				/* Skip */
				pgFileFree(file);
				continue;
			}
			else if (check_res == CHECK_EXCLUDE_FALSE) {
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
			dir_list_file_internal_local(files, file, child, exclude, follow_symlink,
								   backup_logs, skip_hidden, external_dir_num, drive);
	}

	if (errno && errno != ENOENT) {
		int			errno_tmp = errno;
		closedir(dir);
		elog(ERROR, "Cannot read directory \"%s\": %s",
				parent_dir, strerror(errno_tmp));
	}
	closedir(dir);
}

static char
dir_check_file(pgFile *file, bool backup_logs)
{
	int			i;
	int			sscanf_res;
	bool		in_tablespace = false;

	in_tablespace = path_is_prefix_of_path(PG_TBLSPC_DIR, file->rel_path);

	/* Check if we need to exclude file by name */
	if (S_ISREG(file->mode)) {
		if (!exclusive_backup) {
			for (i = 0; pgdata_exclude_files_non_exclusive[i]; i++) {
				if (strcmp(file->rel_path,
						   pgdata_exclude_files_non_exclusive[i]) == 0)
				{
					/* Skip */
					elog(VERBOSE, "Excluding file: %s", file->name);
					return CHECK_FALSE;
				}
			}
		}

		for (i = 0; pgdata_exclude_files[i]; i++) {
			if (strcmp(file->rel_path, pgdata_exclude_files[i]) == 0) {
				/* Skip */
				elog(VERBOSE, "Excluding file: %s", file->name);
				return CHECK_FALSE;
			}
		}
	}
	/*
	 * If the directory name is in the exclude list, do not list the
	 * contents.
	 */
	else if (S_ISDIR(file->mode) && !in_tablespace && file->external_dir_num == 0) {
		/*
		 * If the item in the exclude list starts with '/', compare to
		 * the absolute path of the directory. Otherwise compare to the
		 * directory name portion.
		 */
		for (i = 0; pgdata_exclude_dir[i]; i++) {
			/* exclude by dirname */
			if (strcmp(file->name, pgdata_exclude_dir[i]) == 0) {
				elog(VERBOSE, "Excluding directory content: %s", file->rel_path);
				return CHECK_EXCLUDE_FALSE;
			}
		}

		if (!backup_logs) {
			if (strcmp(file->rel_path, PG_LOG_DIR) == 0) {
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

	if (in_tablespace) {
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
	} else if (path_is_prefix_of_path("global", file->rel_path)) {
		file->tblspcOid = GLOBALTABLESPACE_OID;
	} else if (path_is_prefix_of_path("base", file->rel_path)) {
		file->tblspcOid = DEFAULTTABLESPACE_OID;
		sscanf(file->rel_path, "base/%u/", &(file->dbOid));
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
		else if (isdigit(file->name[0])) {
			char	   *fork_name;
			int			len;
			char		suffix[MAXPGPATH];

			fork_name = strstr(file->name, "_");
			if (fork_name) {
				/* Auxiliary fork of the relfile */
				if (strcmp(fork_name, "_vm") == 0)
					file->forkName = vm;
				else if (strcmp(fork_name, "_fsm") == 0)
					file->forkName = fsm;
				else if (strcmp(fork_name, "_cfm") == 0)
					file->forkName = cfm;
				else if (strcmp(fork_name, "_ptrack") == 0)
					file->forkName = ptrack;
				else if (strcmp(fork_name, "_init") == 0)
					file->forkName = init;

				// extract relOid for certain forks
				if (file->forkName == vm ||
					file->forkName == fsm ||
					file->forkName == init ||
					file->forkName == cfm)
				{
					// sanity
					if (sscanf(file->name, "%u_*", &(file->relOid)) != 1)
						file->relOid = 0;
				}

				/* Do not backup ptrack files */
				if (file->forkName == ptrack)
					return CHECK_FALSE;
			} else {
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

/* REMOTE DRIVE */

static pioFile_i
pioRemoteDrive_pioOpen(VSelf, path_t path,
                       int flags, int permissions,
                       err_i *err)
{
    int i;
    fio_header hdr;
    unsigned long mask;
    fobj_reset_err(err);
    fobj_t file;

    mask = fio_fdset;
    for (i = 0; (mask & 1) != 0; i++, mask >>= 1);
    if (i == FIO_FDMAX)
        elog(ERROR, "Descriptor pool for remote files is exhausted, "
                    "probably too many remote files are opened");

    hdr.cop = FIO_OPEN;
    hdr.handle = i;
    hdr.size = strlen(path) + 1;
    hdr.arg = flags;
    fio_fdset |= 1 << i;

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

    /* check results */
    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

    if (hdr.arg != 0)
    {
        errno = (int)hdr.arg;
        *err = $syserr("Cannot open remote file {path:q}", (path, path));
        fio_fdset &= ~(1 << hdr.handle);
        return (pioFile_i){NULL};
    }
    file = $alloc(pioRemoteFile, .handle = i,
                  .p = { .path = ft_cstrdup(path), .flags = flags });
    return bind_pioFile(file);
}

static struct stat
pioRemoteDrive_pioStat(VSelf, path_t path, bool follow_symlink, err_i *err)
{
    struct stat	st = {0};
    fio_header hdr = {
            .cop = FIO_STAT,
            .handle = -1,
            .size = strlen(path) + 1,
            .arg = follow_symlink,
    };
    fobj_reset_err(err);

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
    ft_dbg_assert(hdr.cop == FIO_STAT);
    IO_CHECK(fio_read_all(fio_stdin, &st, sizeof(st)), sizeof(st));

    if (hdr.arg != 0)
    {
        errno = (int)hdr.arg;
        *err = $syserr("Cannot stat remote file {path:q}", (path, path));
    }
    return st;
}

#define pioRemoteDrive_pioExists common_pioExists

static err_i
pioRemoteDrive_pioRemove(VSelf, path_t path, bool missing_ok)
{
    fio_header hdr = {
            .cop = FIO_REMOVE,
            .handle = -1,
            .size = strlen(path) + 1,
            .arg = missing_ok ? 1 : 0,
    };

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
    ft_dbg_assert(hdr.cop == FIO_REMOVE);

    if (hdr.arg != 0)
    {
        errno = (int)hdr.arg;
        return $syserr("Cannot remove remote file {path:q}", (path, path));
    }
    return $noerr();
}

static err_i
pioRemoteDrive_pioRename(VSelf, path_t old_path, path_t new_path)
{
    size_t old_path_len = strlen(old_path) + 1;
    size_t new_path_len = strlen(new_path) + 1;
    fio_header hdr = {
            .cop = FIO_RENAME,
            .handle = -1,
            .size = old_path_len + new_path_len,
            .arg = 0,
    };

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, old_path, old_path_len), old_path_len);
    IO_CHECK(fio_write_all(fio_stdout, new_path, new_path_len), new_path_len);

    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
    ft_dbg_assert(hdr.cop == FIO_RENAME);

    if (hdr.arg != 0)
    {
        errno = (int)hdr.arg;
        return $syserr("Cannot rename remote file {old_path:q} to {new_path:q}",
                       (old_path, old_path), (new_path, new_path));
    }
    return $noerr();
}

static pg_crc32
pioRemoteDrive_pioGetCRC32(VSelf, path_t path, bool compressed, err_i *err)
{
    fio_header hdr;
    size_t path_len = strlen(path) + 1;
    pg_crc32 crc = 0;
    fobj_reset_err(err);

    hdr.cop = FIO_GET_CRC32;
    hdr.handle = -1;
    hdr.size = path_len;
    hdr.arg = 0;

    if (compressed)
        hdr.arg = 1;
    elog(VERBOSE, "Remote Drive calculate crc32 for '%s', hdr.arg=%d",
         path, compressed);

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);
    IO_CHECK(fio_read_all(fio_stdin, &crc, sizeof(crc)), sizeof(crc));

    return crc;
}

static bool
pioRemoteDrive_pioIsRemote(VSelf)
{
    return true;
}

void
pioRemoteDrive_pioListDir(VSelf, parray *files, const char* root, bool exclude,
						bool follow_symlink, bool add_root, bool backup_logs,
						bool skip_hidden, int external_dir_num)
{
	Self(pioRemoteDrive);
	pgFile	   *file;

	file = pgFileNew_pio(root, "", follow_symlink, external_dir_num, bind_pioDrive(self));
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

	dir_list_file_internal_remote(files, file, root, exclude, follow_symlink,
						   backup_logs, skip_hidden, external_dir_num, bind_pioDrive(self));

	if (!add_root)
		pgFileFree(file);
}

static void dir_list_file_internal_remote(parray *files, pgFile *parent, const char *parent_dir,
									bool exclude, bool follow_symlink, bool backup_logs,
									bool skip_hidden, int external_dir_num, pioDrive_i drive)
{
	DIR			  *dir;
	struct dirent *dent;

	if (!S_ISDIR(parent->mode))
		elog(ERROR, "\"%s\" is not a directory", parent_dir);

	/* Open directory and list contents */
	dir = remote_opendir(parent_dir);
	if (dir == NULL) {
		if (errno == ENOENT) {
			/* Maybe the directory was removed */
			return;
		}
		elog(ERROR, "Cannot open directory \"%s\": %s",
				parent_dir, strerror(errno));
	}

	errno = 0;
	while ((dent = remote_readdir(dir))) {
		pgFile	   *file;
		char		child[MAXPGPATH];
		char		rel_child[MAXPGPATH];
		char		check_res;

		join_path_components(child, parent_dir, dent->d_name);
		join_path_components(rel_child, parent->rel_path, dent->d_name);

		file = pgFileNew_pio(child, rel_child, follow_symlink, external_dir_num, drive);
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

		if (exclude) {
			check_res = dir_check_file(file, backup_logs);
			if (check_res == CHECK_FALSE) {
				/* Skip */
				pgFileFree(file);
				continue;
			}
			else if (check_res == CHECK_EXCLUDE_FALSE) {
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
			dir_list_file_internal_remote(files, file, child, exclude, follow_symlink,
								   backup_logs, skip_hidden, external_dir_num, drive);
	}

	if (errno && errno != ENOENT) {
		int			errno_tmp = errno;
		fio_closedir(dir);
		elog(ERROR, "Cannot read directory \"%s\": %s",
				parent_dir, strerror(errno_tmp));
	}
	remote_closedir(dir);
}

fobj_klass_handle(pioLocalDrive);
fobj_klass_handle(pioRemoteDrive);

void
init_drive_objects() {
	FOBJ_FUNC_ARP();
    localDrive = bindref_pioDrive($alloc(pioLocalDrive));
    remoteDrive = bindref_pioDrive($alloc(pioRemoteDrive));
}
