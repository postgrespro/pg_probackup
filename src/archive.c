/*-------------------------------------------------------------------------
 *
 * archive.c: -  pg_probackup specific archive commands for archive backups.
 *
 *
 * Portions Copyright (c) 2018-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <unistd.h>

/*
 * pg_probackup specific archive command for archive backups
 * set archive_command = 'pg_probackup archive-push -B /home/anastasia/backup
 * --wal-file-path %p --wal-file-name %f', to move backups into arclog_path.
 * Where archlog_path is $BACKUP_PATH/wal/system_id.
 * Currently it just copies wal files to the new location.
 * TODO: Planned options: list the arclog content,
 * compute and validate checksums.
 */
int
do_archive_push(char *wal_file_path, char *wal_file_name, bool overwrite)
{
	char		backup_wal_file_path[MAXPGPATH];
	char		absolute_wal_file_path[MAXPGPATH];
	char		current_dir[MAXPGPATH];
	uint64		system_id;
	bool		is_compress = false;

	if (wal_file_name == NULL && wal_file_path == NULL)
		elog(ERROR, "required parameters are not specified: --wal-file-name %%f --wal-file-path %%p");

	if (wal_file_name == NULL)
		elog(ERROR, "required parameter not specified: --wal-file-name %%f");

	if (wal_file_path == NULL)
		elog(ERROR, "required parameter not specified: --wal-file-path %%p");

	if (!getcwd(current_dir, sizeof(current_dir)))
		elog(ERROR, "getcwd() error");

	/* verify that archive-push --instance parameter is valid */
	system_id = get_system_identifier(current_dir);

	if (instance_config.pgdata == NULL)
		elog(ERROR, "cannot read pg_probackup.conf for this instance");

	if(system_id != instance_config.system_identifier)
		elog(ERROR, "Refuse to push WAL segment %s into archive. Instance parameters mismatch."
					"Instance '%s' should have SYSTEM_ID = " UINT64_FORMAT " instead of " UINT64_FORMAT,
			 wal_file_name, instance_name, instance_config.system_identifier,
			 system_id);

	/* Create 'archlog_path' directory. Do nothing if it already exists. */
	fio_mkdir(arclog_path, DIR_PERMISSION, FIO_BACKUP_HOST);

	join_path_components(absolute_wal_file_path, current_dir, wal_file_path);
	join_path_components(backup_wal_file_path, arclog_path, wal_file_name);

	elog(INFO, "pg_probackup archive-push from %s to %s", absolute_wal_file_path, backup_wal_file_path);

	if (instance_config.compress_alg == PGLZ_COMPRESS)
		elog(ERROR, "pglz compression is not supported");

#ifdef HAVE_LIBZ
	if (instance_config.compress_alg == ZLIB_COMPRESS)
		is_compress = IsXLogFileName(wal_file_name);
#endif

	push_wal_file(absolute_wal_file_path, backup_wal_file_path, is_compress,
				  overwrite);
	elog(INFO, "pg_probackup archive-push completed successfully");

	return 0;
}

/*
 * pg_probackup specific restore command.
 * Move files from arclog_path to pgdata/wal_file_path.
 */
int
do_archive_get(char *wal_file_path, char *wal_file_name)
{
	char		backup_wal_file_path[MAXPGPATH];
	char		absolute_wal_file_path[MAXPGPATH];
	char		current_dir[MAXPGPATH];

	if (wal_file_name == NULL && wal_file_path == NULL)
		elog(ERROR, "required parameters are not specified: --wal-file-name %%f --wal-file-path %%p");

	if (wal_file_name == NULL)
		elog(ERROR, "required parameter not specified: --wal-file-name %%f");

	if (wal_file_path == NULL)
		elog(ERROR, "required parameter not specified: --wal-file-path %%p");

	if (!getcwd(current_dir, sizeof(current_dir)))
		elog(ERROR, "getcwd() error");

	join_path_components(absolute_wal_file_path, current_dir, wal_file_path);
	join_path_components(backup_wal_file_path, arclog_path, wal_file_name);

	elog(INFO, "pg_probackup archive-get from %s to %s",
		 backup_wal_file_path, absolute_wal_file_path);
	get_wal_file(backup_wal_file_path, absolute_wal_file_path);
	elog(INFO, "pg_probackup archive-get completed successfully");

	return 0;
}
