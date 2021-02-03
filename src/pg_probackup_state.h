/*-------------------------------------------------------------------------
 *
 * pg_probackup_state.h: Definitions of internal pg_probackup states 
 *
 * Portions Copyright (c) 2021, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROBACKUP_STATE_H
#define PG_PROBACKUP_STATE_H

/* ====== CatalogState ======= */

typedef struct CatalogState
{
	/* $BACKUP_PATH */
	char		catalog_path[MAXPGPATH]; //previously global var backup_path
	/* $BACKUP_PATH/backups */
	char		backup_subdir_path[MAXPGPATH];
	/* $BACKUP_PATH/wal */
	char		wal_subdir_path[MAXPGPATH]; // previously global var arclog_path
} CatalogState; 

/* ====== CatalogState (END) ======= */


/* ===== instanceState ===== */
typedef struct InstanceState
{
	/* catalog, this instance belongs to */
	CatalogState *catalog_state;

	char		instance_name[MAXPGPATH]; //previously global var instance_name
	/* $BACKUP_PATH/backups/instance_name */
	char		instance_backup_subdir_path[MAXPGPATH];
	/* $BACKUP_PATH/backups/instance_name */
	char		instance_wal_subdir_path[MAXPGPATH]; // previously global var arclog_path

	//TODO add config here
} InstanceState;

/* ===== instanceState (END) ===== */

#endif /* PG_PROBACKUP_STATE_H */
