#include <stdio.h>
#include <unistd.h>
#include <time.h>

#include <CUnit/Basic.h>

#include <pg_probackup.h>
#include <utils/file.h>

#include <s3.h>

#include "pgunit.h"

/* Emulate pgprobackup */
bool 		show_color = true;
ShowFormat show_format = SHOW_PLAIN;
const char  *PROGRAM_NAME = NULL;
const char  *PROGRAM_NAME_FULL = NULL;
const char  *PROGRAM_FULL_PATH = NULL;
pid_t       my_pid = 0;
bool        is_archive_cmd = false;
bool         remote_agent = false;
time_t current_time = 0;
char	   *replication_slot = NULL;
pgBackup	current;
bool perm_slot = false;
bool		temp_slot = false;
bool		progress = false;
int			num_threads = 1;
bool		delete_wal = false;
bool		merge_expired = false;
bool         smooth_checkpoint;
bool skip_block_validation = false;
bool		dry_run = false;
bool		delete_expired = false;

/***********************/
