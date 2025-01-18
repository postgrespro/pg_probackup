/*-------------------------------------------------------------------------
 *
 * remote.h: - prototypes of remote functions.
 *
 * Copyright (c) 2017-2025, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#ifndef REMOTE_H
#define REMOTE_H

typedef struct RemoteConfig
{
	char* proto;
	char* host;
	char* port;
	char* path;
	char* user;
	char *ssh_config;
	char *ssh_options;
} RemoteConfig;

#endif
