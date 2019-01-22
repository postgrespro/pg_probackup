/*-------------------------------------------------------------------------
 *
 * remote.h: - prototypes of remote functions.
 *
 * Copyright (c) 2017-2018, Postgres Professional
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
	char *ssh_config;
	char *ssh_options;
} RemoteConfig;

#endif
