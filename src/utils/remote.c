#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>

#include "pg_probackup.h"
#include "file.h"

#define MAX_CMDLINE_LENGTH  4096
#define MAX_CMDLINE_OPTIONS 256
#define ERR_BUF_SIZE        4096
#define PIPE_SIZE           (64*1024)

static int split_options(int argc, char* argv[], int max_options, char* options)
{
	char* opt = options;
	char in_quote = '\0';
	while (true) {
		switch (*opt) {
		  case '\'':
		  case '\"':
			if (!in_quote) {
				in_quote = *opt++;
				continue;
			}
			if (*opt == in_quote && *++opt != in_quote) {
				in_quote = '\0';
				continue;
			}
			break;
		  case '\0':
			if (opt != options) {
				argv[argc++] = options;
				if (argc >= max_options)
					elog(ERROR, "Too much options");
			}
			return argc;
		  case ' ':
			argv[argc++] = options;
			if (argc >= max_options)
				elog(ERROR, "Too much options");
			*opt++ = '\0';
			options = opt;
			continue;
		  default:
			break;
		}
		opt += 1;
	}
	return argc;
}

static int child_pid;
#if 0
static void kill_child(void)
{
	kill(child_pid, SIGTERM);
}
#endif

#ifdef WIN32
void launch_ssh(char* argv[])
{
	SYS_CHECK(dup2(atoi(argv[2]), 0));
	SYS_CHECK(dup2(atoi(argv[3]), 1));
	SYS_CHECK(execvp(argv[4], argv+4));
}
#endif


bool launch_agent(void)
{
	char cmd[MAX_CMDLINE_LENGTH];
	char* ssh_argv[MAX_CMDLINE_OPTIONS];
	int ssh_argc;
	int outfd[2];
	int infd[2];

	ssh_argc = 0;
#ifdef WIN32
	ssh_argv[ssh_argc++] = pg_probackup;
	ssh_argv[ssh_argc++] = "ssh";
	ssh_argc += 2; /* reserve space for pipe descriptors */
#endif
	ssh_argv[ssh_argc++] = instance_config.remote.proto;
	if (instance_config.remote.port != NULL) {
		ssh_argv[ssh_argc++] = "-p";
		ssh_argv[ssh_argc++] = instance_config.remote.port;
	}
	if (instance_config.remote.user != NULL) {
		ssh_argv[ssh_argc++] = "-l";
		ssh_argv[ssh_argc++] = instance_config.remote.user;
	}
	if (instance_config.remote.ssh_config != NULL) {
		ssh_argv[ssh_argc++] = "-F";
		ssh_argv[ssh_argc++] = instance_config.remote.ssh_config;
	}
	if (instance_config.remote.ssh_options != NULL) {
		ssh_argc = split_options(ssh_argc, ssh_argv, MAX_CMDLINE_OPTIONS, pg_strdup(instance_config.remote.ssh_options));
	}
	if (num_threads > 1)
	{
		ssh_argv[ssh_argc++] = "-o";
		ssh_argv[ssh_argc++] = "PasswordAuthentication=no";
	}

	ssh_argv[ssh_argc++] = "-o";
	ssh_argv[ssh_argc++] = "Compression=no";

	ssh_argv[ssh_argc++] = "-o";
	ssh_argv[ssh_argc++] = "LogLevel=error";

	ssh_argv[ssh_argc++] = instance_config.remote.host;
	ssh_argv[ssh_argc++] = cmd;
	ssh_argv[ssh_argc] = NULL;

	if (instance_config.remote.path)
	{
		char* sep = strrchr(pg_probackup, '/');
		if (sep != NULL) {
			pg_probackup = sep + 1;
		}
		snprintf(cmd, sizeof(cmd), "%s/%s agent %s",
					   instance_config.remote.path, pg_probackup, PROGRAM_VERSION);
	} else {
		snprintf(cmd, sizeof(cmd), "%s agent %s", pg_probackup, PROGRAM_VERSION);
	}

#ifdef WIN32
	SYS_CHECK(_pipe(infd, PIPE_SIZE, O_BINARY)) ;
	SYS_CHECK(_pipe(outfd, PIPE_SIZE, O_BINARY));
	ssh_argv[2] = psprintf("%d", outfd[0]);
	ssh_argv[3] = psprintf("%d", infd[1]);
	{
	    intptr_t pid = _spawnvp(_P_NOWAIT, ssh_argv[0], ssh_argv);
		if (pid < 0)
			return false;
		child_pid = GetProcessId((HANDLE)pid);
#else
	SYS_CHECK(pipe(infd));
	SYS_CHECK(pipe(outfd));

	SYS_CHECK(child_pid = fork());

	if (child_pid == 0) { /* child */
		SYS_CHECK(close(STDIN_FILENO));
		SYS_CHECK(close(STDOUT_FILENO));

		SYS_CHECK(dup2(outfd[0], STDIN_FILENO));
		SYS_CHECK(dup2(infd[1],  STDOUT_FILENO));

		SYS_CHECK(close(infd[0]));
		SYS_CHECK(close(infd[1]));
		SYS_CHECK(close(outfd[0]));
		SYS_CHECK(close(outfd[1]));

		if (execvp(ssh_argv[0], ssh_argv) < 0)
			return false;
	} else {
#endif
		elog(LOG, "Spawn agent %d version %s", child_pid, PROGRAM_VERSION);
		SYS_CHECK(close(infd[1]));  /* These are being used by the child */
		SYS_CHECK(close(outfd[0]));
		/*atexit(kill_child);*/

		fio_redirect(infd[0], outfd[1]); /* write to stdout */
	}
	return true;
}
