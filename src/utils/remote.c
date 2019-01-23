#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <pthread.h>

#include "pg_probackup.h"
#include "file.h"

#define MAX_CMDLINE_LENGTH  4096
#define MAX_CMDLINE_OPTIONS 256
#define ERR_BUF_SIZE        4096

static int append_option(char* buf, size_t buf_size, size_t dst, char const* src)
{
	size_t len = strlen(src);
	if (dst + len + 3 >= buf_size) {
		fprintf(stderr, "Too long command line\n");
		exit(EXIT_FAILURE);
	}
	buf[dst++] = ' ';
	if (strchr(src, ' ') != NULL) { /* need quotes */
		buf[dst++] = '\'';
		memcpy(&buf[dst], src, len);
		dst += len;
		buf[dst++] = '\'';
	} else {
		memcpy(&buf[dst], src, len);
		dst += len;
	}
	return dst;
}

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

static void kill_child(void)
{
	kill(child_pid, SIGTERM);
}

static void* error_reader_proc(void* arg)
{
	int* errfd = (int*)arg;
	char buf[ERR_BUF_SIZE];
	int offs = 0, rc;

	while ((rc = read(errfd[0], &buf[offs], sizeof(buf) - offs)) > 0)
	{
		char* nl;
		offs += rc;
		buf[offs] = '\0';
		nl = strchr(buf, '\n');
		if (nl != NULL) {
			*nl = '\0';
			if (strncmp(buf, "ERROR: ", 7) == 0) {
				elog(ERROR, "%s", buf + 7);
			} if (strncmp(buf, "WARNING: ", 9) == 0) {
				elog(WARNING, "%s", buf + 9);
			} else if (strncmp(buf, "LOG: ", 5) == 0) {
				elog(LOG, "%s", buf + 5);
			} else if (strncmp(buf, "INFO: ", 6) == 0) {
				elog(INFO, "%s", buf + 6);
			} else {
				elog(LOG, "%s", buf);
			}
			memmove(buf, nl+1, offs -= (nl + 1 - buf));
		}
	}
	return NULL;
}

int remote_execute(int argc, char* argv[], bool listen)
{
	char cmd[MAX_CMDLINE_LENGTH];
	size_t dst = 0;
	char* ssh_argv[MAX_CMDLINE_OPTIONS];
	int ssh_argc;
	int i;
	int outfd[2];
	int infd[2];
	int errfd[2];
	char* pg_probackup = argv[0];
	pthread_t error_reader_thread;

	ssh_argc = 0;
	ssh_argv[ssh_argc++] = instance_config.remote.proto;
	if (instance_config.remote.port != NULL) {
		ssh_argv[ssh_argc++] = (char*)"-p";
		ssh_argv[ssh_argc++] = instance_config.remote.port;
	}
	if (instance_config.remote.ssh_config != NULL) {
		ssh_argv[ssh_argc++] = (char*)"-F";
		ssh_argv[ssh_argc++] = instance_config.remote.ssh_config;
	}
	if (instance_config.remote.ssh_options != NULL) {
		ssh_argc = split_options(ssh_argc, ssh_argv, MAX_CMDLINE_OPTIONS, pg_strdup(instance_config.remote.ssh_options));
	}
	ssh_argv[ssh_argc++] = instance_config.remote.host;
	ssh_argv[ssh_argc++] = cmd;
	ssh_argv[ssh_argc] = NULL;

	if (instance_config.remote.path)
	{
		char* sep = strrchr(pg_probackup, '/');
		if (sep != NULL) {
			pg_probackup = sep + 1;
		}
		dst = snprintf(cmd, sizeof(cmd), "%s/%s", instance_config.remote.path, pg_probackup);
	} else {
		dst = snprintf(cmd, sizeof(cmd), "%s", pg_probackup);
	}

	for (i = 1; i < argc; i++) {
		dst = append_option(cmd, sizeof(cmd), dst, argv[i]);
	}

	dst = append_option(cmd, sizeof(cmd), dst, "--agent");
	dst = append_option(cmd, sizeof(cmd), dst, PROGRAM_VERSION);

	for (i = 0; instance_options[i].type; i++) {
		ConfigOption *opt = &instance_options[i];
		char	     *value;
		char         *cmd_opt;

        /* Path only options from file */
		if (opt->source != SOURCE_FILE)
			continue;

		value = opt->get_value(opt);
		if (value == NULL)
			continue;

		cmd_opt = psprintf("--%s=%s", opt->lname, value);

		dst = append_option(cmd, sizeof(cmd), dst, cmd_opt);
		pfree(value);
		pfree(cmd_opt);
	}

	cmd[dst] = '\0';

	SYS_CHECK(pipe(infd));
	SYS_CHECK(pipe(outfd));
	SYS_CHECK(pipe(errfd));

	SYS_CHECK(child_pid = fork());

	if (child_pid == 0) { /* child */
		SYS_CHECK(close(STDIN_FILENO));
		SYS_CHECK(close(STDOUT_FILENO));
		SYS_CHECK(close(STDERR_FILENO));

		SYS_CHECK(dup2(outfd[0], STDIN_FILENO));
		SYS_CHECK(dup2(infd[1],  STDOUT_FILENO));
		SYS_CHECK(dup2(errfd[1], STDERR_FILENO));

		SYS_CHECK(close(infd[0]));
		SYS_CHECK(close(infd[1]));
		SYS_CHECK(close(outfd[0]));
		SYS_CHECK(close(outfd[1]));
		SYS_CHECK(close(errfd[0]));
		SYS_CHECK(close(errfd[1]));

		if (execvp(ssh_argv[0], ssh_argv) < 0)
			elog(ERROR, "Failed to spawn %s: %s", ssh_argv[0], strerror(errno));
		return -1;
	} else {
		SYS_CHECK(close(infd[1]));  /* These are being used by the child */
		SYS_CHECK(close(outfd[0]));
		SYS_CHECK(close(errfd[1]));
		atexit(kill_child);

		pthread_create(&error_reader_thread, NULL, error_reader_proc, errfd);

		if (listen) {
			int status;
			fio_communicate(infd[0], outfd[1]);

			SYS_CHECK(wait(&status));
			return status;
		} else {
			fio_redirect(infd[0], outfd[1]); /* write to stdout */
			return 0;
		}
	}
}

