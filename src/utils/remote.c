#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "pg_probackup.h"
#include "file.h"

#define MAX_CMDLINE_LENGTH  4096
#define MAX_CMDLINE_OPTIONS 256

static int append_option(char* buf, size_t buf_size, size_t dst, char const* src)
{
	size_t len = strlen(src);
	if (dst + len + 1 >= buf_size) {
		fprintf(stderr, "Too long command line\n");
		exit(EXIT_FAILURE);
	}
	buf[dst] = ' ';
	memcpy(&buf[dst+1], src, len);
	return dst + len + 1;
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

int remote_execute(int argc, char* argv[], bool listen)
{
	char cmd[MAX_CMDLINE_LENGTH];
	size_t dst = 0;
	char* ssh_argv[MAX_CMDLINE_OPTIONS];
	int ssh_argc;
	int i;
	int outfd[2];
	int infd[2];
	pid_t pid;
	char* pg_probackup = argv[0];

	ssh_argc = 0;
	ssh_argv[ssh_argc++] = remote_proto;
	if (remote_port != NULL) {
		ssh_argv[ssh_argc++] = (char*)"-p";
		ssh_argv[ssh_argc++] = remote_port;
	}
	if (ssh_config != NULL) {
		ssh_argv[ssh_argc++] = (char*)"-F";
		ssh_argv[ssh_argc++] = ssh_config;
	}
	if (ssh_options != NULL) {
		ssh_argc = split_options(ssh_argc, ssh_argv, MAX_CMDLINE_OPTIONS, ssh_options);
	}
	ssh_argv[ssh_argc++] = remote_host;
	ssh_argv[ssh_argc++] = cmd;
	ssh_argv[ssh_argc] = NULL;

	if (remote_path)
	{
		char* sep = strrchr(pg_probackup, '/');
		if (sep != NULL) {
			pg_probackup = sep + 1;
		}
		dst = snprintf(cmd, sizeof(cmd), "%s/%s", remote_path, pg_probackup);
	} else {
		dst = snprintf(cmd, sizeof(cmd), "%s", pg_probackup);
	}
	for (i = 1; i < argc; i++) {
		dst = append_option(cmd, sizeof(cmd), dst, argv[i]);
	}
	dst = append_option(cmd, sizeof(cmd), dst, "--agent");
	dst = append_option(cmd, sizeof(cmd), dst, PROGRAM_VERSION);
	cmd[dst] = '\0';

	SYS_CHECK(pipe(infd));
	SYS_CHECK(pipe(outfd));

	SYS_CHECK(pid = fork());

	if (pid == 0) { /* child */
		SYS_CHECK(close(STDIN_FILENO));
		SYS_CHECK(close(STDOUT_FILENO));

		SYS_CHECK(dup2(outfd[0], STDIN_FILENO));
		SYS_CHECK(dup2(infd[1], STDOUT_FILENO));

		SYS_CHECK(close(infd[0]));
		SYS_CHECK(close(infd[1]));
		SYS_CHECK(close(outfd[0]));
		SYS_CHECK(close(outfd[1]));

		SYS_CHECK(execvp(ssh_argv[0], ssh_argv));
		return -1;
	} else {
		SYS_CHECK(close(outfd[0])); /* These are being used by the child */
		SYS_CHECK(close(infd[1]));

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

