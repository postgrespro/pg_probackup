#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "pg_probackup.h"
#include "file.h"

#define MAX_CMDLINE_LENGTH 4096

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

int remote_execute(int argc, char* argv[], bool listen)
{
	char cmd[MAX_CMDLINE_LENGTH];
	size_t dst = 0;
	char* ssh_argv[8];
	int ssh_argc;
	int i;
	int outfd[2];
	int infd[2];
	pid_t pid;

	ssh_argc = 0;
	ssh_argv[ssh_argc++] = remote_proto;
	if (remote_port != 0) {
		ssh_argv[ssh_argc++] = (char*)"-p";
		ssh_argv[ssh_argc++] = remote_port;
	}
	if (ssh_config != 0) {
		ssh_argv[ssh_argc++] = (char*)"-F";
		ssh_argv[ssh_argc++] = ssh_config;
	}
	ssh_argv[ssh_argc++] = remote_host;
	ssh_argv[ssh_argc++] = cmd+1;
	ssh_argv[ssh_argc] = NULL;

	for (i = 0; i < argc; i++) {
		dst = append_option(cmd, sizeof(cmd), dst, argv[i]);
	}
	dst = append_option(cmd, sizeof(cmd), dst, "--agent");
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

