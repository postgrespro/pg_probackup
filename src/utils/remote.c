#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>

#ifdef WIN32
#define __thread __declspec(thread)
#else
#include <pthread.h>
#endif

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

static __thread int child_pid;

#if 0
static void kill_child(void)
{
	kill(child_pid, SIGTERM);
}
#endif


void wait_ssh(void)
{
/*
 * We need to wait termination of SSH process to eliminate zombies.
 * There is no waitpid() function at Windows but there are no zombie processes caused by lack of wait/waitpid.
 * So just disable waitpid for Windows.
 */
#ifndef WIN32
	int status;
	waitpid(child_pid, &status, 0);
	elog(LOG, "SSH process %d is terminated with status %d",  child_pid, status);
#endif
}

/*
 * On windows we launch a new pbk process via 'pg_probackup ssh ...'
 * so this process would new that it should exec ssh, because
 * there is no fork on Windows.
 */
#ifdef WIN32
void launch_ssh(char* argv[])
{
	int infd = atoi(argv[2]);
	int outfd = atoi(argv[3]);

	SYS_CHECK(close(STDIN_FILENO));
	SYS_CHECK(close(STDOUT_FILENO));

	SYS_CHECK(dup2(infd, STDIN_FILENO));
	SYS_CHECK(dup2(outfd, STDOUT_FILENO));

	SYS_CHECK(execvp(argv[4], argv+4));
}
#endif

static bool needs_quotes(char const* path)
{
	return strchr(path, ' ') != NULL;
}

bool launch_agent(void)
{
	char cmd[MAX_CMDLINE_LENGTH];
	char* ssh_argv[MAX_CMDLINE_OPTIONS];
	int ssh_argc;
	int outfd[2];
	int infd[2];
	int errfd[2];
	int agent_version;
	//TODO REVIEW XXX review buf_size
	int payload_buf_size = 1024 * 8;
	char payload_buf[payload_buf_size];

	ssh_argc = 0;
#ifdef WIN32
	ssh_argv[ssh_argc++] = PROGRAM_NAME_FULL;
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

	ssh_argv[ssh_argc++] = "-o";
	ssh_argv[ssh_argc++] = "PasswordAuthentication=no";

	ssh_argv[ssh_argc++] = "-o";
	ssh_argv[ssh_argc++] = "Compression=no";

	ssh_argv[ssh_argc++] = "-o";
	ssh_argv[ssh_argc++] = "ControlMaster=no";

	ssh_argv[ssh_argc++] = "-o";
	ssh_argv[ssh_argc++] = "LogLevel=error";

	ssh_argv[ssh_argc++] = instance_config.remote.host;
	ssh_argv[ssh_argc++] = cmd;
	ssh_argv[ssh_argc] = NULL;

	if (instance_config.remote.path)
	{
		char const* probackup = PROGRAM_NAME_FULL;
		char* sep = strrchr(probackup, '/');
		if (sep != NULL) {
			probackup = sep + 1;
		}
#ifdef WIN32
		else {
			sep = strrchr(probackup, '\\');
			if (sep != NULL) {
				probackup = sep + 1;
			}
		}
		if (needs_quotes(instance_config.remote.path) || needs_quotes(PROGRAM_NAME_FULL))
			snprintf(cmd, sizeof(cmd), "\"%s\\%s\" agent",
					 instance_config.remote.path, probackup);
		else
			snprintf(cmd, sizeof(cmd), "%s\\%s agent",
					 instance_config.remote.path, probackup);
#else
		if (needs_quotes(instance_config.remote.path) || needs_quotes(PROGRAM_NAME_FULL))
			snprintf(cmd, sizeof(cmd), "\"%s/%s\" agent",
					 instance_config.remote.path, probackup);
		else
			snprintf(cmd, sizeof(cmd), "%s/%s agent",
					 instance_config.remote.path, probackup);
#endif
	} else {
		if (needs_quotes(PROGRAM_NAME_FULL))
			snprintf(cmd, sizeof(cmd), "\"%s\" agent", PROGRAM_NAME_FULL);
		else
			snprintf(cmd, sizeof(cmd), "%s agent", PROGRAM_NAME_FULL);
	}

#ifdef WIN32
	SYS_CHECK(_pipe(infd, PIPE_SIZE, _O_BINARY)) ;
	SYS_CHECK(_pipe(outfd, PIPE_SIZE, _O_BINARY));
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
			return false;
	} else {
#endif
		elog(LOG, "Start SSH client process, pid %d", child_pid);
		SYS_CHECK(close(infd[1]));  /* These are being used by the child */
		SYS_CHECK(close(outfd[0]));
		SYS_CHECK(close(errfd[1]));
		/*atexit(kill_child);*/

		fio_redirect(infd[0], outfd[1], errfd[0]); /* write to stdout */
	}

	/* Make sure that remote agent has the same version, fork and other features to be binary compatible
	 */
	fio_get_agent_version(&agent_version, payload_buf, payload_buf_size);
	check_remote_agent_compatibility(agent_version, payload_buf, payload_buf_size);

	return true;
}

#define COMPATIBILITY_VAL(macro) #macro, macro
#define COMPATIBILITY_STR(macro) #macro
#define COMPATIBILITY_VAL_STR(macro) #macro, COMPATIBILITY_STR(macro)

#define COMPATIBILITY_VAL_SEPARATOR "="
#define COMPATIBILITY_LINE_SEPARATOR "\n"

static char* compatibility_params[] = {
	COMPATIBILITY_VAL(PG_MAJORVERSION),
	//TODO remove?
	//TODO doesn't work macro name check for ints!!!!
	COMPATIBILITY_VAL_STR(SIZEOF_VOID_P),
	//TODO REVIEW XXX can use edition.h/extract_pgpro_edition()
#ifdef PGPRO_EDN
	//TODO add vanilla
	//TODO make "1c" -> "vanilla"
	COMPATIBILITY_VAL(PGPRO_EDN),
#endif
};

/*
 * Compose compatibility string to be sent by pg_probackup agent
 * through ssh and to be verified by pg_probackup peer.
 * Compatibility string contains postgres essential vars as strings
 * in format "var_name" + COMPATIBILITY_VAL_SEPARATOR + "var_value" + COMPATIBILITY_LINE_SEPARATOR
 */
size_t prepare_compatibility_str(char* compatibility_buf, size_t compatibility_buf_size)
{
	char tmp_buf[1024];
	int size_inc = 0;
	size_t result_size = 1;
	size_t compatibility_params_array_size = sizeof compatibility_params / sizeof compatibility_params[0];;

	*compatibility_buf = '\0';
	Assert(compatibility_params_array_size % 2 == 0);

	//TODO !!!!
	for (int i = 0; i < compatibility_params_array_size; i+=2)
	{
		size_inc = snprintf(compatibility_buf + size_inc, compatibility_buf_size,
							"%s" COMPATIBILITY_VAL_SEPARATOR "%s" COMPATIBILITY_LINE_SEPARATOR,
							compatibility_params[i], compatibility_params[i+1]);

//		size_inc = snprintf(tmp_buf, sizeof tmp_buf,
//							"%s" COMPATIBILITY_VAL_SEPARATOR "%s" COMPATIBILITY_LINE_SEPARATOR,
//							compatibility_params[i], compatibility_params[i+1]);
		if (size_inc >= sizeof tmp_buf)
		{
			//TODO make Assert
			elog(ERROR, "Compatibility params from agent doesn't fit to %zu chars, %s=%s",
				 sizeof tmp_buf - 1, compatibility_params[i], compatibility_params[i+1] );
		}

		result_size += size_inc;
		if (result_size > compatibility_buf_size)
		{
			//TODO make Assert
			elog(ERROR, "Can't fit compatibility string size %zu to buffer size %zu:\n%s\n%s",
				 result_size, compatibility_buf_size, compatibility_buf, tmp_buf);
		}
		strcat(compatibility_buf, tmp_buf);
	}
	return result_size;
}

/*
 * Check incoming remote agent's compatibility params for equality to local ones.
 */
void check_remote_agent_compatibility(int agent_version, char *compatibility_str, size_t compatibility_str_max_size)
{
	elog(LOG, "Agent version=%d", agent_version);

	if (agent_version != AGENT_PROTOCOL_VERSION)
	{
		char agent_version_str[1024];
		sprintf(agent_version_str, "%d.%d.%d",
				agent_version / 10000,
				(agent_version / 100) % 100,
				agent_version % 100);

		elog(ERROR, "Remote agent protocol version %s does not match local program protocol version %s, "
					"consider to upgrade pg_probackup binary",
			agent_version_str, AGENT_PROTOCOL_VERSION_STR);
	}

	if (strnlen(compatibility_str, compatibility_str_max_size) == compatibility_str_max_size)
	{
		elog(ERROR, "Corrupted remote compatibility protocol: compatibility string has no terminating \\0");
	}

	elog(LOG, "Agent compatibility params: '%s'", compatibility_str);

	/* checking compatibility params */
	{
		char *buf[compatibility_str_max_size];

		prepare_compatibility_str(buf, sizeof buf);
		if(!strcmp(compatibility_str, buf))
		{
			elog(ERROR, "Incompatible agent params, expected %s", buf);
		}
	}

}
