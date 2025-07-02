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
		intptr_t pid = _spawnvp(_P_NOWAIT, ssh_argv[0], (const char *const *)ssh_argv);
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
		elog(LOG, "Start SSH client process, pid %d, cmd \"%s\"", child_pid, cmd);
		SYS_CHECK(close(infd[1]));  /* These are being used by the child */
		SYS_CHECK(close(outfd[0]));
		SYS_CHECK(close(errfd[1]));
		/*atexit(kill_child);*/

		fio_redirect(infd[0], outfd[1], errfd[0]); /* write to stdout */
	}


	/* Make sure that remote agent has the same version, fork and other features to be binary compatible */
	{
		char payload_buf[1024];
		fio_get_agent_version(&agent_version, payload_buf, sizeof payload_buf);
		check_remote_agent_compatibility(agent_version, payload_buf, sizeof payload_buf);
	}

	return true;
}

#ifdef PGPRO_EDITION
/* PGPRO 10-13 checks to be "(certified)", with exceptional case PGPRO_11 conforming to "(standard certified)" */
static bool check_certified()
{
	return strstr(PGPRO_VERSION_STR, "(certified)") ||
		   strstr(PGPRO_VERSION_STR, "(standard certified)");
}
#endif

static char* extract_pg_edition_str()
{
	static char *vanilla = "vanilla";
#ifdef PGPRO_EDITION
	static char *_1C = "1C";
	static char *std = "standard";
	static char *ent = "enterprise";
	static char *std_cert = "standard-certified";
	static char *ent_cert = "enterprise-certified";

	if (strcmp(PGPRO_EDITION, _1C) == 0)
		return vanilla;

	if (PG_VERSION_NUM < 100000)
		return PGPRO_EDITION;

	/* these "certified" checks are applicable to PGPRO from 10 up to 12 versions.
	 * 13+ certified versions are compatible to non-certified ones */
	if (PG_VERSION_NUM < 130000 && check_certified())
	{
		if (strcmp(PGPRO_EDITION, std) == 0)
			return std_cert;
		else if (strcmp(PGPRO_EDITION, ent) == 0)
			return ent_cert;
		else
			Assert("Bad #define PGPRO_EDITION value" == 0);
	}

	return PGPRO_EDITION;
#else
	return vanilla;
#endif
}

#define COMPATIBILITY_VAL_STR(macro) { #macro, macro, 0 }
#define COMPATIBILITY_VAL_INT(macro) { #macro, NULL, macro }

#define COMPATIBILITY_VAL_SEPARATOR "="
#define COMPATIBILITY_LINE_SEPARATOR "\n"

/*
 * Compose compatibility string to be sent by pg_probackup agent
 * through ssh and to be verified by pg_probackup peer.
 * Compatibility string contains postgres essential vars as strings
 * in format "var_name" + COMPATIBILITY_VAL_SEPARATOR + "var_value" + COMPATIBILITY_LINE_SEPARATOR
 */
size_t prepare_compatibility_str(char* compatibility_buf, size_t compatibility_buf_size)
{
	typedef struct compatibility_param_tag {
		const char* name;
		const char* strval;
		int intval;
	} compatibility_param;

	compatibility_param compatibility_params[] = {
		COMPATIBILITY_VAL_STR(PG_MAJORVERSION),
		{ "edition", extract_pg_edition_str(), 0 },
		COMPATIBILITY_VAL_INT(SIZEOF_VOID_P),
	};

	size_t result_size = 0;
	int i;
	*compatibility_buf = '\0';

	for (i = 0; i < (sizeof compatibility_params / sizeof(compatibility_param)); i++)
	{
		if (compatibility_params[i].strval != NULL)
			result_size += snprintf(compatibility_buf + result_size, compatibility_buf_size - result_size,
									"%s" COMPATIBILITY_VAL_SEPARATOR "%s" COMPATIBILITY_LINE_SEPARATOR,
									compatibility_params[i].name,
									compatibility_params[i].strval);
		else
			result_size += snprintf(compatibility_buf + result_size, compatibility_buf_size - result_size,
									"%s" COMPATIBILITY_VAL_SEPARATOR "%d" COMPATIBILITY_LINE_SEPARATOR,
									compatibility_params[i].name,
									compatibility_params[i].intval);
		Assert(result_size < compatibility_buf_size);
	}
	return result_size + 1;
}

/*
 * Check incoming remote agent's compatibility params for equality to local ones.
 */
void check_remote_agent_compatibility(int agent_version, char *compatibility_str, size_t compatibility_str_max_size)
{
	elog(LOG, "Agent version=%d\n", agent_version);

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

	/* checking compatibility params */
	if (strnlen(compatibility_str, compatibility_str_max_size) == compatibility_str_max_size)
	{
		elog(ERROR, "Corrupted remote compatibility protocol: compatibility string has no terminating \\0");
	}

	elog(LOG, "Agent compatibility params:\n%s", compatibility_str);

	{
		char buf[1024];

		prepare_compatibility_str(buf, sizeof buf);
		if(strcmp(compatibility_str, buf))
		{
			elog(ERROR, "Incompatible remote agent params, expected:\n%s, actual:\n:%s", buf, compatibility_str);
		}
	}
}
