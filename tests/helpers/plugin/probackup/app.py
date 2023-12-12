import json
import os
import re
import subprocess
import unittest

from .storage.fs_backup import TestBackupDir
from ...gdb import GDBobj
from ...init_helpers import init_params

warning = """
Wrong splint in show_pb
Original Header:
{header}
Original Body:
{body}
Splitted Header
{header_split}
Splitted Body
{body_split}
"""


class ProbackupException(Exception):
    def __init__(self, message, cmd):
        self.message = message
        self.cmd = cmd

    def __str__(self):
        return '\n ERROR: {0}\n CMD: {1}'.format(repr(self.message), self.cmd)


class ProbackupApp:

    def __init__(self, test_class: unittest.TestCase,
                 pg_node, pb_log_path, test_env, auto_compress_alg, backup_dir):
        self.test_class = test_class
        self.pg_node = pg_node
        self.pb_log_path = pb_log_path
        self.test_env = test_env
        self.auto_compress_alg = auto_compress_alg
        self.backup_dir = backup_dir
        self.probackup_path = init_params.probackup_path
        self.probackup_old_path = init_params.probackup_old_path
        self.remote = init_params.remote
        self.verbose = init_params.verbose
        self.archive_compress = init_params.archive_compress
        self.test_class.output = None

    def run(self, command, gdb=False, old_binary=False, return_id=True, env=None,
            skip_log_directory=False, expect_error=False, use_backup_dir=True):
        """
        Run pg_probackup
        backup_dir: target directory for making backup
        command: commandline options
        expect_error: option for ignoring errors and getting error message as a result of running the function
        gdb: when True it returns GDBObj(), when tuple('suspend', port) it runs probackup
             in suspended gdb mode with attachable gdb port, for local debugging
        """
        if isinstance(use_backup_dir, TestBackupDir):
            command = [command[0], *use_backup_dir.pb_args, *command[1:]]
        elif use_backup_dir:
            command = [command[0], *self.backup_dir.pb_args, *command[1:]]

        if not self.probackup_old_path and old_binary:
            print('PGPROBACKUPBIN_OLD is not set')
            exit(1)

        if old_binary:
            binary_path = self.probackup_old_path
        else:
            binary_path = self.probackup_path

        if not env:
            env = self.test_env

        strcommand = ' '.join(str(p) for p in command)
        if '--log-level-file' in strcommand and \
                '--log-directory' not in strcommand and \
                not skip_log_directory:
            command += ['--log-directory=' + self.pb_log_path]
            strcommand += ' ' + command[-1]

        if 'pglz' in strcommand and \
                ' -j' not in strcommand and '--thread' not in strcommand:
            command += ['-j', '1']
            strcommand += ' -j 1'

        self.test_class.cmd = binary_path + ' ' + strcommand
        if self.verbose:
            print(self.test_class.cmd)

        cmdline = [binary_path, *command]
        if gdb is True:
            # general test flow for using GDBObj
            return GDBobj(cmdline, self.test_class)

        try:
            result = None
            if type(gdb) is tuple and gdb[0] == 'suspend':
                # special test flow for manually debug probackup
                gdb_port = gdb[1]
                cmdline = ['gdbserver'] + ['localhost:' + str(gdb_port)] + cmdline
                print("pg_probackup gdb suspended, waiting gdb connection on localhost:{0}".format(gdb_port))

            self.test_class.output = subprocess.check_output(
                cmdline,
                stderr=subprocess.STDOUT,
                env=env
            ).decode('utf-8', errors='replace')
            if command[0] == 'backup' and return_id:
                # return backup ID
                for line in self.test_class.output.splitlines():
                    if 'INFO: Backup' and 'completed' in line:
                        result = line.split()[2]
            else:
                result = self.test_class.output
            if expect_error is True:
                assert False, f"Exception was expected, but run finished successful with result: `{result}`\n" \
                              f"CMD: {self.test_class.cmd}"
            elif expect_error:
                assert False, f"Exception was expected {expect_error}, but run finished successful with result: `{result}`\n" \
                              f"CMD: {self.test_class.cmd}"
            return result
        except subprocess.CalledProcessError as e:
            self.test_class.output = e.output.decode('utf-8').replace("\r", "")
            if expect_error:
                return self.test_class.output
            else:
                raise ProbackupException(self.test_class.output, self.test_class.cmd)

    def init(self, options=None, old_binary=False, skip_log_directory=False, expect_error=False, use_backup_dir=True):
        if options is None:
            options = []
        return self.run([
                     'init',
                 ] + options,
                 old_binary=old_binary,
                 skip_log_directory=skip_log_directory,
                 expect_error=expect_error,
                 use_backup_dir=use_backup_dir
                 )

    def add_instance(self, instance, node, old_binary=False, options=None, expect_error=False):
        if options is None:
            options = []
        cmd = [
            'add-instance',
            '--instance={0}'.format(instance),
            '-D', node.data_dir
        ]

        # don`t forget to kill old_binary after remote ssh release
        if self.remote and not old_binary:
            options = options + [
                '--remote-proto=ssh',
                '--remote-host=localhost']

        return self.run(cmd + options, old_binary=old_binary, expect_error=expect_error)

    def set_config(self, instance, old_binary=False, options=None, expect_error=False):
        if options is None:
            options = []
        cmd = [
            'set-config',
            '--instance={0}'.format(instance),
        ]

        return self.run(cmd + options, old_binary=old_binary, expect_error=expect_error)

    def set_backup(self, instance, backup_id=False,
                   old_binary=False, options=None, expect_error=False):
        if options is None:
            options = []
        cmd = [
            'set-backup',
        ]

        if instance:
            cmd = cmd + ['--instance={0}'.format(instance)]

        if backup_id:
            cmd = cmd + ['-i', backup_id]

        return self.run(cmd + options, old_binary=old_binary, expect_error=expect_error)

    def del_instance(self, instance, old_binary=False, expect_error=False):

        return self.run([
            'del-instance',
            '--instance={0}'.format(instance),
        ],
            old_binary=old_binary,
            expect_error=expect_error
        )

    def backup_node(
            self, instance, node, data_dir=False,
            backup_type='full', datname=False, options=None,
            gdb=False,
            old_binary=False, return_id=True, no_remote=False,
            env=None,
            expect_error=False,
            sync=False
    ):
        if options is None:
            options = []
        if not node and not data_dir:
            print('You must provide ether node or data_dir for backup')
            exit(1)

        if not datname:
            datname = 'postgres'

        cmd_list = [
            'backup',
            '--instance={0}'.format(instance),
            # "-D", pgdata,
            '-p', '%i' % node.port,
            '-d', datname
        ]

        if data_dir:
            cmd_list += ['-D', self._node_dir(data_dir)]

        # don`t forget to kill old_binary after remote ssh release
        if self.remote and not old_binary and not no_remote:
            options = options + [
                '--remote-proto=ssh',
                '--remote-host=localhost']

        if self.auto_compress_alg and '--compress' in options and \
                self.archive_compress and self.archive_compress != 'zlib':
            options = [o if o != '--compress' else f'--compress-algorithm={self.archive_compress}'
                       for o in options]

        if backup_type:
            cmd_list += ['-b', backup_type]

        if not (old_binary or sync):
            cmd_list += ['--no-sync']

        return self.run(cmd_list + options, gdb, old_binary, return_id, env=env,
                        expect_error=expect_error)

    def backup_replica_node(self, instance, node, data_dir=False, *,
                            master, backup_type='full', datname=False,
                            options=None, env=None):
        """
        Try to reliably run backup on replica by switching wal at master
        at the moment pg_probackup is waiting for archived wal segment
        """
        if options is None:
            options = []
        assert '--stream' not in options or backup_type == 'page', \
            "backup_replica_node should be used with one of archive-mode or " \
            "page-stream mode"

        options = options.copy()
        if not any('--log-level-file' in x for x in options):
            options.append('--log-level-file=INFO')

        gdb = self.backup_node(
            instance, node, data_dir,
            backup_type=backup_type,
            datname=datname,
            options=options,
            env=env,
            gdb=True)
        gdb.set_breakpoint('wait_wal_lsn')
        # we need to break on wait_wal_lsn in pg_stop_backup
        gdb.run_until_break()
        if backup_type == 'page':
            self.test_class.switch_wal_segment(master)
        if '--stream' not in options:
            gdb.continue_execution_until_break()
        self.test_class.switch_wal_segment(master)
        gdb.continue_execution_until_exit()

        output = self.test_class.read_pb_log()
        self.test_class.unlink_pg_log()
        parsed_output = re.compile(r'Backup \S+ completed').search(output)
        assert parsed_output, f"Expected: `Backup 'backup_id' completed`, but found `{output}`"
        backup_id = parsed_output[0].split(' ')[1]
        return (backup_id, output)

    def checkdb_node(
            self, use_backup_dir=False, instance=False, data_dir=False,
            options=None, gdb=False, old_binary=False,
            skip_log_directory=False,
            expect_error=False
    ):
        if options is None:
            options = []
        cmd_list = ["checkdb"]

        if instance:
            cmd_list += ["--instance={0}".format(instance)]

        if data_dir:
            cmd_list += ["-D", self._node_dir(data_dir)]

        return self.run(cmd_list + options, gdb, old_binary,
                        skip_log_directory=skip_log_directory, expect_error=expect_error,
                        use_backup_dir=use_backup_dir)

    def merge_backup(
            self, instance, backup_id,
            gdb=False, old_binary=False, options=None, expect_error=False):
        if options is None:
            options = []
        cmd_list = [
            'merge',
            '--instance={0}'.format(instance),
            '-i', backup_id
        ]

        return self.run(cmd_list + options, gdb, old_binary, expect_error=expect_error)

    def restore_node(
            self, instance, node=None, restore_dir=None,
            backup_id=None, old_binary=False, options=None,
            gdb=False,
            expect_error=False,
            sync=False
    ):
        if options is None:
            options = []
        if node:
            data_dir = node.data_dir
        elif restore_dir:
            data_dir = self._node_dir(restore_dir)
        else:
            raise ValueError("You must provide ether node or base_dir for backup")

        cmd_list = [
            'restore',
            '-D', data_dir,
            '--instance={0}'.format(instance)
        ]

        # don`t forget to kill old_binary after remote ssh release
        if self.remote and not old_binary:
            options = options + [
                '--remote-proto=ssh',
                '--remote-host=localhost']

        if backup_id:
            cmd_list += ['-i', backup_id]

        if not (old_binary or sync):
            cmd_list += ['--no-sync']

        return self.run(cmd_list + options, gdb=gdb, old_binary=old_binary, expect_error=expect_error)

    def catchup_node(
            self,
            backup_mode, source_pgdata, destination_node,
            options=None,
            remote_host='localhost',
            expect_error=False,
            gdb=False
    ):

        if options is None:
            options = []
        cmd_list = [
            'catchup',
            '--backup-mode={0}'.format(backup_mode),
            '--source-pgdata={0}'.format(source_pgdata),
            '--destination-pgdata={0}'.format(destination_node.data_dir)
        ]
        if self.remote:
            cmd_list += ['--remote-proto=ssh', '--remote-host=%s' % remote_host]
        if self.verbose:
            cmd_list += [
                '--log-level-file=VERBOSE',
                '--log-directory={0}'.format(destination_node.logs_dir)
            ]

        return self.run(cmd_list + options, gdb=gdb, expect_error=expect_error, use_backup_dir=False)

    def show(
            self, instance=None, backup_id=None,
            options=None, as_text=False, as_json=True, old_binary=False,
            env=None,
            expect_error=False,
            gdb=False
    ):

        if options is None:
            options = []
        backup_list = []
        specific_record = {}
        cmd_list = [
            'show',
        ]
        if instance:
            cmd_list += ['--instance={0}'.format(instance)]

        if backup_id:
            cmd_list += ['-i', backup_id]

        # AHTUNG, WARNING will break json parsing
        if as_json:
            cmd_list += ['--format=json', '--log-level-console=error']

        if as_text:
            # You should print it when calling as_text=true
            return self.run(cmd_list + options, old_binary=old_binary, env=env,
                            expect_error=expect_error, gdb=gdb)

        # get show result as list of lines
        if as_json:
            text_json = str(self.run(cmd_list + options, old_binary=old_binary, env=env,
                                     expect_error=expect_error, gdb=gdb))
            try:
                if expect_error:
                    return text_json
                data = json.loads(text_json)
            except ValueError:
                assert False, f"Couldn't parse {text_json} as json. " \
                              f"Check that you don't have additional messages inside the log or use 'as_text=True'"

            for instance_data in data:
                # find specific instance if requested
                if instance and instance_data['instance'] != instance:
                    continue

                for backup in reversed(instance_data['backups']):
                    # find specific backup if requested
                    if backup_id:
                        if backup['id'] == backup_id:
                            return backup
                    else:
                        backup_list.append(backup)

            if backup_id is not None:
                assert False, "Failed to find backup with ID: {0}".format(backup_id)

            return backup_list
        else:
            show_splitted = self.run(cmd_list + options, old_binary=old_binary, env=env,
                                     expect_error=expect_error).splitlines()
            if instance is not None and backup_id is None:
                # cut header(ID, Mode, etc) from show as single string
                header = show_splitted[1:2][0]
                # cut backup records from show as single list
                # with string for every backup record
                body = show_splitted[3:]
                # inverse list so oldest record come first
                body = body[::-1]
                # split string in list with string for every header element
                header_split = re.split('  +', header)
                # Remove empty items
                for i in header_split:
                    if i == '':
                        header_split.remove(i)
                        continue
                header_split = [
                    header_element.rstrip() for header_element in header_split
                ]
                for backup_record in body:
                    backup_record = backup_record.rstrip()
                    # split list with str for every backup record element
                    backup_record_split = re.split('  +', backup_record)
                    # Remove empty items
                    for i in backup_record_split:
                        if i == '':
                            backup_record_split.remove(i)
                    if len(header_split) != len(backup_record_split):
                        print(warning.format(
                            header=header, body=body,
                            header_split=header_split,
                            body_split=backup_record_split)
                        )
                        exit(1)
                    new_dict = dict(zip(header_split, backup_record_split))
                    backup_list.append(new_dict)
                return backup_list
            else:
                # cut out empty lines and lines started with #
                # and other garbage then reconstruct it as dictionary
                # print show_splitted
                sanitized_show = [item for item in show_splitted if item]
                sanitized_show = [
                    item for item in sanitized_show if not item.startswith('#')
                ]
                # print sanitized_show
                for line in sanitized_show:
                    name, var = line.partition(' = ')[::2]
                    var = var.strip('"')
                    var = var.strip("'")
                    specific_record[name.strip()] = var

                if not specific_record:
                    assert False, "Failed to find backup with ID: {0}".format(backup_id)

                return specific_record

    def show_archive(
            self, instance=None, options=None,
            as_text=False, as_json=True, old_binary=False,
            tli=0,
            expect_error=False
    ):
        if options is None:
            options = []
        cmd_list = [
            'show',
            '--archive',
        ]
        if instance:
            cmd_list += ['--instance={0}'.format(instance)]

        # AHTUNG, WARNING will break json parsing
        if as_json:
            cmd_list += ['--format=json', '--log-level-console=error']

        if as_text:
            # You should print it when calling as_text=true
            return self.run(cmd_list + options, old_binary=old_binary, expect_error=expect_error)

        if as_json:
            if as_text:
                data = self.run(cmd_list + options, old_binary=old_binary, expect_error=expect_error)
            else:
                data = json.loads(self.run(cmd_list + options, old_binary=old_binary, expect_error=expect_error))

            if instance:
                instance_timelines = None
                for instance_name in data:
                    if instance_name['instance'] == instance:
                        instance_timelines = instance_name['timelines']
                        break

                if tli > 0:
                    timeline_data = None
                    for timeline in instance_timelines:
                        if timeline['tli'] == tli:
                            return timeline

                    return {}

                if instance_timelines:
                    return instance_timelines

            return data
        else:
            show_splitted = self.run(cmd_list + options, old_binary=old_binary,
                                     expect_error=expect_error).splitlines()
            print(show_splitted)
            exit(1)

    def validate(
            self, instance=None, backup_id=None,
            options=None, old_binary=False, gdb=False, expect_error=False
    ):
        if options is None:
            options = []
        cmd_list = [
            'validate',
        ]
        if instance:
            cmd_list += ['--instance={0}'.format(instance)]
        if backup_id:
            cmd_list += ['-i', backup_id]

        return self.run(cmd_list + options, old_binary=old_binary, gdb=gdb,
                        expect_error=expect_error)

    def delete(
            self, instance, backup_id=None,
            options=None, old_binary=False, gdb=False, expect_error=False):
        if options is None:
            options = []
        cmd_list = [
            'delete',
        ]

        cmd_list += ['--instance={0}'.format(instance)]
        if backup_id:
            cmd_list += ['-i', backup_id]

        return self.run(cmd_list + options, old_binary=old_binary, gdb=gdb,
                        expect_error=expect_error)

    def delete_expired(
            self, instance, options=None, old_binary=False, expect_error=False):
        if options is None:
            options = []
        cmd_list = [
            'delete',
            '--instance={0}'.format(instance)
        ]
        return self.run(cmd_list + options, old_binary=old_binary, expect_error=expect_error)

    def show_config(self, instance, old_binary=False, expect_error=False, gdb=False):
        out_dict = {}
        cmd_list = [
            'show-config',
            '--instance={0}'.format(instance)
        ]

        res = self.run(cmd_list, old_binary=old_binary, expect_error=expect_error, gdb=gdb).splitlines()
        for line in res:
            if not line.startswith('#'):
                name, var = line.partition(' = ')[::2]
                out_dict[name] = var
        return out_dict

    def run_binary(self, command, asynchronous=False, env=None):

        if not env:
            env = self.test_env

        if self.verbose:
            print([' '.join(map(str, command))])
        try:
            if asynchronous:
                return subprocess.Popen(
                    command,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env
                )
            else:
                self.test_class.output = subprocess.check_output(
                    command,
                    stderr=subprocess.STDOUT,
                    env=env
                ).decode('utf-8')
                return self.test_class.output
        except subprocess.CalledProcessError as e:
            raise ProbackupException(e.output.decode('utf-8'), command)

    def _node_dir(self, base_dir):
        return os.path.join(self.pg_node.test_path, base_dir)

    def set_archiving(
            self, instance, node, replica=False,
            overwrite=False, compress=True, old_binary=False,
            log_level=False, archive_timeout=False,
            custom_archive_command=None):

        # parse postgresql.auto.conf
        options = {}
        if replica:
            options['archive_mode'] = 'always'
            options['hot_standby'] = 'on'
        else:
            options['archive_mode'] = 'on'

        if custom_archive_command is None:
            archive_command = " ".join([f'"{init_params.probackup_path}"',
                                        'archive-push', *self.backup_dir.pb_args])
            if os.name == "nt":
                archive_command = archive_command.replace("\\", "\\\\")
            archive_command += f' --instance={instance}'

            # don`t forget to kill old_binary after remote ssh release
            if init_params.remote and not old_binary:
                archive_command += ' --remote-proto=ssh --remote-host=localhost'

            if init_params.archive_compress and compress:
                archive_command += ' --compress-algorithm='+init_params.archive_compress

            if overwrite:
                archive_command += ' --overwrite'

            archive_command += ' --log-level-console=VERBOSE'
            archive_command += ' -j 5'
            archive_command += ' --batch-size 10'
            archive_command += ' --no-sync'

            if archive_timeout:
                archive_command += f' --archive-timeout={archive_timeout}'

            if os.name == 'posix':
                archive_command += ' --wal-file-path=%p --wal-file-name=%f'

            elif os.name == 'nt':
                archive_command += ' --wal-file-path="%p" --wal-file-name="%f"'

            if log_level:
                archive_command += f' --log-level-console={log_level}'
        else:  # custom_archive_command is not None
            archive_command = custom_archive_command
        options['archive_command'] = archive_command

        node.set_auto_conf(options)
