import functools
import os
import re
import subprocess
import sys
import unittest
from time import sleep


class GdbException(Exception):
    def __init__(self, message="False"):
        self.message = message

    def __str__(self):
        return '\n ERROR: {0}\n'.format(repr(self.message))


class GDBobj:
    _gdb_enabled = False
    _gdb_ok = False
    _gdb_ptrace_ok = False

    def __init__(self, cmd, env, attach=False):
        self.verbose = env.verbose
        self.output = ''
        self._did_quit = False
        self.has_breakpoint = False

        # Check gdb flag is set up
        if not hasattr(env, "_gdb_decorated") or not env._gdb_decorated:
            raise GdbException("Test should be decorated with @needs_gdb")
        if not self._gdb_enabled:
            raise GdbException("No `PGPROBACKUP_GDB=on` is set.")
        if not self._gdb_ok:
            if not self._gdb_ptrace_ok:
                raise GdbException("set /proc/sys/kernel/yama/ptrace_scope to 0"
                                   " to run GDB tests")
            raise GdbException("No gdb usage possible.")

        # Check gdb presense
        try:
            gdb_version, _ = subprocess.Popen(
                ['gdb', '--version'],
                stdout=subprocess.PIPE
            ).communicate()
        except OSError:
            raise GdbException("Couldn't find gdb on the path")

        self.base_cmd = [
            'gdb',
            '--interpreter',
            'mi2',
        ]

        if attach:
            self.cmd = self.base_cmd + ['--pid'] + cmd
        else:
            self.cmd = self.base_cmd + ['--args'] + cmd

        # Get version
        gdb_version_number = re.search(
            br"^GNU gdb [^\d]*(\d+)\.(\d)",
            gdb_version)
        self.major_version = int(gdb_version_number.group(1))
        self.minor_version = int(gdb_version_number.group(2))

        if self.verbose:
            print([' '.join(map(str, self.cmd))])

        self.proc = subprocess.Popen(
            self.cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
            text=True,
            errors='replace',
        )
        self.gdb_pid = self.proc.pid

        while True:
            line = self.get_line()

            if 'No such process' in line:
                raise GdbException(line)

            if not line.startswith('(gdb)'):
                pass
            else:
                break

    def __del__(self):
        if not self._did_quit and hasattr(self, "proc"):
            try:
                self.quit()
            except subprocess.TimeoutExpired:
                self.kill()

    def get_line(self):
        line = self.proc.stdout.readline()
        self.output += line
        return line

    def kill(self):
        self._did_quit = True
        self.proc.kill()
        self.proc.wait(3)
        self.proc.stdin.close()
        self.proc.stdout.close()

    def set_breakpoint(self, location):

        result = self._execute('break ' + location)
        self.has_breakpoint = True
        for line in result:
            if line.startswith('~"Breakpoint'):
                return

            elif line.startswith('=breakpoint-created'):
                return

            elif line.startswith('^error'):  # or line.startswith('(gdb)'):
                break

            elif line.startswith('&"break'):
                pass

            elif line.startswith('&"Function'):
                raise GdbException(line)

            elif line.startswith('&"No line'):
                raise GdbException(line)

            elif line.startswith('~"Make breakpoint pending on future shared'):
                raise GdbException(line)

        raise GdbException(
            'Failed to set breakpoint.\n Output:\n {0}'.format(result)
        )

    def remove_all_breakpoints(self):
        if not self.has_breakpoint:
            return

        result = self._execute('delete')
        self.has_breakpoint = False
        for line in result:

            if line.startswith('^done'):
                return

        raise GdbException(
            'Failed to remove breakpoints.\n Output:\n {0}'.format(result)
        )

    def run_until_break(self):
        result = self._execute('run', False)
        for line in result:
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                return
        raise GdbException(
            'Failed to run until breakpoint.\n'
        )

    def continue_execution_until_running(self):
        result = self._execute('continue')

        for line in result:
            if line.startswith('*running') or line.startswith('^running'):
                return
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                continue
            if line.startswith('*stopped,reason="exited-normally"'):
                continue

        raise GdbException(
            'Failed to continue execution until running.\n'
        )

    def signal(self, sig):
        if 'KILL' in sig:
            self.remove_all_breakpoints()
        self._execute(f'signal {sig}')

    def continue_execution_until_exit(self):
        self.remove_all_breakpoints()
        result = self._execute('continue', False)

        for line in result:
            if line.startswith('*running'):
                continue
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                continue
            if (
                    line.startswith('*stopped,reason="exited') or
                    line == '*stopped\n'
            ):
                self.quit()
                return

        raise GdbException(
            'Failed to continue execution until exit.\n'
        )

    def continue_execution_until_error(self):
        self.remove_all_breakpoints()
        result = self._execute('continue', False)

        for line in result:
            if line.startswith('^error'):
                return
            if line.startswith('*stopped,reason="exited'):
                return
            if line.startswith(
                    '*stopped,reason="signal-received",signal-name="SIGABRT"'):
                return

        raise GdbException(
            'Failed to continue execution until error.\n')

    def continue_execution_until_break(self, ignore_count=0):
        if ignore_count > 0:
            result = self._execute(
                'continue ' + str(ignore_count),
                False
            )
        else:
            result = self._execute('continue', False)

        for line in result:
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                return
            if line.startswith('*stopped,reason="exited-normally"'):
                break

        raise GdbException(
            'Failed to continue execution until break.\n')

    def show_backtrace(self):
        return self._execute("backtrace", running=False)

    def stopped_in_breakpoint(self):
        while True:
            line = self.get_line()
            if self.verbose:
                print(line)
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                return True

    def detach(self):
        if not self._did_quit:
            self._execute('detach')

    def quit(self):
        if not self._did_quit:
            self._did_quit = True
            self.proc.terminate()
            self.proc.wait(3)
            self.proc.stdin.close()
            self.proc.stdout.close()

    # use for breakpoint, run, continue
    def _execute(self, cmd, running=True):
        output = []
        self.proc.stdin.flush()
        self.proc.stdin.write(cmd + '\n')
        self.proc.stdin.flush()
        sleep(1)

        # look for command we just send
        while True:
            line = self.get_line()
            if self.verbose:
                print(repr(line))

            if cmd not in line:
                continue
            else:
                break

        while True:
            line = self.get_line()
            output += [line]
            if self.verbose:
                print(repr(line))
            if line.startswith('^done') or line.startswith('*stopped'):
                break
            if line.startswith('^error'):
                break
            if running and (line.startswith('*running') or line.startswith('^running')):
                #            if running and line.startswith('*running'):
                break
        return output


def _set_gdb(self):
    test_env = os.environ.copy()
    self._gdb_enabled = test_env.get('PGPROBACKUP_GDB') == 'ON'
    self._gdb_ok = self._gdb_enabled
    if not self._gdb_enabled or sys.platform != 'linux':
        return
    try:
        with open('/proc/sys/kernel/yama/ptrace_scope') as f:
            ptrace = f.read()
    except FileNotFoundError:
        self._gdb_ptrace_ok = True
        return
    self._gdb_ptrace_ok = int(ptrace) == 0
    self._gdb_ok = self._gdb_ok and self._gdb_ptrace_ok


def _check_gdb_flag_or_skip_test():
    if not GDBobj._gdb_enabled:
        return ("skip",
                "Specify PGPROBACKUP_GDB and build without "
                "optimizations for run this test"
                )
    if GDBobj._gdb_ok:
        return None
    if not GDBobj._gdb_ptrace_ok:
        return ("fail", "set /proc/sys/kernel/yama/ptrace_scope to 0"
                        " to run GDB tests")
    else:
        return ("fail", "use of gdb is not possible")


def needs_gdb(func):
    check = _check_gdb_flag_or_skip_test()
    if not check:
        @functools.wraps(func)
        def ok_wrapped(self):
            self._gdb_decorated = True
            func(self)

        return ok_wrapped
    reason = check[1]
    if check[0] == "skip":
        return unittest.skip(reason)(func)
    elif check[0] == "fail":
        @functools.wraps(func)
        def fail_wrapper(self):
            self.fail(reason)

        return fail_wrapper
    else:
        raise "Wrong action {0}".format(check)


_set_gdb(GDBobj)
