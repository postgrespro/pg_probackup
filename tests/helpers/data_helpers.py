import re
import unittest
import functools
import time

def _tail_file(file, linetimeout, totaltimeout):
    start = time.time()
    with open(file, 'r') as f:
        waits = 0
        while waits < linetimeout:
            line = f.readline()
            if line == '':
                waits += 1
                time.sleep(1)
                continue
            waits = 0
            yield line
            if time.time() - start > totaltimeout:
                raise TimeoutError("total timeout tailing %s" % (file,))
        else:
            raise TimeoutError("line timeout tailing %s" % (file,))


class tail_file(object): # snake case to immitate function
    def __init__(self, filename, *, linetimeout=10, totaltimeout=60, collect=False):
        self.filename = filename
        self.tailer = _tail_file(filename, linetimeout, totaltimeout)
        self.collect = collect
        self.lines = []
        self._content = None

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.tailer)
        if self.collect:
            self.lines.append(line)
            self._content = None
        return line

    @property
    def content(self):
        if not self.collect:
            raise AttributeError("content collection is not enabled",
                                 name="content", obj=self)
        if not self._content:
            self._content = "".join(self.lines)
        return self._content

    def drop_content(self):
        self.lines.clear()
        self._content = None

    def stop_collect(self):
        self.drop_content()
        self.collect = False

    def wait(self, *, contains:str = None, regex:str = None):
        assert contains != None or regex != None
        assert contains == None or regex == None
        try:
            for line in self:
                if contains is not None and contains in line:
                    break
                if regex is not None and re.search(regex, line):
                    break
        except TimeoutError:
            msg = "Didn't found expected "
            if contains is not None:
                msg += repr(contains)
            elif regex is not None:
                msg += f"/{regex}/"
            msg += f" in {self.filename}"
            raise unittest.TestCase.failureException(msg)

    def wait_shutdown(self):
        self.wait(contains='database system is shut down')
