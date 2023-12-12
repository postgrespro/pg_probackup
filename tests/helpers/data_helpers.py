import os
import re
import random
import string
import unittest
import time


def find_by_extension(dir, extensions, backup_dir=None):
    """
    find_by_extensions('path1',['.txt','.log'])

    Add backup_dir if we need to check files from backup folder
    :return:
    Return list of files by file extensions.
    If backup_dir is not passed, then file path include full path.
    Otherwise file path is relative to backup_dir.
    """
    if isinstance(extensions, str):
        extensions = [extensions]

    if backup_dir is not None:
        return [obj for obj in backup_dir.list_files(dir, recursive=True)
                if os.path.splitext(obj)[1] in extensions]

    return [os.path.join(rootdir, obj)
            for rootdir, dirs, objs in os.walk(dir, followlinks=True)
            for obj in objs
                     if os.path.splitext(obj)[1] in extensions]

def find_by_pattern(dir, pattern, backup_dir=None):
    """
    find_by_pattern('path1','^.*/*.txt')
    :return:
    Return list of files include full path by pattern
    """
    if backup_dir is not None:
        return [obj for obj in backup_dir.list_files(dir, recursive=True)
                if re.match(pattern, obj)]

    objs = (os.path.join(rootdir, obj)
            for rootdir, dirs, objs in os.walk(dir, followlinks=True)
            for obj in objs)
    return [obj for obj in objs if re.match(pattern, obj)]

def find_by_name(dir, filenames, backup_dir=None):
    if isinstance(filenames, str):
        filenames = [filenames]

    if backup_dir is not None:
        return [obj for obj in backup_dir.list_files(dir, recursive=True)
                if os.path.basename(obj) in filenames]

    return [os.path.join(rootdir, obj)
            for rootdir, dirs, objs in os.walk(dir, followlinks=True)
            for obj in objs
            if obj in filenames]


def corrupt_file(filename):
    file_size = None
    try:
        file_size = os.path.getsize(filename)
    except OSError:
        return False

    try:
        with open(filename, "rb+") as f:
            pos = random.randint(int(0.1*file_size),int(0.8*file_size))
            len = int(0.1 * file_size) + 1
            f.seek(pos)
            old = f.read(len)
            new = random_string(len)
            while new == old:
                new = random_string(len)
            f.seek(pos)
            f.write(new)
            f.close()
    except OSError:
        return False

    return True


def random_string(n):
    a = string.ascii_letters + string.digits
    random_str = ''.join([random.choice(a) for i in range(int(n)+1)])
    return str.encode(random_str)
#    return ''.join([random.choice(a) for i in range(int(n)+1)])


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

    def wait_archive_push_completed(self):
        self.wait(contains='archive-push completed successfully')
