import os
import re
import random
import string
import unittest
import time
from array import array
import struct


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


def get_page_size(filename):
    # fixed PostgreSQL page header size
    PAGE_HEADER_SIZE = 24
    with open(filename, "rb+") as f:
        page_header = f.read(PAGE_HEADER_SIZE)
    assert len(page_header) == PAGE_HEADER_SIZE

    size = struct.unpack('H', page_header[18:20])[0] & 0xff00
    assert (size & (size - 1)) == 0

    return size


def pg_checksum_block(raw_page, blkno):
    N_SUMS = 32
    # prime multiplier of FNV-1a hash
    FNV_PRIME = 16777619
    MASK = (1<<32) - 1

    # Set pd_checksum to zero, so that the checksum calculation isn't
    # affected by the old checksum stored on the page.
    assert array('I').itemsize == 4
    page = array('I', raw_page[:8] + bytes([0, 0]) + raw_page[10:])

    assert len(page) % N_SUMS == 0

    sums = [
        0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A,
        0x79FF467A, 0x9BB9F8A3, 0x217E7CD2, 0x83E13D2C,
        0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA,
        0x7B093B5D, 0x98DAFF3C, 0xF718902A, 0x0B1C9CDB,
        0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE,
        0x92BEC979, 0xCCA6C0B2, 0x304A0979, 0x85AA43D4,
        0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E,
        0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756
    ]

    def mix2sum(s, v):
        tmp = s ^ v
        return ((tmp * FNV_PRIME) & MASK) ^ (tmp >> 17)

    def mix_chunk2sums(sums, values):
        return [mix2sum(s, v) for s, v in zip(sums, values)]

    # main checksum calculation
    for i in range(0, len(page), N_SUMS):
        sums = mix_chunk2sums(sums, page[i:i+N_SUMS])

    # finally add in two rounds of zeroes for additional mixing
    for _ in range(2):
        sums = mix_chunk2sums(sums, [0] * N_SUMS)

    # xor fold partial checksums together
    result = blkno
    for s in sums:
        result ^= s

    return result % 65535 + 1


def validate_data_file(filename, blcksz = 0) -> bool:
    file_size = os.path.getsize(filename)
    if blcksz == 0:
        blcksz = get_page_size(filename)
    assert file_size % blcksz == 0

    # determine positional number of first page based on segment number
    fname = os.path.basename(filename)
    if '.' in fname:
        segno = int(fname.rsplit('.', 1)[1])
        # Hardwired segments size 1GB
        basepage = (1<<30) / blcksz * segno
    else:
        basepage = 0

    with open(filename, "rb") as f:
        for blckno in range(file_size // blcksz):
            raw_page = f.read(blcksz)
            if len(raw_page) == 0:
                break
            if len(raw_page) != blcksz:
                return False
            checksum = struct.unpack('H', raw_page[8:10])[0]

            calculated_checksum = pg_checksum_block(raw_page, basepage + blckno)
            if checksum != calculated_checksum:
                return False

    return True


def corrupt_data_file(filename):
    blcksz = get_page_size(filename)
    try:
        while True:
            if not corrupt_file(filename):
                return False
            if not validate_data_file(filename, blcksz):
                return True
    except OSError:
        return False


def corrupt_file(filename):
    file_size = None
    try:
        file_size = os.path.getsize(filename)

        with open(filename, "rb+") as f:
            pos = random.randint(int(0.1*file_size),int(0.8*file_size))
            len = int(0.1 * file_size) + 1
            f.seek(pos)
            old = f.read(len)
            new = random_string(len, old)
            f.seek(pos)
            f.write(new)
    except OSError:
        return False

    return True


def random_string(n, old_bytes=b''):
    """
    Generate random string so that it's not equal neither to old bytes nor
    to casefold text of these bytes
    """
    old_str = old_bytes.decode('latin-1', errors='replace').casefold()
    template = string.ascii_letters + string.digits
    random_bytes = old_bytes
    random_str = old_str
    while random_bytes == old_bytes or random_str.casefold() == old_str:
        random_str = ''.join([random.choice(template) for i in range(int(n))])
        random_bytes = str.encode(random_str)
    return random_bytes


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
