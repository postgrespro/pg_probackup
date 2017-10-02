import os
import re
import random
import string


def find_by_extensions(dirs=None, extensions=None):
    """
    find_by_extensions(['path1','path2'],['.txt','.log'])
    :return:
    Return list of files include full path by file extensions
    """
    files = []
    new_dirs = []

    if dirs is not None and extensions is not None:
        for d in dirs:
            try:
                new_dirs += [os.path.join(d, f) for f in os.listdir(d)]
            except OSError:
                if os.path.splitext(d)[1] in extensions:
                    files.append(d)

        if new_dirs:
            files.extend(find_by_extensions(new_dirs, extensions))

    return files


def find_by_pattern(dirs=None, pattern=None):
    """
    find_by_pattern(['path1','path2'],'^.*/*.txt')
    :return:
    Return list of files include full path by pattern
    """
    files = []
    new_dirs = []

    if dirs is not None and pattern is not None:
        for d in dirs:
            try:
                new_dirs += [os.path.join(d, f) for f in os.listdir(d)]
            except OSError:
                if re.match(pattern,d):
                    files.append(d)

        if new_dirs:
            files.extend(find_by_pattern(new_dirs, pattern))

    return files


def find_by_name(dirs=None, filename=None):
    files = []
    new_dirs = []

    if dirs is not None and filename is not None:
        for d in dirs:
            try:
                new_dirs += [os.path.join(d, f) for f in os.listdir(d)]
            except OSError:
                if os.path.basename(d) in filename:
                    files.append(d)

        if new_dirs:
            files.extend(find_by_name(new_dirs, filename))

    return files


def corrupt_file(filename):
    file_size = None
    try:
        file_size = os.path.getsize(filename)
    except OSError:
        return False

    try:
        with open(filename, "rb+") as f:
            f.seek(random.randint(int(0.1*file_size),int(0.8*file_size)))
            f.write(random_string(0.1*file_size))
            f.close()
    except OSError:
        return False

    return True


def random_string(n):
    a = string.ascii_letters + string.digits
    return ''.join([random.choice(a) for i in range(int(n)+1)])