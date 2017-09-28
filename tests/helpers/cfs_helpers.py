import os

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