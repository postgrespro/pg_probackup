"""
Utilities for accessing pg_probackup backup data on file system.
"""
import os
import shutil

from ....init_helpers import init_params


class TestBackupDir:

    def list_instance_backups(self, instance):
        raise NotImplementedError()

    def list_files(self, sub_dir, recursive=False):
        raise NotImplementedError()

    def list_dirs(self, sub_dir):
        raise NotImplementedError()

    def read_file(self, sub_path, *, text=True):
        raise NotImplementedError()

    def write_file(self, sub_path, data, *, text=True):
        raise NotImplementedError()

    def cleanup(self):
        raise NotImplementedError()

    def remove_file(self, sub_path):
        raise NotImplementedError()

    def remove_dir(self, sub_path):
        raise NotImplementedError()

    def exists(self, sub_path):
        raise NotImplementedError()


class FSTestBackupDir(TestBackupDir):
    is_file_based = True

    """ Backup directory. Usually created by running pg_probackup init -B <path>"""

    def __init__(self, *, rel_path, backup):
        self.path = os.path.join(init_params.tmp_path, rel_path, backup)
        self.pb_args = ('-B', self.path)

    def list_instance_backups(self, instance):
        full_path = os.path.join(self.path, 'backups', instance)
        return sorted((x for x in os.listdir(full_path)
                      if os.path.isfile(os.path.join(full_path, x, 'backup.control'))))

    def list_files(self, sub_dir, recursive=False):
        full_path = os.path.join(self.path, sub_dir)
        if not recursive:
            return [f for f in os.listdir(full_path)
                    if os.path.isfile(os.path.join(full_path, f))]
        files = []
        for rootdir, dirs, files_in_dir in os.walk(full_path):
            rootdir = rootdir[len(self.path) + 1:]
            files.extend(os.path.join(rootdir, file) for file in files_in_dir)
        return files

    def list_dirs(self, sub_dir):
        full_path = os.path.join(self.path, sub_dir)
        return [f for f in os.listdir(full_path)
                if os.path.isdir(os.path.join(full_path, f))]

    def read_file(self, sub_path, *, text=True):
        full_path = os.path.join(self.path, sub_path)
        with open(full_path, 'r' if text else 'rb') as fin:
            return fin.read()

    def write_file(self, sub_path, data, *, text=True):
        full_path = os.path.join(self.path, sub_path)
        with open(full_path, 'w' if text else 'wb') as fout:
            fout.write(data)

    def cleanup(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def remove_file(self, sub_path):
        os.remove(os.path.join(self.path, sub_path))

    def remove_dir(self, sub_path):
        full_path = os.path.join(self.path, sub_path)
        shutil.rmtree(full_path, ignore_errors=True)

    def exists(self, sub_path):
        full_path = os.path.join(self.path, sub_path)
        return os.path.exists(full_path)

    def __str__(self):
        return self.path

    def __repr__(self):
        return "FSTestBackupDir" + str(self.path)

    def __fspath__(self):
        return self.path
