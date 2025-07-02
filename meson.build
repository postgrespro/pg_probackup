pg_probackup_sources = files(
  'src/utils/configuration.c',
  'src/utils/json.c',
  'src/utils/logger.c',
  'src/utils/parray.c',
  'src/utils/pgut.c',
  'src/utils/thread.c',
  'src/utils/remote.c',
  'src/utils/file.c',
  'src/archive.c',
  'src/backup.c',
  'src/catalog.c',
  'src/checkdb.c',
  'src/configure.c',
  'src/data.c',
  'src/delete.c',
  'src/dir.c',
  'src/fetch.c',
  'src/help.c',
  'src/init.c',
  'src/merge.c',
  'src/parsexlog.c',
  'src/ptrack.c',
  'src/pg_probackup.c',
  'src/restore.c',
  'src/show.c',
  'src/stream.c',
  'src/util.c',
  'src/validate.c',
  'src/datapagemap.c',
  'src/catchup.c',
)

pg_probackup_sources += files('../../backend/access/transam/xlogreader.c')
pg_probackup_sources += files('../../backend/utils/hash/pg_crc.c')
pg_probackup_sources += files('../pg_basebackup/receivelog.c')
pg_probackup_sources += files('../pg_basebackup/streamutil.c')
pg_probackup_sources += files('../pg_basebackup/walmethods.c')

pg_probackup_deps = [frontend_code, backend_code, libpq, lz4, zlib, zstd]

if host_system == 'windows'
  pg_probackup_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_probackup',
    '--FILEDESC', 'pg_probackup - utility to manage backup and recovery of PostgreSQL database clusters',])
endif

pg_probackup = executable('pg_probackup',
  pg_probackup_sources,
  dependencies: pg_probackup_deps,
  include_directories: include_directories('./src', '../../include/portability', '../../port', '../pg_basebackup'),
  c_args: ['-DFRONTEND'], # needed for xlogreader et al
  kwargs: default_bin_args,
)
bin_targets += pg_probackup

subdir('po', if_found: libintl)
