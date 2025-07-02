common_sources = files(
  'src/utils/configuration.c',
  'src/utils/json.c',
  'src/utils/parray.c',
  'src/utils/pgut.c',
  'src/utils/thread.c',
  'src/utils/remote.c',
  'src/utils/file.c',
  'src/catalog.c',
  'src/checkdb.c',
  'src/configure.c',
  'src/data.c',
  'src/delete.c',
  'src/dir.c',
  'src/fetch.c',
  'src/help.c',
  'src/merge.c',
  'src/parsexlog.c',
  'src/ptrack.c',
  'src/restore.c',
  'src/show.c',
  'src/stream.c',
  'src/util.c',
  'src/validate.c',
  'src/datapagemap.c',
  'src/catchup.c',
)

common_sources += files('../../include/portability/instr_time.h')
common_sources += files('../../backend/access/transam/xlogreader.c')
common_sources += files('../../backend/utils/hash/pg_crc.c')
common_sources += files('../pg_basebackup/receivelog.h')
common_sources += files('../pg_basebackup/receivelog.c')
common_sources += files('../pg_basebackup/streamutil.h')
common_sources += files('../pg_basebackup/streamutil.c')
common_sources += files('../pg_basebackup/walmethods.h')
common_sources += files('../pg_basebackup/walmethods.c')

pg_probackup_deps = [frontend_code, libpq, lz4, zlib, zstd]
pg_probackup_common = static_library('libpg_probackup_common',
  common_sources,
  dependencies: pg_probackup_deps,
  include_directories: include_directories('./src', '../../port'),
  kwargs: internal_lib_args,
)

pg_probackup_sources = files(
  'src/pg_probackup.c',
)

if host_system == 'windows'
  pg_probackup_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_probackup',
    '--FILEDESC', 'pg_probackup - utility to manage backup and recovery of PostgreSQL database clusters',])
endif

pg_probackup = executable('pg_probackup',
  pg_probackup_sources,
  link_with: [pg_probackup_common],
  dependencies: pg_probackup_deps,
  include_directories: include_directories('./src', '../../port'),
  c_args: ['-DFRONTEND'], # needed for xlogreader et al
  kwargs: default_bin_args,
)
bin_targets += pg_probackup

subdir('po', if_found: libintl)
