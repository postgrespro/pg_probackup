import json
from datetime import datetime
from unittest import TestCase

from ..enums.date_time_enum import DateTimePattern


class ShowJsonResultValidator(TestCase):
    """
    This class contains all fields from show command result in json format.
    It used for more convenient way to set up and validate output results.

    If we want to check the field we should set up it using the appropriate set method
    For ex:
        my_validator = ShowJsonResultValidator().set_backup_mode("PAGE")\
                                                .set_status("OK")

    After that we can compare json result from self.pb.show command with this class using `check_show_json` method.

    For informative error output, the validator class is inherited from TestClass. It allows us to use assertEqual
    and do not worry about the readability of the error result.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.backup_id = None
        self.parent_backup_id = None
        self.backup_mode = None
        self.wal = None
        self.compress_alg = None
        self.compress_level = None
        self.from_replica = None
        self.block_size = None
        self.xlog_block_size = None
        self.checksum_version = None
        self.program_version = None
        self.server_version = None
        self.current_tli = None
        self.parent_tli = None
        self.start_lsn = None
        self.stop_lsn = None
        self.start_time = None
        self.end_time = None
        self.end_validation_time = None
        self.recovery_xid = None
        self.recovery_time = None
        self.data_bytes = None
        self.wal_bytes = None
        self.uncompressed_bytes = None
        self.pgdata_bytes = None
        self.primary_conninfo = None
        self.status = None
        self.content_crc = None

    def check_show_json(self, show_result: json):
        # Check equality if the value was set
        if self.backup_id:
            self.assertEqual(show_result["id"], self.backup_id)
        if self.parent_backup_id:
            self.assertEqual(show_result["parent-backup-id"], self.parent_backup_id)
        if self.backup_mode:
            self.assertEqual(show_result["backup-mode"], self.backup_mode)
        if self.wal:
            self.assertEqual(show_result["wal"], self.wal)
        if self.compress_alg:
            self.assertEqual(show_result["compress-alg"], self.compress_alg)
        if self.compress_level:
            self.assertEqual(show_result["compress-level"], self.compress_level)
        if self.from_replica:
            self.assertEqual(show_result["from-replica"], self.from_replica)
        if self.block_size:
            self.assertEqual(show_result["block-size"], self.block_size)
        if self.xlog_block_size:
            self.assertEqual(show_result["xlog-block-size"], self.xlog_block_size)
        if self.checksum_version:
            self.assertEqual(show_result["checksum-version"], self.checksum_version)
        if self.program_version:
            self.assertEqual(show_result["program-version"], self.program_version)
        if self.server_version:
            self.assertEqual(int(show_result["server-version"]), int(self.server_version))
        if self.current_tli:
            self.assertEqual(show_result["current-tli"], self.current_tli)
        if self.parent_tli:
            self.assertEqual(show_result["parent-tli"], self.parent_tli)
        if self.start_lsn:
            self.assertEqual(show_result["start-lsn"], self.start_lsn)
        if self.stop_lsn:
            self.assertEqual(show_result["stop-lsn"], self.stop_lsn)
        if self.start_time:
            self.assertEqual(show_result["start-time"], self.start_time)
        if self.end_time:
            self.assertEqual(show_result["end-time"], self.end_time)
        if self.end_validation_time:
            self.assertEqual(show_result["end-validation-time"], self.end_validation_time)
        if self.recovery_xid:
            self.assertEqual(show_result["recovery-xid"], self.recovery_xid)
        if self.recovery_time:
            self.assertEqual(show_result["recovery-time"], self.recovery_time)
        if self.data_bytes:
            self.assertEqual(show_result["data-bytes"], self.data_bytes)
        if self.wal_bytes:
            self.assertEqual(show_result["wal-bytes"], self.wal_bytes)
        if self.uncompressed_bytes:
            self.assertEqual(show_result["uncompressed-bytes"], self.uncompressed_bytes)
        if self.pgdata_bytes:
            self.assertEqual(show_result["pgdata-bytes"], self.pgdata_bytes)
        if self.primary_conninfo:
            self.assertEqual(show_result["primary-conninfo"], self.primary_conninfo)
        if self.status:
            self.assertEqual(show_result["status"], self.status)
        if self.content_crc:
            self.assertEqual(show_result["content-crc"], self.content_crc)

        # Sanity checks

        start_time = self.str_time_to_datetime(show_result["start-time"])
        end_time = self.str_time_to_datetime(show_result["end-time"])
        end_validation_time = self.str_time_to_datetime(show_result["end-validation-time"])
        self.assertLessEqual(start_time, end_time)
        self.assertLessEqual(end_time, end_validation_time)

        recovery_time = datetime.strptime(show_result["recovery-time"] + '00', DateTimePattern.Y_m_d_H_M_S_f_z_dash.value)
        self.assertLessEqual(start_time, recovery_time)

        data_bytes = show_result["data-bytes"]
        self.assertTrue(data_bytes > 0)

        wal_bytes = show_result["wal-bytes"]
        self.assertTrue(wal_bytes > 0)

        pgdata_bytes = show_result["pgdata-bytes"]
        self.assertTrue(pgdata_bytes > 0)

    @staticmethod
    def str_time_to_datetime(time: str):
        """
        Convert string time from pg_probackup to datetime format
        String '00' was added because '%z' works with 4 digits values (like +0100), but from pg_probackup we get only
        2 digits timezone value (like +01). Because of that we should add additional '00' in the end
        """
        return datetime.strptime(time + '00', str(DateTimePattern.Y_m_d_H_M_S_z_dash.value))
