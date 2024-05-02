from enum import Enum


class DateTimePattern(Enum):
    # 2022-12-30 14:07:30+01
    Y_m_d_H_M_S_z_dash = '%Y-%m-%d %H:%M:%S%z'
    Y_m_d_H_M_S_f_z_dash = '%Y-%m-%d %H:%M:%S.%f%z'
