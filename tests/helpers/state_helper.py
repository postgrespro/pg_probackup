import re
from os import path


def get_program_version() -> str:
    """
    Get pg_probackup version from source file /src/pg_probackup.h
    value of PROGRAM_VERSION
    The alternative for file /tests/expected/option_version.out
    """
    probackup_h_path = '../../src/pg_probackup.h'
    probackup_h_full_path = path.join(path.dirname(__file__), probackup_h_path)
    define_sub = "#define PROGRAM_VERSION"
    try:
        with open(probackup_h_full_path, 'r') as probackup_h:
            for line in probackup_h:
                clean_line = re.sub(' +', ' ', line)  # Line without doubled spaces
                if define_sub in clean_line:
                    version = re.findall(r'"([^""]+)"', clean_line)[0]  # Get the value between two quotes
                    return str(version)
        raise Exception(f"Couldn't find the line with `{define_sub}` in file `{probackup_h_full_path}` "
                        f"that contains version between 2 quotes")
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Couldn't get version, check that file `{probackup_h_full_path}` exists and `PROGRAM_VERSION` defined")