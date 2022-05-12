# Grant the Github runner user full control of the workspace for initdb to successfully process the data folder
icacls.exe "$env:GITHUB_WORKSPACE" /grant "${env:USERNAME}:(OI)(CI)F"
$env:PATH += ";$env:GITHUB_WORKSPACE\postgres\postgres_install\lib;C:\dep\zlib\lib"
$Env:LC_MESSAGES = "English"
$Env:PG_CONFIG = "$env:GITHUB_WORKSPACE\postgres\postgres_install\bin\pg_config.exe"
$Env:PGPROBACKUPBIN = "$env:GITHUB_WORKSPACE\postgres\Release\pg_probackup\pg_probackup.exe"
$Env:PG_PROBACKUP_PTRACK = "ON"
If (!$Env:MODE -Or $Env:MODE -Eq "basic") {
  $Env:PG_PROBACKUP_TEST_BASIC = "ON"
  python -m unittest -v tests
} else {
  python -m unittest -v tests.$Env:MODE
}

