git clone -b REL_15_STABLE https://github.com/postgres/postgres.git
# Copy ptrack to contrib to build the ptrack extension
# Convert line breaks in the patch file to LF otherwise the patch doesn't apply
git clone -b master --depth 1 https://github.com/postgrespro/ptrack.git
Copy-Item -Path ptrack -Destination postgres\contrib -Recurse
(Get-Content ptrack\patches\REL_15_STABLE-ptrack-core.diff -Raw).Replace("`r`n","`n") | Set-Content ptrack\patches\REL_15_STABLE-ptrack-core.diff -Force -NoNewline
cd postgres
git apply -3 ../ptrack/patches/REL_15_STABLE-ptrack-core.diff

# Pacman packages are installed into MSYS
$env:PATH += ";C:\msys64\usr\bin"

# Build Postgres
cd "$env:GITHUB_WORKSPACE\postgres\src\tools\msvc"
(Get-Content config_default.pl) -Replace "zlib *=>(.*?)(?=,? *#)", "zlib => 'C:\dep\zlib'" | Set-Content config.pl
cmd.exe /s /c "`"C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvarsall.bat`" amd64 && .\build.bat"
If (-Not $?) {exit 1}
# Install Postgres
.\install.bat postgres_install
If (-Not $?) {exit 1}

# Build Pg_probackup
cd "$env:GITHUB_WORKSPACE"
cmd.exe /s /c "`"C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvarsall.bat`" amd64 && perl .\gen_probackup_project.pl `"$env:GITHUB_WORKSPACE\postgres`""

