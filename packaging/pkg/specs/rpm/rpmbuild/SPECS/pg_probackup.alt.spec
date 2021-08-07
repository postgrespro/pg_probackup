%global version           @PKG_VERSION@
%global release           @PKG_RELEASE@
%global hash              @PKG_HASH@
%global pgsql_major       @PG_VERSION@
%global pgsql_full        @PG_FULL_VERSION@
%set_verify_elf_method rpath=relaxed

Name:           pg_probackup-%{pgsql_major}
Version:        %{version}
Release:        %{release}.%{hash}
Summary:        Backup utility for PostgreSQL
Group:          Applications/Databases
License:        BSD
Url:            http://postgrespro.ru/
Source0:        http://ftp.postgresql.org/pub/source/v%{pgsql_full}/postgresql-%{pgsql_major}.tar.bz2
Source1:        pg_probackup-%{version}.tar.bz2
BuildRequires:  gcc make perl glibc-devel bison flex
BuildRequires:  readline-devel openssl-devel gettext zlib-devel


%description
Backup tool for PostgreSQL.

%prep
%setup -q -b1 -n postgresql-%{pgsql_full}

%build
mv %{_builddir}/pg_probackup-%{version} contrib/pg_probackup
./configure --enable-debug --without-readline
make -C 'src/common'
make -C 'src/port'
make -C 'src/interfaces'
cd contrib/pg_probackup && make

%install
%{__mkdir} -p %{buildroot}%{_bindir}
%{__install} -p -m 755 contrib/pg_probackup/pg_probackup %{buildroot}%{_bindir}/%{name}

%files
%{_bindir}/%{name}

%clean
rm -rf $RPM_BUILD_ROOT


%changelog
* Mon Nov 17 2019 Grigory Smolkin <g.smolkin@postgrespro.ru> - 2.2.6-1
- Initial release.
