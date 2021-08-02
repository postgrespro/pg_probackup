%global version           @PKG_VERSION@
%global release           @PKG_RELEASE@
%global hash              @PKG_HASH@
%global pgsql_major       @PG_VERSION@
%global pgsql_full        @PG_FULL_VERSION@
%global edition           @EDITION@
%global edition_full      @EDITION_FULL@
%global prefix            @PREFIX@

#%set_verify_elf_method unresolved=relaxed, rpath=relaxed
%set_verify_elf_method rpath=relaxed,unresolved=relaxed

Name:           pg_probackup-%{edition}-%{pgsql_major}
Version:        %{version}
Release:        %{release}.%{hash}
Summary:        Backup utility for PostgresPro %{edition_full}
Group:          Applications/Databases
License:        BSD
Url:            http://postgrespro.ru/
Source0:        postgrespro-%{edition}-%{pgsql_full}.tar.bz2
Source1:        pg_probackup-%{edition}-%{version}.tar.bz2
BuildRequires:  gcc make perl glibc-devel bison flex
BuildRequires:  readline-devel openssl-devel gettext zlib-devel


%description
Backup tool for PostgresPro %{edition_full}.

%prep
%setup -q -b1 -n postgrespro-%{edition}-%{pgsql_full}

%build
mv %{_builddir}/pg_probackup-%{edition}-%{version} contrib/pg_probackup
%if "%{pgsql_major}" == "9.6"
./configure --enable-debug
%else
./configure --enable-debug --prefix=%{prefix}
%endif
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
