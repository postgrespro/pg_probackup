%global version           @PKG_VERSION@
%global release           @PKG_RELEASE@

Summary:	pg_probackup repo RPM
Name:		pg_probackup-repo-forks
Version:	%{version}
Release:	%{release}
Group:		Applications/Databases
License:	BSD
Url:		http://postgrespro.ru/

Source0:    http://repo.postgrespro.ru/pg_probackup-forks/keys/GPG-KEY-PG_PROBACKUP
Source1:    pg_probackup-forks.repo

BuildArch:     noarch

%description
This package contains yum configuration for @SHORT_CODENAME@, and also the GPG key
for pg_probackup RPMs for PostgresPro Standard and Enterprise.

%prep
%setup -q  -c -T
install -pm 644 %{SOURCE0} .
install -pm 644 %{SOURCE1} .

%build

%install
rm -rf $RPM_BUILD_ROOT

#GPG Key
install -Dpm 644 %{SOURCE0} \
    $RPM_BUILD_ROOT%{_sysconfdir}/pki/rpm-gpg/GPG-KEY-PG_PROBACKUP

# yum
install -dm 755 $RPM_BUILD_ROOT%{_sysconfdir}/yum.repos.d
install -pm 644 %{SOURCE1} $RPM_BUILD_ROOT%{_sysconfdir}/yum.repos.d

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%config(noreplace) /etc/yum.repos.d/*
/etc/pki/rpm-gpg/*

%changelog
* Fri Oct 26 2019 Grigory Smolkin <g.smolkin@postgrespro.ru>
- Initial package
