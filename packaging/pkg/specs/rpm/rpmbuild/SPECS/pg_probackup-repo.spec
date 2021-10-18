%global version           @PKG_VERSION@
%global release           @PKG_RELEASE@

Summary:	PG_PROBACKUP RPMs
Name:		pg_probackup-repo
Version:	%{version}
Release:	%{release}
Group:		Applications/Databases
License:	BSD
Url:		http://postgrespro.ru/

Source0:    http://repo.postgrespro.ru/pg_probackup/keys/GPG-KEY-PG_PROBACKUP
Source1:    pg_probackup.repo

BuildArch:	noarch

%description
This package contains yum configuration for Centos, and also the GPG key for PG_PROBACKUP RPMs.

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

# yum /etc/zypp/repos.d/repo-update.repo

%if 0%{?suse_version}
	install -dm 755 $RPM_BUILD_ROOT%{_sysconfdir}/zypp/repos.d
	install -pm 644 %{SOURCE1} $RPM_BUILD_ROOT%{_sysconfdir}/zypp/repos.d
%else
	install -dm 755 $RPM_BUILD_ROOT%{_sysconfdir}/yum.repos.d
	install -pm 644 %{SOURCE1} $RPM_BUILD_ROOT%{_sysconfdir}/yum.repos.d
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%if 0%{?suse_version}
	%config(noreplace) /etc/zypp/repos.d/*
%else
	%config(noreplace) /etc/yum.repos.d/*
%endif
/etc/pki/rpm-gpg/*

%changelog
* Mon Jun 29 2020 Grigory Smolkin <g.smolkin@postgrespro.ru>
- release update
