# SPEC file for pg_rman
# Copyright(C) 2009-2012 NIPPON TELEGRAPH AND TELEPHONE CORPORATION

%define _pgdir   /usr/pgsql-9.2
%define _bindir  %{_pgdir}/bin
%define _libdir  %{_pgdir}/lib
%define _datadir %{_pgdir}/share

## Set general information for pg_rman.
Summary:    Backup and Recovery Tool for PostgreSQL
Name:       pg_rman
Version:    1.2.5
Release:    1%{?dist}
License:    BSD
Group:      Applications/Databases
Source0:    %{name}-%{version}.tar.gz
URL:        http://code.google.com/p/pg-rman/
BuildRoot:  %{_tmppath}/%{name}-%{version}-%{release}-%(%{__id_u} -n)
Vendor:	    NIPPON TELEGRAPH AND TELEPHONE CORPORATION

## We use postgresql-devel package
BuildRequires:  postgresql92-devel
Requires:  postgresql92-libs

## Description for "pg_rman"
%description
pg_rman manages backup and recovery of PostgreSQL.
pg_rman has the features below:
-Takes a backup while database including tablespaces with just one command. 
-Can recovery from backup with just one command. 
-Supports incremental backup and compression of backup files so that it takes less disk spaces. 
-Manages backup generations and shows a catalog of the backups. 


## pre work for build pg_rman
%prep
%setup -q -n %{name}-%{version}

## Set variables for build environment
%build
USE_PGXS=1 make %{?_smp_mflags}

## Set variables for install
%install
rm -rf %{buildroot}

USE_PGXS=1 DESTDIR=%{buildroot} make %{?_smp_mflags} install

install -d %{buildroot}%{_bindir}
install -m 755 pg_rman %{buildroot}%{_bindir}/pg_rman

%clean
rm -rf %{buildroot}

%files
%defattr(755,root,root)
%{_bindir}/pg_rman

# History of pg_rman.
%changelog
* Wed Nov 10  2010 - NTT OSS Center <tomonari.katsumata@oss.ntt.co.jp> 1.2.0-1
* Wed Dec 9  2009 - NTT OSS Center <itagaki.takahiro@oss.ntt.co.jp> 1.1.1-1
- Initial cut for 1.1.1

