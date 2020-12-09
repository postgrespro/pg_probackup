# -*-perl-*- hey - emacs - this is a perl file
# my $currpath = cwd();

our $pgsrc;
our $currpath;

BEGIN {
# path to the pg_probackup dir
$currpath = File::Basename::dirname(Cwd::abs_path($0));
use Cwd;
use File::Basename;
if (($#ARGV+1)==1) 
{
	$pgsrc = shift @ARGV;
	if($pgsrc eq "--help"){
		print STDERR "Usage $0 pg-source-dir \n";
		print STDERR "Like this: \n";
		print STDERR "$0 C:/PgProject/postgresql.10dev/postgrespro \n";
		print STDERR "May be need input this before:  \n";
		print STDERR "CALL \"C:\\Program Files (x86)\\Microsoft Visual Studio 12.0\\VC\\vcvarsall\" amd64\n";
		exit 1;
	}
}
else
{
	use Cwd qw(abs_path);
	my $path = dirname(abs_path($0));
	chdir($path);
	chdir("../..");
	$pgsrc = cwd();
	$currpath = "contrib/pg_probackup";
}
chdir("$pgsrc/src/tools/msvc");
push(@INC, "$pgsrc/src/tools/msvc");
chdir("../../..") if (-d "../msvc" && -d "../../../src");

}
use Win32;
use Carp;
use strict;
use warnings;


use Project;
use Solution;
use File::Copy;
use Config;
use VSObjectFactory;
use List::Util qw(first);

use Exporter;
our (@ISA, @EXPORT_OK);
@ISA       = qw(Exporter);
@EXPORT_OK = qw(Mkvcbuild);

my $solution;
my $libpgport;
my $libpgcommon;
my $libpgfeutils;
my $postgres;
my $libpq;
my @unlink_on_exit;

if (-d "src/fe_utils")
{
	$libpgfeutils = 1;
}
else
{
	$libpgfeutils = 0;
}



use lib "src/tools/msvc";

use Mkvcbuild;

# if (-e "src/tools/msvc/buildenv.pl")
# {
#	do "src/tools/msvc/buildenv.pl";
# }
# elsif (-e "./buildenv.pl")
# {
#	do "./buildenv.pl";
# }

# set up the project
our $config;
do "config_default.pl";
do "config.pl" if (-f "src/tools/msvc/config.pl");

# my $vcver = Mkvcbuild::mkvcbuild($config);
my $vcver = build_pgprobackup($config);

# check what sort of build we are doing

my $bconf     = $ENV{CONFIG}   || "Release";
my $msbflags  = $ENV{MSBFLAGS} || "";
my $buildwhat = $ARGV[1]       || "";

# if (uc($ARGV[0]) eq 'DEBUG')
# {
# 	$bconf = "Debug";
# }
# elsif (uc($ARGV[0]) ne "RELEASE")
# {
# 	$buildwhat = $ARGV[0] || "";
# }

# printf "currpath=$currpath";

# exit(0);
# ... and do it
system("msbuild pg_probackup.vcxproj /verbosity:normal $msbflags /p:Configuration=$bconf" );

# report status

my $status = $? >> 8;
printf("Status: $status\n");
printf("Output file built in the folder $pgsrc/$bconf/pg_probackup\n");

exit $status;



sub build_pgprobackup
{
	our $config = shift;

	chdir('../../..') if (-d '../msvc' && -d '../../../src');
	die 'Must run from root or msvc directory'
	  unless (-d 'src/tools/msvc' && -d 'src');

	# my $vsVersion = DetermineVisualStudioVersion();
	my $vsVersion = '12.00';

	$solution = CreateSolution($vsVersion, $config);

	$libpq = $solution->AddProject('libpq', 'dll', 'interfaces',
		'src/interfaces/libpq');
	if ($libpgfeutils)
	{
		$libpgfeutils = $solution->AddProject('libpgfeutils', 'lib', 'misc');
	}
	$libpgcommon = $solution->AddProject('libpgcommon', 'lib', 'misc');
	$libpgport = $solution->AddProject('libpgport', 'lib', 'misc');

	#vvs test
	my $probackup =
	  $solution->AddProject("pg_probackup", 'exe', "pg_probackup"); #, 'contrib/pg_probackup'
	$probackup->AddDefine('FRONTEND');
	$probackup->AddFiles(
		"$currpath/src", 
		'archive.c',
		'backup.c',
		'catalog.c',
		'configure.c',
		'data.c',
		'delete.c',
		'dir.c',
		'fetch.c',
		'help.c',
		'init.c',
		'merge.c',
		'parsexlog.c',
		'pg_probackup.c',
		'restore.c',
		'show.c',
		'stream.c',
		'util.c',
		'validate.c',
		'checkdb.c',
		'ptrack.c'
		);
	$probackup->AddFiles(
		"$currpath/src/utils",
		'configuration.c',
		'file.c',
		'remote.c',
		'json.c',
		'logger.c',
		'parray.c',
		'pgut.c',
		'thread.c',
		'remote.c'
		);
	$probackup->AddFile("$pgsrc/src/backend/access/transam/xlogreader.c");
	$probackup->AddFile("$pgsrc/src/backend/utils/hash/pg_crc.c");
	$probackup->AddFiles(
		"$pgsrc/src/bin/pg_basebackup", 
		'receivelog.c',
		'streamutil.c'
		);

	if (-e "$pgsrc/src/bin/pg_basebackup/walmethods.c") 
	{
		$probackup->AddFile("$pgsrc/src/bin/pg_basebackup/walmethods.c");
	}

	$probackup->AddFile("$pgsrc/src/bin/pg_rewind/datapagemap.c");

	$probackup->AddFile("$pgsrc/src/interfaces/libpq/pthread-win32.c");
	$probackup->AddFile("$pgsrc/src/timezone/strftime.c");

        $probackup->AddIncludeDir("$pgsrc/src/bin/pg_basebackup");
        $probackup->AddIncludeDir("$pgsrc/src/bin/pg_rewind");
        $probackup->AddIncludeDir("$pgsrc/src/interfaces/libpq");
        $probackup->AddIncludeDir("$pgsrc/src");
        $probackup->AddIncludeDir("$pgsrc/src/port");
        $probackup->AddIncludeDir("$pgsrc/src/include/portability");

        $probackup->AddIncludeDir("$currpath");
        $probackup->AddIncludeDir("$currpath/src");
        $probackup->AddIncludeDir("$currpath/src/utils");

    if ($libpgfeutils)
    {
		$probackup->AddReference($libpq, $libpgfeutils, $libpgcommon, $libpgport);
    }
	else
	{
		$probackup->AddReference($libpq, $libpgcommon, $libpgport);
	}
	$probackup->AddLibrary('ws2_32.lib');

	$probackup->Save();
	return $solution->{vcver};

}
