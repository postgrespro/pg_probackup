# -*-perl-*- hey - emacs - this is a perl file
BEGIN{
use Cwd;
use File::Basename;

my $pgsrc="";
if (@ARGV==1) 
{
	$pgsrc = shift @ARGV;
	if($pgsrc == "--help"){
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
if (uc($ARGV[0]) eq 'DEBUG')
{
	$bconf = "Debug";
}
elsif (uc($ARGV[0]) ne "RELEASE")
{
	$buildwhat = $ARGV[0] || "";
}

# ... and do it
system("msbuild pg_probackup.vcxproj /verbosity:normal $msbflags /p:Configuration=$bconf" );


# report status

my $status = $? >> 8;

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
	$libpgfeutils = $solution->AddProject('libpgfeutils', 'lib', 'misc');
	$libpgcommon = $solution->AddProject('libpgcommon', 'lib', 'misc');
	$libpgport = $solution->AddProject('libpgport', 'lib', 'misc');

	#vvs test
	my $probackup =
	  $solution->AddProject('pg_probackup', 'exe', 'pg_probackup'); #, 'contrib/pg_probackup'
	$probackup->AddFiles(
		'contrib/pg_probackup/src', 
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
		'parsexlog.c',
		'pg_probackup.c',
		'restore.c',
		'show.c',
		'status.c',
		'util.c',
		'validate.c'
		);
	$probackup->AddFiles(
		'contrib/pg_probackup/src/utils', 
		'json.c',
		'logger.c',
		'parray.c',
		'pgut.c',
		'thread.c'
		);
	$probackup->AddFile('src/backend/access/transam/xlogreader.c');
	$probackup->AddFiles(
		'src/bin/pg_basebackup', 
		'receivelog.c',
		'streamutil.c'
		);

	if (-e 'src/bin/pg_basebackup/walmethods.c') 
	{
		$probackup->AddFile('src/bin/pg_basebackup/walmethods.c');
	}

	$probackup->AddFile('src/bin/pg_rewind/datapagemap.c');

	$probackup->AddFile('src/interfaces/libpq/pthread-win32.c');

        $probackup->AddIncludeDir('src/bin/pg_basebackup');
        $probackup->AddIncludeDir('src/bin/pg_rewind');
        $probackup->AddIncludeDir('src/interfaces/libpq');
        $probackup->AddIncludeDir('src');
        $probackup->AddIncludeDir('src/port');

        $probackup->AddIncludeDir('contrib/pg_probackup');
        $probackup->AddIncludeDir('contrib/pg_probackup/src');
        $probackup->AddIncludeDir('contrib/pg_probackup/src/utils');

	$probackup->AddReference($libpq, $libpgfeutils, $libpgcommon, $libpgport);
	$probackup->AddLibrary('ws2_32.lib');

	$probackup->Save();
	return $solution->{vcver};

}
