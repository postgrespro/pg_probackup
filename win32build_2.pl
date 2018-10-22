#!/usr/bin/perl
use JSON;
our $repack_version;
our $pgdir;
our $pgsrc;
if (@ARGV!=2) {
	print STDERR "Usage $0 postgress-instalation-root pg-source-dir \n";
	exit 1;
}


our $liblist="";


$pgdir = shift @ARGV;
$pgsrc = shift @ARGV if @ARGV;


our $arch = $ENV{'ARCH'} || "x64";
$arch='Win32' if ($arch eq 'x86' || $arch eq 'X86');
$arch='x64' if $arch  eq 'X64';

$conffile = $pgsrc."/tools/msvc/config.pl";


die 'Could not find config.pl'
  unless (-f $conffile);

our $config;
do $conffile;


if (! -d "$pgdir/bin" || !-d "$pgdir/include" || !-d "$pgdir/lib") {
	print STDERR "Directory $pgdir doesn't look like root of postgresql installation\n";
	exit 1;
}
our $includepath="";
our $libpath="";
AddProject();

print "\n\n";
print $libpath."\n";
print $includepath."\n";

# open F,"<","META.json" or die "Cannot open META.json: $!\n";
# {
# 	local $/ = undef;
# 	$decoded = decode_json(<F>);
# 	$repack_version= $decoded->{'version'};
# }
	
# substitute new path in the project files



preprocess_project("./msvs/template.pg_probackup_2.vcxproj","./msvs/pg_probackup.vcxproj");

exit 0;


sub preprocess_project {
	my $in = shift;
	my $out = shift;
	our $pgdir;
	our $adddir;
	my $libs;
	if (defined $adddir) {
		$libs ="$adddir;";
	} else{
		$libs ="";
	}
	open IN,"<",$in or die "Cannot open $in: $!\n";
	open OUT,">",$out or die "Cannot open $out: $!\n";

# $includepath .= ";";
# $libpath .= ";";

	while (<IN>) {
		s/\@PGROOT\@/$pgdir/g;
		s/\@ADDLIBS\@/$libpath/g;
		s/\@PGSRC\@/$pgsrc/g;
		s/\@ADDINCLUDE\@/$includepath/g;


		print OUT $_;
	}
	close IN;
	close OUT;

}



#  my sub 
sub AddLibrary
{
	$inc = shift;
	if ($libpath ne '')
	{
		$libpath .= ';';
	}
	$libpath .= $inc;

}
sub AddIncludeDir
{
	# my ($self, $inc) = @_;
	$inc = shift;
	if ($includepath ne '')
	{
		$includepath .= ';';
	}
	$includepath .= $inc;

}
                                
sub AddProject
{
	# my ($self, $name, $type, $folder, $initialdir) = @_;

	if ($config->{zlib})
	{
		AddIncludeDir($config->{zlib} . '\include');
		AddLibrary($config->{zlib} . '\lib\zdll.lib');
	}
	if ($config->{openssl})
	{
		AddIncludeDir($config->{openssl} . '\include');
		if (-e "$config->{openssl}/lib/VC/ssleay32MD.lib")
		{
			AddLibrary(
				$config->{openssl} . '\lib\VC\ssleay32.lib', 1);
			AddLibrary(
				$config->{openssl} . '\lib\VC\libeay32.lib', 1);
		}
		else
		{
			# We don't expect the config-specific library to be here,
			# so don't ask for it in last parameter
			AddLibrary(
				$config->{openssl} . '\lib\ssleay32.lib', 0);
			AddLibrary(
				$config->{openssl} . '\lib\libeay32.lib', 0);
		}
	}
	if ($config->{nls})
	{
		AddIncludeDir($config->{nls} . '\include');
		AddLibrary($config->{nls} . '\lib\libintl.lib');
	}
	if ($config->{gss})
	{
		AddIncludeDir($config->{gss} . '\inc\krb5');
		AddLibrary($config->{gss} . '\lib\i386\krb5_32.lib');
		AddLibrary($config->{gss} . '\lib\i386\comerr32.lib');
		AddLibrary($config->{gss} . '\lib\i386\gssapi32.lib');
	}
	if ($config->{iconv})
	{
		AddIncludeDir($config->{iconv} . '\include');
		AddLibrary($config->{iconv} . '\lib\iconv.lib');
	}
	if ($config->{icu})
	{
		AddIncludeDir($config->{icu} . '\include');
		if ($arch eq 'Win32')
		{
			AddLibrary($config->{icu} . '\lib\icuin.lib');
			AddLibrary($config->{icu} . '\lib\icuuc.lib');
			AddLibrary($config->{icu} . '\lib\icudt.lib');
		}
		else
		{
			AddLibrary($config->{icu} . '\lib64\icuin.lib');
			AddLibrary($config->{icu} . '\lib64\icuuc.lib');
			AddLibrary($config->{icu} . '\lib64\icudt.lib');
		}
	}
	if ($config->{xml})
	{
		AddIncludeDir($config->{xml} . '\include');
		AddIncludeDir($config->{xml} . '\include\libxml2');
		AddLibrary($config->{xml} . '\lib\libxml2.lib');
	}
	if ($config->{xslt})
	{
		AddIncludeDir($config->{xslt} . '\include');
		AddLibrary($config->{xslt} . '\lib\libxslt.lib');
	}
	if ($config->{libedit})
	{
		AddIncludeDir($config->{libedit} . '\include');
		AddLibrary($config->{libedit} . "\\" .
			($arch eq 'x64'? 'lib64': 'lib32').'\edit.lib');
	}
	if ($config->{uuid})
	{
		AddIncludeDir($config->{uuid} . '\include');
		AddLibrary($config->{uuid} . '\lib\uuid.lib');
	}
	if ($config->{libedit})
	{
		AddIncludeDir($config->{libedit} . '\include');
		AddLibrary($config->{libedit} . "\\" .
			($arch eq 'x64'? 'lib64': 'lib32').'\edit.lib');
	}
	if ($config->{zstd})
	{
		AddIncludeDir($config->{zstd});
		AddLibrary($config->{zstd}. "\\".
			($arch eq 'x64'? "zstdlib_x64.lib" : "zstdlib_x86.lib")
	    );
	}
	# return $proj;
}




