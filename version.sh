#!/usr/bin/env bash

# Output the program version based on the state of the repository (source code
# and tags).
# Tags must be in the form of
# x.y.z, e.g. 3.1.0
# or
# x.y.z-<prerelease part (alpha, alpha2, beta etc.)>, e.g. 3.1.0-beta2
# If the tag consists only of the version number (i.e. it doesn't contain a
# prerelease part) and this number is equal to the version in the header file
# then the version is considered a release and no additional version data is
# appended to it by default (but can be forced by "-p" and "-r" command line
# arguments). Otherwise, provided Git is available, the prerelease part and Git
# revision are automatically added to the version.

cd `dirname "$0"`

while getopts p:r:sv: opt; do
  case $opt in
    p) prerelease=$OPTARG;;
    r) revision=$OPTARG;;
    s) ID=semver;;
    v) version=$OPTARG;;
  esac
done

if [ -z "$ID" ]; then
  . /etc/os-release
fi
case $ID in
  altlinux | astra | debian | ubuntu)
    # The only scheme that properly sorts metadata and prerelease fields is
    # when the both are specified after a '~'
    presep='~'; metasep='~';;
  centos | opensuse-leap | redos)
    presep='~'; metasep=^;;
  *)  # semver
    presep=-; metasep=+
esac

if [ -z "$version" ]; then
  version=`grep '#define PROGRAM_VERSION' src/pg_probackup.h | cut -f 2 | tr -d '"'`
fi

if which git >/dev/null 2>&1; then
  tag=`git describe --tags 2> /dev/null`
  # Shallow cloned repository may not have tags
  if [ -z "$prerelease" -a "$tag" ]; then
    f1=`cut -d - -f 1 <<< $tag`
    f2=`cut -d - -f 2 <<< $tag`
    # Append the prerelease part only if the tag refers to the current version
    # Assume that the prerelease part contains letters
    if [ $f1 = $version ] && expr "$f2" : "[1-9]*[a-zA-Z]" 1>/dev/null; then
      prerelease="$f2"
    fi
  fi
  if [ -z "$revision" ]; then
    revision=g`git rev-parse --short HEAD`
  fi
fi

out=$version${prerelease:+$presep$prerelease}
if [ "$tag" != $version -a "$revision" ]; then
  out=$out$metasep`date +%Y%m%d`$revision
fi

echo $out
