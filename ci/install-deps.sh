#!/usr/bin/env sh

apt -qq update && \
  DEBIAN_FRONTEND=noninteractive apt install -y \
  libc-dev \
  bison \
  flex \
  ccache \
  libreadline-dev \
  zlib1g-dev \
  libzstd-dev \
  libssl-dev \
  perl \
  libperl-dev \
  libdbi-perl \
  cpanminus \
  locales \
  python3 \
  python3-dev \
  python3-pip \
  libicu-dev \
  libgss-dev \
  libkrb5-dev \
  libxml2-dev \
  libxslt1-dev \
  libldap2-dev \
  tcl-dev \
  diffutils \
  gdb \
  gettext \
  lcov \
  openssh-client \
  openssh-server \
  libipc-run-perl \
  libtime-hires-perl \
  libtimedate-perl \
  libdbd-pg-perl

pip3 install testgres

