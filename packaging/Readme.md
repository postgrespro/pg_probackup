```
export PBK_VERSION=2.4.17
export PBK_HASH=57f871accce2604
export PBK_RELEASE=1
export PBK_EDITION=std
make pkg
```

To build binaries for PostgresPro Standart or Enterprise, a pgpro.tar.bz2 with latest git tree must be preset in `packaging/tarballs` directory:
```
cd packaging/tarballs
git clone pgpro_repo pgpro
tar -cjSf pgpro.tar.bz2 pgpro
```

To build repo the gpg keys for package signing must be present ...
Repo must be build with 1 thread:
```
make repo -j1
```
