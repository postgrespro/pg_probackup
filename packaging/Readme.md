Example:
```
export PBK_VERSION=2.4.17
export PBK_HASH=57f871accce2604
export PBK_RELEASE=1
export PBK_EDITION=std|ent
make pkg
```

To build binaries for PostgresPro Standart or Enterprise, a pgpro.tar.bz2 with latest git tree must be preset in `packaging/pkg/tarballs` directory:
```
cd packaging/pkg/tarballs
git clone pgpro_repo pgpro
tar -cjSf pgpro.tar.bz2 pgpro
```

To build repo the gpg keys for package signing must be present ...
Repo must be build using 1 thread (due to debian bullshit):
```
make repo -j1
```
