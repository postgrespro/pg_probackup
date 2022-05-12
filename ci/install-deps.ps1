$env:PATH += ";C:\msys64\usr\bin"
pacman -S --noconfirm --needed bison flex

git clone -b v1.2.11 --depth 1 https://github.com/madler/zlib.git
cd zlib
cmake -DCMAKE_INSTALL_PREFIX:PATH=C:\dep\zlib -G "Visual Studio 17 2022" .
cmake --build . --config Release --target ALL_BUILD
cmake --build . --config Release --target INSTALL
Copy-Item "C:\dep\zlib\lib\zlibstatic.lib" -Destination "C:\dep\zlib\lib\zdll.lib"
Copy-Item "C:\dep\zlib\bin\zlib.dll" -Destination "C:\dep\zlib\lib"
If (-Not $?) {exit 1}

#Install Testgres
cd "$env:GITHUB_WORKSPACE"
git clone -b no-port-for --single-branch --depth 1 https://github.com/postgrespro/testgres.git
cd testgres
python setup.py install

