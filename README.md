# cpp-driver

Ceci est une première tentative pour utiliser une base NoSQL *cassandra* (version 3) avec le langage *Swift* (version 4) d'*Apple*
en utilisant *cpp-driver* de *Datastax*.

## Installation du driver 'Datastax'

    git clone https://github.com/datastax/cpp-driver.git
    cd cpp-driver
    mkdir build
    cd build
    cmake .. -G"Unix Makefiles"
    make
    sudo make install

