ZOOKEEPER_PATH=/Users/fxtentacle/Downloads/zookeeper-3.4.5/src/recipes/lock/src/c/../../../../../src/c
gcc -I/opt/local/include -I${ZOOKEEPER_PATH}/include -I${ZOOKEEPER_PATH}/generated  -L/opt/local/lib -lzookeeper_mt main.c
