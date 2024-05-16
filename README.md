# rdma-fabric

# build
```code
$ mkdir build
$ cd build
$ cmake ..
$ make
```
You must install libfabric before building this project.
repo:https://github.com/ofiwg/libfabric

# run
```code
$ sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/ ./client SERVER_IP -S 65536 -o write -w 50 -I 23510 -p verbs
```

another terminal
```code
sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/ ./client -S 65536 -o write -w 50 -I 235106 -p verbs
```