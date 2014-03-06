zoo-locked
==========

mutually exclusive task execution using ZooKeeper

This small tool will acquire an exclusive lock using ZooKeeper and then execute an arbitrary program/script through `popen`. While the task is running, the lock will be held. Should the tool crash for whatever reason, the ZooKeeper connection will time out which will release the lock. 

The tool is written in C and has no dependencies apart from the `zookeper_mt` lib.

