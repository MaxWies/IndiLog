IndiLog
==================================

IndiLog is a scalable FaaS runtime for stateful serverless computing with shared logs. It uses distributed indexing to efficiently locate records on the shared logs.

IndiLog extends [Boki](https://github.com/ut-osa/boki). Boki exports the shared log API to serverless functions, allowing them to manage states with strong consistency, durability, and fault tolerance.
Indilog and Boki use [Nightcore](https://github.com/ut-osa/nightcore) as the runtime for serverless functions.

IndiLog's name originates from one of its main design characteristics: **di**stributed **in**dexing

### Building IndiLog ###

Under Ubuntu 20.04, building IndiLog needs following dependencies installed:
~~~
sudo apt install g++ make cmake pkg-config autoconf automake libtool curl unzip
~~~

Once installed, build IndiLog with:

~~~
./build_deps.sh
make -j $(nproc)
~~~

### Kernel requirements ###

IndiLog uses like Boki [io_uring](https://en.wikipedia.org/wiki/Io_uring) for asynchronous I/Os.

IndiLog requires Linux kernel 5.10 or later to run.

### IndiLog support libraries ###

IndiLog supports Boki's libraries for transactional workflows and durable object storage.

### System evaluation ###

A separate repository [IndiLog-Benchmarks](https://github.com/MaxWies/indilog-benchmarks) includes scripts and detailed instructions on running evaluation workloads for IndiLog and Boki.

### Limitations of the current prototype ###

The shared log API is only exported to functions written in Go.
