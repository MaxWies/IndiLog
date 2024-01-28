Ixe
==================================

Ixe is a scalable FaaS runtime for stateful serverless computing with shared logs. It uses distributed indexing to efficiently locate records on the shared logs.

Ixe extends [Boki](https://github.com/ut-osa/boki). Boki exports the shared log API to serverless functions, allowing them to manage states with strong consistency, durability, and fault tolerance.
Ixe and Boki use [Nightcore](https://github.com/ut-osa/nightcore) as the runtime for serverless functions.

Ixe's name originates as infix anagram from one of its main design characteristics: distributed ind**exi**ng

### Building Ixe ###

Under Ubuntu 20.04, building Ixe needs following dependencies installed:
~~~
sudo apt install g++ make cmake pkg-config autoconf automake libtool curl unzip
~~~

Once installed, build Ixe with:

~~~
./build_deps.sh
make -j $(nproc)
~~~

### Kernel requirements ###

Ixe uses like Boki [io_uring](https://en.wikipedia.org/wiki/Io_uring) for asynchronous I/Os.

Ixe requires Linux kernel 5.10 or later to run.

### Ixe support libraries ###

Ixe supports Boki's libraries for transactional workflows and durable object storage.

### System evaluation ###

A separate repository [Ixe-Benchmarks](https://github.com/MaxWies/ixe-benchmarks) includes scripts and detailed instructions on running evaluation workloads for Ixe and Boki.

### Limitations of the current prototype ###

The shared log API is only exported to functions written in Go.
