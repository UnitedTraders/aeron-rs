# Aeron client written in Rust

[![Crates.io](https://img.shields.io/crates/v/aeron-rs)](https://crates.io/crates/aeron-rs)
[![minimum rustc version](https://img.shields.io/badge/rustc-1.39+-green.svg)](https://blog.rust-lang.org/2019/11/07/Rust-1.39.0.html)
[![Build Status](https://travis-ci.org/UnitedTraders/aeron-rs.svg?branch=master)](https://travis-ci.org/UnitedTraders/aeron-rs)

## About *aeron-rs*
Aeron is efficient reliable UDP and IPC message transport. Originally it was developed by RealLogic 
and hosted on GitHub [real-logic/aeron](https://github.com/real-logic/aeron)

Aeron has two main components: 
* Media driver
* Client library

The client library is linked with applications and allows several applications to talk with each
other through Media driver(s). For more information about how Aeron works please read 
[documentation](https://github.com/real-logic/aeron/wiki).

*aeron-rs* library implements client functionality to work with Aeron Media driver. To get functioning system
one need to download (and compile) Media driver from [real-logic/aeron](https://github.com/real-logic/aeron) and write
some Aeron enabled application using *aeron-rs*. 
Examples of such applications could be found inside the [bin](https://github.com/UnitedTraders/aeron-rs/tree/master/src/bin) and
in the library integration [tests](https://github.com/UnitedTraders/aeron-rs/tree/master/tests).

## Why Aeron?
Aeron could be a good choice if you need to organize fast and ordered message streams. There are number
of widely used messaging frameworks and to choose the right one for your particular case it is better to test 
them if close to real-life situations and compare the performance. 
Just some hints why Aeron could be a good choice:
* Supports various transmission media without the need to change application code when switching between the media. 
Application can use IPC (shared memory) today and could switch to UDP in a matter of config change and restart.
* Can work over UDP which is generally faster than over TCP (as some other messaging systems do)
* Provides reliable and ordered message flow (even over UPD). Not all fast messaging frameworks guaranty messages order but Aeron do

## Running library tests
Integration tests for *aeron-rs* assume that Media driver executable (aeronmd) is present in the PATH. So prior
to run these tests install *aeronmd* accordingly.
Also integration tests designed to run sequentially one by one. Therefore use 
```
cargo test -- --test-threads=1
```
command to run them.

## Tips for contributors

If you use POSIX-compliant OS (or at least has `/bin/sh`), you can use
predefined git [pre-push](.pre-push.sh) hook to ensure coding standards are met.

```
ln -s ../../.pre-push.sh .git/hooks/pre-push
```

Also, if you use IDE from IntelliJ IDEA family with Rust plugin, you can enable autoformatting:

`Settings -> Languages and Frameworks -> Rust -> Rustfmt -> Run rustfmt on Save`