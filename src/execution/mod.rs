/*
    TODO: Create a very abstract and general API for different execution engines. We can then
          implement different execution engines for the fun of it. I will probably start with
          a very traditional Volcano-style pull-based tuple-at-a-time pure interpreted engine.
          Then I would like to implement a modern push-based data-centric compiling engine likely
          using Cranelift and maybe also LLVM to get a kind of adaptive compilation. Maybe I will
          write a fork of the Volcano-style engine using a push based model first.
 */

pub mod engine;