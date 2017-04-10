# Karps-Haskell - Haskell bindings for Spark Datasets and Dataframes

This project is an exploration vehicle for developing safe, robust and reliable
data pipelines over Apache Spark. It consists in multiple sub-projects:
 - a specification to describe data pipelines in a language-agnostic manner,
   and a communication protocol to submit these pipelines to Spark. The
   specification is currently specified in [this repository](https://github.com/krapsh/karps-interface), using
    [Protocol Buffers 3](https://developers.google.com/protocol-buffers/docs/proto3) (
     which is also compatible with JSON).
 - a serving library, called
   [karps-server](https://github.com/krapsh/kraps-server), that implements this specification on top of Spark.
   It is written in Scala and is loaded as a standard Spark package.
 - a client written in Haskell that sends pipelines to Spark for execution. In
   addition, this client serves as an experimental platform for whole-program optimization and verification, as well as compiler-enforced type checking.

There is also a separate set of utilities to visualize such pipelines using
Jupyter notebooks and IHaskell.

This is a preview, the API may (will) change in the future.

The name is a play on a tasty fish of the family Cyprinidae, and an anagram of Spark. The programming model is strongly influenced by the
[TensorFlow project](https://www.tensorflow.org/) and follows a similar design.

## Introduction

This project explores an alternative API to run complex workflows on top of
Apache Spark. Note that it is neither endorsed or supported by the Apache
Foundation nor by Databricks, Inc.

For the developers of Spark bindings:

By using this API, rich transforms can be expressed on top of Spark using various programming languages, without having to implement elaborate socket protocols to communicate with Java objects. Each programming language only needs to implement a relatively small interface that does not rely on features specific to the Java virtual machines, and which can be implemented using standard REST technologies. Each language is then free to express computations in the most idiomatic way.

As a reference, a set of bindings is being developed in the Haskell programming language and is used as the reference implementation. Despite its limited usage in data science, it is a useful tool to design strongly principled APIs that work across various programming languages.


For the user:

The user may be interested by a few features unique to this interface.

1. Lazy computations.
No call to Spark is issued until a result is absolutely required. Unlike standard Spark interfaces, even aggregation operations such as `collect()` or `sum()` are lazy. This allows Karps to perform whole-program analysis of the computation and to make optimizations that are currently beyond the reach of Spark.

2. Strong checks.
Thanks to lazy evaluation, a complete data science pipeline can be checked for correctness before it is evaluated. This is useful when composing multiple notebooks or commands together. For example, a lot of interesting operations in Spark such as Machine Learning algorithms involve an aggregation step. In Spark, such a step would break the analysis of the program and prevent the Spark analyzer from checking further transforms. Karps does not suffer from such limitations and checks all the pipeline at once.

3. Automatic resource management.
Because Karps has a global view of the pipeline, it can check when data needs to be cached or uncached. It is able to schedule caching and uncaching operations automatically, and it refuses to run program that may be incorrect with respect to caching (for example when uncaching happens before the data is accessed again)

4. Complex nested pipelines.
Computations can be arbitrarily nested and composed, allowing to conceptually condense complex sequences of operations into a single high-level operations. This is useful for debugging and understanding a pipeline at a high-level without being distracted by the implementation details of each step. This follows the same approach as TensorFlow.

5. Stable format and language agnostic.
A complex pipeline may be stored as a JSON file in one programming language and read/restored in a different programming language. If this program is run on the same session, that other language can access the cached data.

## Installation

You need the stack tool to build and install Karps. Additionally, you will need the Karps server running to run some queries against Spark.


## Development ideas

These are notes for developers interested in understanding the general philosophy behind Karps.

Doing data science at scale is hard and requires multiple iterations. Accessing the data can be a long operations (minutes, or hours), even in Spark. Because of that, data should only be accessed if we are reasonably confident that the program is going to finish.

As a result, Karps is designed to detect a number of common programming mistakes in Spark before even attempting to access the data. These checks are either enforced by the runtime, or by Haskell's compiler when using the typed API. This is possible thanks to whole program analysis and lazy evaluation.

The execution model is heavily inspired by TensorFlow:

1. Deterministic operations:
All the operations are deterministic, and some non-deterministic operations such as `currentTime` in SQL are forbidden. While this may seem a restriction, it provides multiple benefits such as aggressive caching and computation reuse.

2. Stateless operations on a graph. A lot of the transforms operate as simple graph transforms and will eventually be merged in the server, making them available to all languages.

3. Simple JSON-based format for encoding transform. It may be switched to Protocol Buffers (v3) if I figure out how to use it with Haskell and Spray-Can.

4. Separation of the description of the transform from the operational details required by Spark.

5. Trying to use the type system in the most effective way (Haskell interface). Making sure that the type system can enforce some simple rules such as not adding an integer to a string, but at the same time trying to give understandable error messages to the user when something goes wrong.


## Differences from Spark

As mentioned Karps attempts to provide strong semantic guarantees at the expense of some flexibility.

In order to guarantee determinism, some operations are/will be forbidden:
 - randn() and rand() without a seed
 - get_current_time

Also, all the operations are meant to be expressed in a way that does not depend on the partitioning of the data. This is a significant departure from Spark, in which the PartitionID is available. It is possible in most cases to replace these at the expense of extra shuffles. In this case, it is considered worth it because of the strong guarantees that it offers with respect to reproducibility. In any case, it is a matter of debate.

## Current status

Most of the underlying engines is working. Some important pieces are
still incomplete or missing though:
  - simple json ingest with data specification
  - debug mode for the backend (check for correctness)
  - autogeneration of accessors with template Haskell
  - better IHaskell interface (especially for reporting errors)
  - python frontend
  - pandas backend

Advanced feature that require more thoughts are considered after that:
  - SQL commands (interned strings)
  - user-defined functions (Scala only)
  - simple ML pipelines
  - meta data and user-defined types
