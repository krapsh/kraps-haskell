# Kraps-H - Haskell bindings for Spark Datasets and Dataframes

This project is an exploration vehicle for developing safe, robust and reliable
data pipelines over Apache Spark. It consists in multiple sub-projects:
 - a specification to describe data pipelines in a language-agnostic manner,
   and a communication protocol to submit these pipelines to Spark. The language is JSON (to be switched to proto3).
 - a serving library, called krapsh-server, that implements this specification
   on top of Spark. It is written in Scala and is loaded as a standard Spark package.
 - a client written in Haskell that sends pipelines to Spark for execution. In
   addition, this client serves as an experimental platform for whole-program optimization and verification, as well as compiler-enforced type checking.

There is also a separate set of utilities to visualize such pipelines using
Jupyter notebooks and IHaskell.

This is a preview, the API may (will) change in the future.

The name is a pun on the dice game called craps, because like data science, it
is easy to start rolling dice, and there is a significant element of chance
involved in getting any significant benefit from your data. It also happens to be the anagram of Spark. The programming model is strongly influenced by the TensorFlow project and follows a similar design.

## How to use (quick)

These instructions assume that the following software is installed on your computer:
 - Spark 2.x (2.1+ recommended). See the [installation instructions](http://spark.apache.org/docs/latest/#downloading) for a local install. It is usually a matter a downloading and unzipping the prebuilt binaries.
 - the [stack build tool](https://docs.haskellstack.org/en/stable/README/)

_Launching Spark locally_ Assuming the `SPARK_HOME` environment variable is set
to the location of your current installation of Spark, run:
```sh
$SPARK_HOME/bin/spark-shell --packages krapsh:kraps-server:0.1.9-s_2.11\
   --name kraps-server --class org.krapsh.Boot --master "local[1]" -v
```

You should see a flurry of log messages that ends with something like: `WARN SparkContext: Use an existing SparkContext, some configuration may not take effect.` The server is now running.

_Connecting the Kraps-Haskell client_ All the integration tests should be able
to connect to the server and execute some Spark commands:

```sh
stack build
stack test
```

You are now all set to run your first interactive program:

```sh
stack ghci
```

```haskell
import Spark.Core.Dataset
import Spark.Core.Context
import Spark.Core.Functions
let ds = dataset ([1 ,2, 3, 4]::[Int])
let c = count ds

createSparkSessionDef defaultConf
mycount <- exec1Def c
```

## Status

This project has so far focused on solving the most challenging issues, at the
expense of breadth and functionality. That being said, the basic building blocks of
Spark are here:
 - dataframes, datasets and observables (the results of `collect`)
 - basic data types: ints, strings, arrays, structures (both nullable and strict)
 - basic arithmetic operators on columns of data
 - converting between the typed and untyped operations
 - grouping, joining

You can take a look at the notebooks in the `notebooks` directory to see what is
possible currently.

What is missing? A lot of things. In particular, users will most probably miss:
 - an input interface. The only way to use the bindings is currently to pass a list of data.
 - filters
 - long types, floats, doubles
 - broadcasting observables (scalar * col). This one is interesting and is probably the next piece.
 - setting the number of partitions of the data

## Contributions

Contributions are most welcome. This is the author's _first_ Haskell project, so
all suggestions regarding style, idiomatic code, etc. will be gladly accepted.
Also, if someone wants to setup a style checker, it will be really helpful.

## Theory

The API and design goals are slightly more general than Spark's. A more thorough
explanation can be found in the `INTRO.md` file.
