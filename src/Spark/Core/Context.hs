{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | This module defines session objects that act as entry points to spark.

There are two ways to interact with Spark: using an explicit state object,
or using the default state object (interactive session).

While the interactive session is the most convenient, it should not be
used for more than quick experimentations. Any complex code should use
the SparkSession and SparkState objects.
-}
module Spark.Core.Context(
  SparkSessionConf(..),
  SparkSession,
  SparkState,
  SparkInteractiveException,
  FromSQL,
  defaultConf,
  executeCommand1,
  executeCommand1',
  computationStats,
  createSparkSessionDef,
  closeSparkSessionDef,
  currentSessionDef,
  computationStatsDef,
  exec1Def,
  exec1Def',
  execStateDef
  ) where

import Data.Text(pack)

import Spark.Core.Internal.ContextStructures
import Spark.Core.Internal.ContextIOInternal
import Spark.Core.Internal.ContextInteractive
import Spark.Core.Internal.RowGenericsFrom(FromSQL)


-- | The default configuration if the Karps server is being run locally.
defaultConf :: SparkSessionConf
defaultConf =
  SparkSessionConf {
    confEndPoint = pack "http://127.0.0.1",
    confPort = 8081,
    confPollingIntervalMillis = 500,
    confRequestedSessionName = "",
    confUseNodePrunning = False -- Disable graph pruning by default
  }
