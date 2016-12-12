{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.Internal.ContextStructures(
  SparkSessionConf(..),
  SparkSession(..),
  SparkState,
  SparkStatePure,
  ComputeGraph
) where

import Data.Text(Text)
import Control.Monad.State(StateT, State)
import Control.Monad.Logger(LoggingT)

import Spark.Core.Internal.Client(LocalSessionId)
import Spark.Core.Internal.ComputeDag(ComputeDag)
import Spark.Core.Internal.DatasetStructures(UntypedNode, StructureEdge)

-- | The configuration of a remote spark session in krapsh.
data SparkSessionConf = SparkSessionConf {
 -- | The URL of the end point.
  confEndPoint :: !Text,
  -- | The port used to configure the end point.
  confPort :: !Int,
  -- | (internal) the polling interval
  confPollingIntervalMillis :: !Int,
  -- | (optional) the requested name of the session.
  -- This name must obey a number of rules:
  --  - it must consist in alphanumerical and -,_: [a-zA-Z0-9\-_]
  --  - if it already exists on the server, it will be reconnected to
  --
  -- The default value is "" (a new random context name will be chosen).
  confRequestedSessionName :: !Text
} deriving (Show)

-- | A session in Spark.
-- Encapsualates all the state needed to communicate with Spark
-- and to perfor some simple optimizations on the code.
data SparkSession = SparkSession {
  ssConf :: SparkSessionConf,
  ssId :: LocalSessionId,
  ssCommandCounter :: Integer
} deriving (Show)

-- | Represents the state of a session and accounts for the communication
-- with the server.
type SparkState a = LoggingT (StateT SparkSession IO) a

-- More minimalistic state transforms when doing pure evaluation.
-- (internal type)
type SparkStatePure x = State SparkSession x

{-| internal

A graph of computations. This graph is a direct acyclic graph. Each node is
associated to a global path.
-}
type ComputeGraph = ComputeDag UntypedNode StructureEdge
