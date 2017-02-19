{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.Internal.ContextStructures(
  SparkSessionConf(..),
  SparkSession(..),
  SparkState,
  SparkStatePure,
  ComputeGraph,
  HdfsPath(..),
  NodeCacheInfo(..),
  NodeCacheStatus(..),
  -- InternalStateT,
  SparkStateT,
  SparkStatePureT
) where

import Data.Text(Text)
import Control.Monad.State(StateT, State)
import Control.Monad.Logger(LoggingT)
import Data.HashMap.Strict as HM

import Spark.Core.StructuresInternal(NodeId, NodePath)
import Spark.Core.Internal.Client(LocalSessionId, ComputationID)
import Spark.Core.Internal.ComputeDag(ComputeDag)
import Spark.Core.Internal.OpStructures(HdfsPath(..))
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
  ssConf :: !SparkSessionConf,
  ssId :: !LocalSessionId,
  ssCommandCounter :: !Integer,
  ssNodeCache :: HM.HashMap NodeId NodeCacheInfo
} deriving (Show)

{-| The status of a node being computed.

On purpose, it does not store data. This is meant to be
only the control plane of the compuations.
-}
data NodeCacheStatus =
    NodeCacheRunning
  | NodeCacheError
  | NodeCacheSuccess
  deriving (Eq, Show)

{-| This structure describes the last time a node was observed by the
controller, and the state it was in.

This information is used to do smart computation pruning, by assuming
that the observables are kept by the Spark processes.
-}
data NodeCacheInfo = NodeCacheInfo {
  nciStatus :: !NodeCacheStatus,
  nciComputation :: !ComputationID,
  nciPath :: !NodePath
} deriving (Eq, Show)

-- | Represents the state of a session and accounts for the communication
-- with the server.
type SparkState a = SparkStateT IO a

-- More minimalistic state transforms when doing pure evaluation.
-- (internal type)
-- TODO: use the transformer
type SparkStatePure x = State SparkSession x

type SparkStatePureT m = StateT SparkSession m
type SparkStateT m = LoggingT (SparkStatePureT m)

{-| internal

A graph of computations. This graph is a direct acyclic graph. Each node is
associated to a global path.
-}
type ComputeGraph = ComputeDag UntypedNode StructureEdge
