{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
-- The communication protocol with the server

module Spark.Core.Internal.Client where

import Spark.Core.StructuresInternal
import Spark.Core.Dataset(UntypedNode)
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.TypesStructures(DataType)
import Spark.Core.Internal.TypesFunctions()

import Data.Text(Text, pack)
import Data.Aeson
import Data.Aeson.Types(Parser)
import GHC.Generics


-- Imports for the client

{-| The ID of an RDD in Spark.
-}
data RDDId = RDDId {
 unRDDId :: !Int
} deriving (Eq, Show, Ord)

data LocalSessionId = LocalSessionId {
  unLocalSession :: !Text
} deriving (Eq, Show)

data Computation = Computation {
  cSessionId :: !LocalSessionId,
  cId :: !ComputationID,
  cNodes :: ![UntypedNode],
  -- Non-empty
  cTerminalNodes :: ![NodePath],
  -- The node at the top of the computation.
  -- Must be part of the terminal nodes.
  cCollectingNode :: !NodePath,
  -- This redundant information is not serialized.
  -- It is used internally to track the resulting nodes.
  cTerminalNodeIds :: ![NodeId]
} deriving (Show, Generic)

data BatchComputationKV = BatchComputationKV {
  bckvLocalPath :: !NodePath,
  bckvDeps :: ![NodePath],
  bckvResult :: !PossibleNodeStatus
} deriving (Show, Generic)

data BatchComputationResult = BatchComputationResult {
  bcrTargetLocalPath :: !NodePath,
  bcrResults :: ![(NodePath, [NodePath], PossibleNodeStatus)]
} deriving (Show, Generic)

data RDDInfo = RDDInfo {
 rddiId :: !RDDId,
 rddiClassName :: !Text,
 rddiRepr :: !Text,
 rddiParents :: ![RDDId]
} deriving (Show, Generic)

data SparkComputationItemStats = SparkComputationItemStats {
  scisRddInfo :: ![RDDInfo]
} deriving (Show, Generic)

data PossibleNodeStatus =
    NodeQueued
  | NodeRunning
  | NodeFinishedSuccess !(Maybe NodeComputationSuccess) !(Maybe SparkComputationItemStats)
  | NodeFinishedFailure NodeComputationFailure deriving (Show, Generic)

data NodeComputationSuccess = NodeComputationSuccess {
  -- Because Row requires additional information to be deserialized.
  ncsData :: Value,
  -- The data type is also available, but it is not going to be parsed for now.
  ncsDataType :: DataType
} deriving (Show, Generic)

data NodeComputationFailure = NodeComputationFailure {
  ncfMessage :: !Text
} deriving (Show, Generic)


-- **** AESON INSTANCES ***

instance ToJSON LocalSessionId where
  toJSON = toJSON . unLocalSession

instance FromJSON RDDId where
  parseJSON x = RDDId <$> parseJSON x

instance FromJSON RDDInfo where
  parseJSON = withObject "RDDInfo" $ \o -> do
    _id <- o .: "id"
    className <- o .: "className"
    repr <- o .: "repr"
    parents <- o .: "parents"
    return $ RDDInfo _id className repr parents

instance FromJSON SparkComputationItemStats where
  parseJSON = withObject "SparkComputationItemStats" $ \o -> do
    rddinfo <- o .: "rddInfo"
    return $ SparkComputationItemStats rddinfo

instance FromJSON BatchComputationKV where
  parseJSON = withObject "BatchComputationKV" $ \o -> do
    np <- o .: "localPath"
    deps <- o .: "pathDependencies"
    res <- o .: "result"
    return $ BatchComputationKV np deps res

instance FromJSON BatchComputationResult where
  parseJSON = withObject "BatchComputationResult" $ \o -> do
    kvs <- o .: "results"
    tlp <- o .: "targetLocalPath"
    let f (BatchComputationKV k d v) = (k, d, v)
    return $ BatchComputationResult tlp (f <$> kvs)

instance FromJSON NodeComputationSuccess where
  parseJSON = withObject "NodeComputationSuccess" $ \o -> NodeComputationSuccess
    <$> o .: "content"
    <*> o .: "type"

-- Because we get a row back, we need to supply a SQLType for deserialization.
instance FromJSON PossibleNodeStatus where
  parseJSON =
    let parseSuccess :: Object -> Parser PossibleNodeStatus
        parseSuccess o = NodeFinishedSuccess
          <$> o .:? "finalResult"
          <*> o .:? "stats"
        parseFailure :: Object -> Parser PossibleNodeStatus
        parseFailure o =
          (NodeFinishedFailure . NodeComputationFailure) <$> o .: pack "finalError"
    in
      withObject "PossibleNodeStatus" $ \o -> do
      status <- o .: pack "status"
      case pack status of
        "running" -> return NodeRunning
        "finished_success" -> parseSuccess o
        "finished_failure" -> parseFailure o
        "scheduled" -> return NodeQueued
        _ -> failure $ pack ("FromJSON PossibleNodeStatus " ++ show status)
