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


data LocalSessionId = LocalSessionId {
  unLocalSession :: !Text
} deriving (Eq, Show)

data ComputationID = ComputationID {
  unComputationID :: !Text
} deriving (Eq, Show, Generic)

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


data PossibleNodeStatus =
    NodeQueued
  | NodeRunning
  | NodeFinishedSuccess NodeComputationSuccess
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

-- instance ToJSON Computation where
--   toJSON c = object [
--         "sessionId" .= toJSON (cSessionId c),
--         "id" .= toJSON (cId c),
--         "nodes" .= toJSON (cNodes c),
--         "terminalNodes" .= toJSON (cTerminalNodes c),
--         "collectingNode" .= toJSON (cCollectingNode c)
--       ]

instance ToJSON LocalSessionId where
  toJSON = toJSON . unLocalSession

-- instance FromJSON LocalSessionID where
--   parseJSON = LocalSessionID <$> parseJSON

instance ToJSON ComputationID where
  toJSON = toJSON . unComputationID

-- Because we get a row back, we need to supply a SQLType for deserialization.
instance FromJSON PossibleNodeStatus where
  parseJSON =
    let parseSuccess :: Object -> Parser PossibleNodeStatus
        parseSuccess o = do
          res <- o .: pack "finalResult"
          x <- res .: pack "content"
          dt <- res .: pack "type"
          return . NodeFinishedSuccess $ NodeComputationSuccess x dt
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

    -- Person <$> o .: "name" <*> o .: "age"

-- instance ToJSON PossibleNodeStatus where
--   toJSON NodeQueued = toJSON "queued"
--   toJSON NodeRunning = toJSON "running"
--   toJSON NodeFinished = toJSON "finished"
