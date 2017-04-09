{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

-- Some basic structures about nodes in a graph, etc.

module Spark.Core.StructuresInternal(
  NodeName(..),
  NodePath(..),
  NodeId(..),
  FieldName(..),
  FieldPath(..),
  ComputationID(..),
  catNodePath,
  fieldName,
  unsafeFieldName,
  emptyFieldPath,
  nullFieldPath,
  headFieldPath,
  fieldPath,
  prettyNodePath,
) where

import qualified Data.Text as T
import Data.ByteString(ByteString)
import GHC.Generics (Generic)
import Data.Hashable(Hashable)
import Data.List(intercalate)
import qualified Data.Aeson as A
import Data.String(IsString(..))
import Data.Vector(Vector)
import qualified Data.Vector as V

import Spark.Core.Internal.Utilities

-- | The name of a node (without path information)
newtype NodeName = NodeName { unNodeName :: T.Text } deriving (Eq, Ord)

-- | The user-defined path of the node in the hierarchical representation of the graph.
newtype NodePath = NodePath { unNodePath :: Vector NodeName } deriving (Eq, Ord)

-- | The unique ID of a node. It is based on the parents of the node
-- and all the relevant intrinsic values of the node.
newtype NodeId = NodeId { unNodeId :: ByteString } deriving (Eq, Ord, Generic)

-- | The name of a field in a sql structure
-- This structure ensures that proper escaping happens if required.
-- TODO: prevent the constructor from being used, it should be checked first.
newtype FieldName = FieldName { unFieldName :: T.Text } deriving (Eq)

-- | A path to a nested field an a sql structure.
-- This structure ensures that proper escaping happens if required.
newtype FieldPath = FieldPath { unFieldPath :: Vector FieldName } deriving (Eq)

{-| A unique identifier for a computation (a batch of nodes sent for execution
to Spark).
-}
data ComputationID = ComputationID {
  unComputationID :: !T.Text
} deriving (Eq, Show, Generic)



-- | A safe constructor for field names that fixes all the issues relevant to
-- SQL escaping
-- TODO: proper implementation
fieldName :: T.Text -> Either String FieldName
fieldName = Right . FieldName

-- | Constructs the field name, but will fail if the content is not correct.
unsafeFieldName :: (HasCallStack) => T.Text -> FieldName
unsafeFieldName = forceRight . fieldName

-- | A safe constructor for field names that fixes all the issues relevant to SQL escaping
-- TODO: proper implementation
fieldPath :: T.Text -> Either String FieldPath
fieldPath x = Right . FieldPath . V.singleton $ FieldName x

emptyFieldPath :: FieldPath
emptyFieldPath = FieldPath V.empty

nullFieldPath :: FieldPath -> Bool
nullFieldPath = V.null . unFieldPath

headFieldPath :: FieldPath -> Maybe FieldName
headFieldPath (FieldPath v) | V.null v = Nothing
headFieldPath (FieldPath v) = Just $ V.head v

-- | The concatenated path. This is the inverse function of fieldPath.
-- | TODO: this one should be hidden?
catNodePath :: NodePath -> T.Text
catNodePath (NodePath np) =
  T.intercalate "/" (unNodeName <$> V.toList np)

prettyNodePath :: NodePath -> T.Text
-- Only a single slash, double slashes are reserved for the case
-- of global paths (including session and computation)
prettyNodePath np = "/" <> catNodePath np

instance Show NodeId where
  show (NodeId bs) = let s = show bs in
    if length s > 9 then
      (drop 1 . take 6) s ++ ".."
    else
      s

instance Show NodeName where
  show (NodeName nn) = T.unpack nn

instance Show NodePath where
  show np = T.unpack $ T.concat ["NPath(", catNodePath np, ")" ]

instance Show FieldPath where
  show (FieldPath l) =
    intercalate "." (show <$> V.toList l)

instance Show FieldName where
  -- TODO(kps) escape the '.' characters in the field name
  show (FieldName fn) = T.unpack fn

instance Hashable NodeId

instance IsString FieldName where
  fromString = FieldName . T.pack

instance A.ToJSON NodeName where
  toJSON = A.toJSON . unNodeName

instance A.FromJSON NodeName where
  -- TODO: more parse checks
  parseJSON x = NodeName <$> A.parseJSON x

instance A.ToJSON NodePath where
  toJSON = A.toJSON . unNodePath

instance A.FromJSON NodePath where
  parseJSON x = NodePath <$> A.parseJSON x

instance A.ToJSON FieldName where
  toJSON = A.toJSON . unFieldName

instance A.ToJSON FieldPath where
  toJSON = A.toJSON . unFieldPath

instance Ord FieldName where
  compare f1 f2 = compare (unFieldName f1) (unFieldName f2)

instance A.ToJSON ComputationID where
  toJSON = A.toJSON . unComputationID
