{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}


module Spark.Core.Internal.TypesStructuresRepr(
  DataTypeElementRepr(..),
  DataTypeRepr(..)
) where

import qualified Data.Text as T
import GHC.Generics(Generic)

-- The inner representation of a dataype as a Row object.
-- This representation is meant to be internal.
-- Because the Spark data types do not support recursive types (trees),
-- This is a flattened representation of types.
data DataTypeElementRepr = DataTypeElementRepr {
  fieldPath :: ![T.Text],
  isNullable :: !Bool,
  typeId :: !Int,
  fieldIndex :: !Int
} deriving (Eq, Show, Generic)

data DataTypeRepr = DataTypeRepr {
  rows :: [DataTypeElementRepr]
} deriving (Eq, Show, Generic)
