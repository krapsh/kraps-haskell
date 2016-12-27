{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.Functions(
  dataset,
  dataframe,
  constant,
  collect,
  count,
  identity,
  autocache,
  cache,
  uncache,
  (@@)
  ) where


import Data.Aeson(toJSON)
import qualified Data.Vector as V

import Spark.Core.Dataset
import Spark.Core.Types
import Spark.Core.Row
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.LocalDataFunctions
import Spark.Core.Internal.FunctionsInternals()
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.AggregationFunctions
import Spark.Core.Internal.TypesStructures(SQLType(..))

dataset :: (ToSQL a, SQLTypeable a, HasCallStack) => [a] -> Dataset a
dataset l = emptyDataset op tp where
  tp = buildType
  op = NodeDistributedLit (unSQLType tp) (V.fromList ((toJSON . valueToCell) <$> l))
