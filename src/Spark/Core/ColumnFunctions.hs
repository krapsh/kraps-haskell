
{-|
Module      : Spark.Core.ColumnFunctions
Description : Column operations

The standard library of functions that operate on
data columns.
-}
module Spark.Core.ColumnFunctions(
  -- * Arithmetic operations
  (.+),
  -- * Reductions
  sumCol,
  sumCol'
) where

import Spark.Core.Internal.AlgebraStructures
import Spark.Core.Internal.AggregationFunctions
