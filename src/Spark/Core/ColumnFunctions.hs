
{-|
Module      : Spark.Core.ColumnFunctions
Description : Column operations

The standard library of functions that operate on
data columns.
-}
module Spark.Core.ColumnFunctions(
  -- * Reductions
  sumCol,
  sumCol',
  countCol,
  countCol',
  -- * Casting
  asDoubleCol
) where

import Spark.Core.Internal.ArithmeticsImpl()
import Spark.Core.Internal.ColumnStandard
import Spark.Core.Internal.AggregationFunctions
import Spark.Core.Internal.Projections()
