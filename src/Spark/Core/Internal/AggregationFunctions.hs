{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}

-- A number of standard aggregation functions.

module Spark.Core.Internal.AggregationFunctions where

import Data.Aeson(Value(Null))
import qualified Data.Text as T
import Formatting

import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.RowGenerics(ToSQL)
import Spark.Core.Internal.LocalDataFunctions()
import Spark.Core.Internal.FunctionsInternals
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.Utilities(failure, HasCallStack, missing)
import Spark.Core.Internal.TypesStructures
import Spark.Core.Types

{-| The sum of all the elements in a column.

If the data type is too small to represent the sum, the value being returned is
undefined.
-}
colSum :: forall ref a. (Num a, SQLTypeable a, ToSQL a) =>
  Column ref a -> LocalData a
colSum = applyUniAgg (_sumAgg :: UniversalAggregator a a)

{-| The number of elements in a column.

-}
-- TODO use Long for the return data type.
count :: forall a. Dataset a -> LocalData Int
count ds = applyUniAgg (_countAgg2 :: UniversalAggregator a Int) (asCol ds)

{-| Collects all the elements of a column into a list.

NOTE:
This list is sorted in the canonical ordering of the data type: however the
data may be stored by Spark, the result will always be in the same order.
This is a departure from Spark, which does not guarantee an ordering on
the returned data.
-}
collect :: forall ref a. (SQLTypeable a) => Column ref a -> LocalData [a]
collect = applyUniAgg (_collectAgg :: UniversalAggregator a [a])

collect' :: DynColumn -> LocalFrame
collect' = missing "collect'"

{-|
This is the universal aggregator: the invariant aggregator and
some extra laws to combine multiple outputs.
It is useful for combining the results over multiple passes.
A real implementation in Spark has also an inner pass.
-}
data UniversalAggregator a buff = UniversalAggregator {
  -- The result is partioning invariant
  uaInitialOuter :: Dataset a -> LocalData buff,
  -- This operation is associative and commutative
  -- The logical parents of the final observable have to be the 2 inputs
  uaMergeBuffer :: LocalData buff -> LocalData buff -> LocalData buff
}

data UntypedUniversalAggregator = UntypedUniversalAggregator {
  uuaInitialOuter :: UntypedDataset -> LocalFrame,
  uuaMergeBuffer :: UntypedLocalData -> UntypedLocalData -> LocalFrame
}

-- | (internal)
univAggToOp :: forall a buff. (SQLTypeable a, SQLTypeable buff) =>
  UniversalAggregator a buff -> UniversalAggregatorOp
univAggToOp = univAggToOpTyped (buildType :: SQLType a) (buildType :: SQLType buff)

-- | (internal)
univAggToOpTyped :: forall a buff.
  SQLType a ->
  SQLType buff ->
  UniversalAggregator a buff ->
  UniversalAggregatorOp
univAggToOpTyped sqlta sqltm ua =
  let
    mt = unSQLType sqltm
    outer = _unsafeExtractOp $ fun1ToOpTyped sqlta (uaInitialOuter ua)
    merge = _unsafeExtractOp $ fun2ToOpTyped sqltm sqltm (uaMergeBuffer ua)
  in UniversalAggregatorOp {
    uaoMergeType = mt,
    uaoInitialOuter = outer,
    uaoMergeBuffer = merge
  }

-- | (internal)
applyUniAgg :: UniversalAggregator a b -> Column ref a -> LocalData b
applyUniAgg ua c =
  let
    ds = pack1 c
    ld1 = uaInitialOuter ua ds
    -- TODO understand how to pass this info
    -- aggop = univAggToOpTyped (nodeType ds) (nodeType ld1) ua
    -- ld = emptyLocalData (NodeUniversalAggregator aggop) (nodeType ld1)
  in ld1


-- (internal)
simpleOp1Typed :: (IsLocality locb) =>
  SQLType b ->
  T.Text ->
  ComputeNode loca a -> ComputeNode locb b
simpleOp1Typed sqltb name =
  let so = StandardOperator {
             soName = name,
             soOutputType = unSQLType sqltb,
             soExtra = Null
           }
      no = NodeLocalOp so
  in nodeOpToFun1Typed sqltb no

-- (internal)
simpleOp1 :: forall a b loca locb. (IsLocality locb, SQLTypeable b) =>
  T.Text ->
  ComputeNode loca a -> ComputeNode locb b
simpleOp1 = simpleOp1Typed (buildType :: SQLType b)

-- (internal)
simpleOp2 :: forall a1 a2 b loc1 loc2 locb. (SQLTypeable b, IsLocality locb) =>
  T.Text ->
  ComputeNode loc1 a1 -> ComputeNode loc2 a2 -> ComputeNode locb b
simpleOp2 = simpleOp2Typed (buildType :: SQLType b)

-- (internal)
simpleOp2Typed :: (IsLocality locb) =>
  SQLType b ->
  T.Text ->
  ComputeNode loc1 a1 -> ComputeNode loc2 a2 -> ComputeNode locb b
simpleOp2Typed sqltb name =
  let so = StandardOperator {
             soName = name,
             soOutputType = unSQLType sqltb,
             soExtra = Null
           }
      no = NodeLocalOp so
  in nodeOpToFun2Typed sqltb no

_unsafeExtractOp :: (HasCallStack) => NodeOp -> StandardOperator
_unsafeExtractOp (NodeLocalOp so) = so
_unsafeExtractOp (NodeOpaqueAggregator so) = so
_unsafeExtractOp (NodeDistributedOp so) = so
_unsafeExtractOp x = failure $ sformat ("Expected standard op, found "%shown) x

_countAgg2 :: UniversalAggregator a Int
_countAgg2 = UniversalAggregator {
    uaInitialOuter = simpleOp1 "org.spark.Count",
    uaMergeBuffer = (+)
  }

_sumAgg :: forall a. (SQLTypeable a, Num a, ToSQL a) => UniversalAggregator a a
_sumAgg = UniversalAggregator {
    uaInitialOuter = simpleOp1 "org.spark.Sum",
    uaMergeBuffer = (+)
  }

_collectAgg :: forall a. SQLTypeable a => UniversalAggregator a [a]
_collectAgg =
  UniversalAggregator {
    uaInitialOuter = simpleOp1 "org.spark.Collect",
    uaMergeBuffer = simpleOp2 "org.spark.CatSorted"
  }
