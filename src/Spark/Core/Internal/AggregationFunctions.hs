{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}

-- A number of standard aggregation functions.

module Spark.Core.Internal.AggregationFunctions(
  -- Standard library
  collect,
  collect',
  count,
  count',
  countCol,
  countCol',
  sumCol,
  sumCol',
  -- Developer functions
  AggTry,
  UniversalAggregator(..),
  applyUAOUnsafe,
  applyUntypedUniAgg3
) where

import Data.Aeson(Value(Null))
import qualified Data.Text as T
import qualified Data.Vector as V

import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.ColumnFunctions(colType, untypedCol)
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.RowGenerics(ToSQL)
import Spark.Core.Internal.LocalDataFunctions()
import Spark.Core.Internal.FunctionsInternals
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.TypesFunctions(arrayType')
import Spark.Core.StructuresInternal(emptyFieldPath)
import Spark.Core.Types
import Spark.Core.Try

{-| The sum of all the elements in a column.

If the data type is too small to represent the sum, the value being returned is
undefined.
-}
sumCol :: forall ref a. (Num a, SQLTypeable a, ToSQL a) =>
  Column ref a -> LocalData a
sumCol = applyUAOUnsafe _sumAgg'

sumCol' :: DynColumn -> LocalFrame
sumCol' = applyUntypedUniAgg3 _sumAgg'

{-| The number of elements in a column.

-}
-- TODO use Long for the return data type.
count :: forall a. Dataset a -> LocalData Int
count = countCol . asCol

count' :: DataFrame -> LocalFrame
count' = countCol' . asCol'

countCol :: Column ref a -> LocalData Int
countCol = applyUAOUnsafe _countAgg'

countCol' :: DynColumn -> LocalFrame
countCol' = applyUntypedUniAgg3 _countAgg'


{-| Collects all the elements of a column into a list.

NOTE:
This list is sorted in the canonical ordering of the data type: however the
data may be stored by Spark, the result will always be in the same order.
This is a departure from Spark, which does not guarantee an ordering on
the returned data.
-}
collect :: forall ref a. (SQLTypeable a) => Column ref a -> LocalData [a]
collect = applyUAOUnsafe _collectAgg'

{-| See the documentation of collect. -}
collect' :: DynColumn -> LocalFrame
collect' = applyUntypedUniAgg3 _collectAgg'

type AggTry a = Either T.Text a

{-|
This is the universal aggregator: the invariant aggregator and
some extra laws to combine multiple outputs.
It is useful for combining the results over multiple passes.
A real implementation in Spark has also an inner pass.
-}
data UniversalAggregator a buff = UniversalAggregator {
  uaMergeType :: SQLType buff,
  -- The result is partioning invariant
  uaInitialOuter :: Dataset a -> LocalData buff,
  -- This operation is associative and commutative
  -- The logical parents of the final observable have to be the 2 inputs
  uaMergeBuffer :: LocalData buff -> LocalData buff -> LocalData buff
}

-- TODO(kps) check the coming type for non-summable types
_sumAgg' :: DataType -> AggTry UniversalAggregatorOp
_sumAgg' dt = pure UniversalAggregatorOp {
    uaoMergeType = dt,
    uaoInitialOuter = InnerAggOp $ AggFunction "SUM" (V.singleton emptyFieldPath),
    uaoMergeBuffer = ColumnSemiGroupLaw "SUM_SL"
  }

_countAgg' :: DataType -> AggTry UniversalAggregatorOp
-- Counting will always succeed.
_countAgg' _ = pure UniversalAggregatorOp {
    -- TODO(kps) switch to BigInt
    uaoMergeType = StrictType IntType,
    uaoInitialOuter = InnerAggOp $ AggFunction "COUNT" (V.singleton emptyFieldPath),
    uaoMergeBuffer = ColumnSemiGroupLaw "SUM"
  }

_collectAgg' :: DataType -> AggTry UniversalAggregatorOp
-- Counting will always succeed.
_collectAgg' dt =
  let ldt = arrayType' dt
      soMerge = StandardOperator {
                 soName = "org.spark.Collect",
                 soOutputType = ldt,
                 soExtra = Null
           }
      soMono = StandardOperator {
                  soName = "org.spark.CatSorted",
                  soOutputType = ldt,
                  soExtra = Null
            }
  in pure UniversalAggregatorOp {
    -- TODO(kps) switch to BigInt
    uaoMergeType = ldt,
    uaoInitialOuter = OpaqueAggTransform soMerge,
    uaoMergeBuffer = OpaqueSemiGroupLaw soMono
  }

applyUntypedUniAgg3 :: (DataType -> AggTry UniversalAggregatorOp) -> DynColumn -> LocalFrame
applyUntypedUniAgg3 f dc = do
  c <- dc
  let uaot = f . unSQLType . colType $ c
  uao <- tryEither uaot
  let no = NodeAggregatorReduction uao
  let ds = pack1 c
  return $ emptyLocalData no (SQLType (uaoMergeType uao)) `parents` [untyped ds]

applyUAOUnsafe :: forall a b ref. (SQLTypeable b, HasCallStack) => (DataType -> AggTry UniversalAggregatorOp) -> Column ref a -> LocalData b
applyUAOUnsafe f c =
  let lf = applyUntypedUniAgg3 f (untypedCol c)
  in forceRight (asObservable lf)

-- _guardType :: DataType -> (UntypedDataset -> UntypedLocalData) -> (UntypedDataset -> LocalFrame)
-- _guardType dt f ds =
--   if unSQLType (nodeType ds) == dt
--   then
--     pure $ f ds
--   else
--     tryError $ sformat ("Expected type "%sh%" but got type "%sh) dt (nodeType ds)
