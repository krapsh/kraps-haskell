{-# LANGUAGE OverloadedStrings #-}
{-| Exposes some of Spark's joining algorithms.
-}
module Spark.Core.Internal.Joins(
  join,
  join',
  joinInner,
  joinInner'
) where

import qualified Data.Aeson as A
import qualified Data.Vector as V

import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.ColumnFunctions
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.FunctionsInternals
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.Utilities
import Spark.Core.Try

{-| Standard (inner) join on two sets of data.
-}
join :: Column ref1 key -> Column ref1 value1 -> Column ref2 key -> Column ref2 value2 -> Dataset (key, value1, value2)
join = joinInner

{-| Untyped version of the standard join.
-}
join' :: DynColumn -> DynColumn -> DynColumn -> DynColumn -> DataFrame
join' = joinInner'

{-| Explicit inner join.
-}
joinInner :: Column ref1 key -> Column ref1 value1 -> Column ref2 key -> Column ref2 value2 -> Dataset (key, value1, value2)
joinInner key1 val1 key2 val2 = unsafeCastDataset (forceRight df) where
  df = joinInner' (untypedCol key1) (untypedCol val1) (untypedCol key2) (untypedCol val2)

joinInner' :: DynColumn -> DynColumn -> DynColumn -> DynColumn -> DataFrame
joinInner' key1 val1 key2 val2 = do
  df1 <- pack' (struct' [key1, val1])
  df2 <- pack' (struct' [key2, val2])
  dt <- _joinTypeInner key1 val1 val2
  let so = StandardOperator { soName = "org.spark.Join", soOutputType = dt, soExtra = A.String "inner" }
  let ds = emptyDataset (NodeDistributedOp so) (SQLType dt)
  let f ds' = ds' { _cnParents = V.fromList [untyped df1, untyped df2] }
  return $ updateNode ds f

_joinTypeInner :: DynColumn -> DynColumn -> DynColumn -> Try DataType
_joinTypeInner kcol col1 col2 = (unSQLType . colType) <$> struct' [kcol, col1, col2]
