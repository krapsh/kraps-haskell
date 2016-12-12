
module Spark.Core.Experimental.GroupBy where

import Control.Arrow

import Spark.Core.Dataset
import Spark.Core.Types
import Spark.Core.Column
import Spark.Core.Try
import Spark.Core.Internal.Utilities

data RelationalGroupedData key val = RelationalGroupedData {
  _key :: Column key,
  _value :: Column val
}

type Group key val = RelationalGroupedData key val

type DynGroup = Try (RelationalGroupedData UnknownType UnknownType)

-- The column may not come from the dataset, or it may not be a valid grouping
-- column
groupBy :: Column key -> Dataset a -> Try (Group key a)
groupBy = undefined

groupBy' :: DynColumn -> DataFrame -> DynGroup
groupBy' = undefined

mapGroup :: Group key a -> (Dataset a -> Dataset b) -> Group key b
mapGroup = undefined

agg :: Group key a -> (Dataset (key, a) -> LocalData b) -> Dataset (key, b)
agg = undefined

aggNoKey :: Group key a -> (Dataset a -> LocalData b) -> Dataset b
aggNoKey = undefined

aggNoKey' :: DynGroup -> (DataFrame -> LocalFrame) -> DataFrame
aggNoKey' = undefined


count :: Dataset a -> LocalData Int
count = undefined

sum' :: Column Int -> LocalData Int
sum' = undefined

proj :: Dataset a -> Column Int
proj = undefined

g :: Group Double Float
g = undefined

composeObs :: (LocalData a, LocalData b) -> LocalData (a, b)
composeObs = undefined

myAgg ds =
  let o1 = sum' (proj ds)
      o2 = count ds
  in composeObs (o1, o2)


z = g `agg` (proj >>> sum')
z1 = g `agg` ((count &&& (proj >>> sum')) >>> composeObs)
z2 = g `agg` myAgg
