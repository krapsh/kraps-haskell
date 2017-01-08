{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}

-- A number of standard aggregation functions.

module Spark.Core.Internal.Groups where

import Data.Aeson(Value(Null))
import qualified Data.Text as T
import Formatting

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
import Spark.Core.Internal.RowStructures(Cell)
import Spark.Core.Types
import Spark.Core.Try

{-| A dataset that has been partitioned according to some given field.
-}
data GroupData key val = GroupData {

}

type LogicalGroupData = Try UntypedGroupData

{-| (developper)

A group data type with no typing information.
-}
type UntypedGroupData = GroupData Cell Cell


{-| Performs a logical group of data based on a key.
-}
groupByKey :: Column ref key -> Column ref val -> GroupData key val
groupByKey = undefined

{-| Transforms the values in a group.
-}
mapGroupWithKey :: (LocalData key -> Column ref val -> Column ref val') -> GroupData key val -> GroupData key val'
mapGroupWithKey = undefined

{-| Given a group and an aggregation function, aggregates the data.

Note: not all the reduction functions may be used in this case. The analyzer
will fail if the function is not universal.
-}
aggKey :: GroupData key val -> (LocalData key -> Column ref val -> LocalData val') -> Dataset (key, val')
aggKey = undefined

{-| Creates a group by 'expanding' a value into a potentially large collection.

Note on performance: this function is optimized to work at any scale and may not
be the most efficient when the generated collections are small (a few elements).
-}
expand :: Column ref key -> Column ref val -> (LocalData val -> Dataset val') -> GroupData key val'
expand = undefined

{-| Builds groups within groups.

This function allows groups to be constructed from each collections inside a
group.

This function is usually not used directly by the user, but rather as part of
more complex pipelines that may involve multiple levels of nesting.
-}
groupInGroupWithKey :: GroupData key val -> (LocalData key -> Column ref val -> GroupData key' val') -> GroupData (key', key) val'
groupInGroupWithKey = undefined

{-| Reduces a group in group into a single group.
-}
aggGroup :: GroupData (key, key') val -> (LocalData key -> Column ref val -> LocalData val') -> GroupData key val
aggGroup = undefined
