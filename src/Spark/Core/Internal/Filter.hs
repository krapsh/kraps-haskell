{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.Internal.Filter where

import Data.Aeson(Value(Null))
import qualified Data.Text as T
import Formatting

import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.ColumnFunctions(colType, untypedCol, iUntypedColData, colOrigin)
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


{-| Performs a filtering operation on columns of a dataset.

The first column is the reference column that is used to filter out values of
the second column.
-}
filterCol :: Column ref Bool -> Column ref a -> Dataset a
filterCol = missing "filterCol"

{-| Filters a column depending to only keep the strict data.

This function is useful to filter out some data within a structure, some of which
may not be strict.
-}
filterOpt :: Column ref (Maybe a) -> Column ref b -> Dataset (a, b)
filterOpt = missing "filterOpt"

{-| Only keeps the strict values of a column.
-}
filterMaybe :: Column ref (Maybe a) -> Dataset a
filterMaybe = missing "filterMaybe"
