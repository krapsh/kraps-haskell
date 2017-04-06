{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Spark.Core.Internal.ColumnStructures where

import Control.Arrow ((&&&))
import Data.Function(on)
import Data.Vector(Vector)

import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.DatasetFunctions()
import Spark.Core.Internal.RowStructures
import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.OpStructures
import Spark.Core.StructuresInternal
import Spark.Core.Try

{-| The data structure that implements the notion of data columns.

The type on this one may either be a Cell or a proper type.

A column of data from a dataset
The ref is a reference potentially to the originating
dataset, but it may be more general than that to perform
type-safe tricks.

Unlike Spark, columns are always attached to a reference dataset or dataframe.
One cannot materialize a column out of thin air. In order to broadcast a value
along a given column, the `broadcast` function is provided.

TODO: try something like this https://www.vidarholen.net/contents/junk/catbag.html
-}
data ColumnData ref a = ColumnData {
  _cOrigin :: !UntypedDataset,
  _cType :: !DataType,
  _cOp :: !GeneralizedColOp,
  -- The name in the dataset.
  -- If not set, it will be deduced from the operation.
  _cReferingPath :: !(Maybe FieldName)
}

{-| A generalization of the column operation.

This structure is useful to performn some extra operations not supported by
the Spark engine:
 - express joins with an observable
 - keep track of DAGs of column operations (not implemented yet)
-}
data GeneralizedColOp =
    GenColExtraction !FieldPath
  | GenColFunction !SqlFunctionName !(Vector GeneralizedColOp)
  | GenColLit !DataType !Cell
    -- This is the extra operation that needs to be flattened with a broadcast.
  | BroadcastColOp !UntypedLocalData
  | GenColStruct !(Vector GeneralizedTransField)
  deriving (Eq, Show)

data GeneralizedTransField = GeneralizedTransField {
  gtfName :: !FieldName,
  gtfValue :: !GeneralizedColOp
} deriving (Eq, Show)

{-| A column of data from a dataset or a dataframe.

This column is typed: the operations on this column will be
validdated by Haskell' type inferenc.
-}
type Column ref a = ColumnData ref a

{-| An untyped column of data from a dataset or a dataframe.

This column is untyped and may not be properly constructed. Any error
will be found during the analysis phase at runtime.
-}
type DynColumn = Try (ColumnData UnknownReference Cell)


-- | (dev)
-- The type of untyped column data.
type UntypedColumnData = ColumnData UnknownReference Cell

{-| (dev)
A column for which the type of the cells is unavailable (at the type level),
 but for which the origin is available at the type level.
-}
type GenericColumn ref = Column ref Cell

{-| A dummy data type that indicates the data referenc is missing.
-}
data UnknownReference

{-| A tag that carries the reference information of a column at a
type level. This is useful when creating column.

See ref and colRef.
-}
data ColumnReference a = ColumnReference

instance forall ref a. Eq (ColumnData ref a) where
  (==) = (==) `on` (_cOrigin &&& _cType &&& _cOp &&& _cReferingPath)
