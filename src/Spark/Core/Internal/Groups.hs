{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}

-- A number of standard aggregation functions.

module Spark.Core.Internal.Groups(
  GroupData,
  LogicalGroupData,
  -- Typed functions
  groupByKey,
  mapGroup,
  aggKey,
  groupAsDS
  -- Developer

) where

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
import Spark.Core.StructuresInternal

{-| A dataset that has been partitioned according to some given field.
-}
data GroupData key val = GroupData {
  -- The dataset of reference for this group
  _gdRef :: !UntypedDataset,
  -- The columns used to partition the data by keys.
  _gdKey :: !UntypedColumnData,
  -- The columns that contain the values.
  _gdValue :: !UntypedColumnData
}

type LogicalGroupData = Try UntypedGroupData

{-| (developper)

A group data type with no typing information.
-}
type UntypedGroupData = GroupData Cell Cell

type GroupTry a = Either T.Text a

-- A useful type when chaining operations withing groups.
data PipedTrans =
    PipedError !T.Text
  | PipedDataset !UntypedDataset
  | PipedGroup !UntypedGroupData
  deriving (Show)


{-| Performs a logical group of data based on a key.
-}
groupByKey :: (HasCallStack) => Column ref key -> Column ref val -> GroupData key val
groupByKey keys vals = forceRight $ _castGroup (colType keys) (colType vals) =<< _groupByKey (iUntypedColData keys) (iUntypedColData vals)

{-| Transforms the values in a group.
-}
-- This only allows direct transforms, so it is probably valid in all cases.
mapGroup :: GroupData key val -> (forall ref. Column ref val -> Column ref val') -> GroupData key val'
mapGroup g f =
  let c = _unsafeCastColData (_gdValue g)
  -- TODO: this is wrong, an aggregation may have been forced in between.
  in g { _gdValue = iUntypedColData (f c) }

{-| The generalized value transform.

This generalizes mapGroup to allow more complex transforms involving joins,
groups, etc.
-}
-- TODO: this can fail
magGroupGen :: (forall ref. Column ref val -> Dataset val') -> GroupData key val -> GroupData key val'
magGroupGen f g = undefined

{-| Given a group and an aggregation function, aggregates the data.

Note: not all the reduction functions may be used in this case. The analyzer
will fail if the function is not universal.
-}
-- TODO: it should be a try, this can fail
aggKey :: GroupData key val -> (forall ref. Column ref val -> LocalData val') -> Dataset (key, val')
aggKey _ _ = undefined

{-| Creates a group by 'expanding' a value into a potentially large collection.

Note on performance: this function is optimized to work at any scale and may not
be the most efficient when the generated collections are small (a few elements).
-}
-- TODO: it should be a try, this can fail
expand :: Column ref key -> Column ref val -> (LocalData val -> Dataset val') -> GroupData key val'
expand = undefined

{-| Builds groups within groups.

This function allows groups to be constructed from each collections inside a
group.

This function is usually not used directly by the user, but rather as part of
more complex pipelines that may involve multiple levels of nesting.
-}
groupInGroup :: GroupData key val -> (forall ref. Column ref val -> GroupData key' val') -> GroupData (key', key) val'
groupInGroup _ _ = undefined

{-| Reduces a group in group into a single group.
-}
aggGroup :: GroupData (key, key') val -> (forall ref. LocalData key -> Column ref val -> LocalData val') -> GroupData key val
aggGroup _ _ = undefined

{-| Returns the collapsed representation of a grouped dataset, discarding group
information.
-}
groupAsDS :: forall key val. GroupData key val -> Dataset (key, val)
groupAsDS g = pack s where
  c1 = _unsafeCastColData (_gdKey g) :: Column UnknownReference key
  c2 = _unsafeCastColData (_gdValue g) :: Column UnknownReference val
  s = struct (c1, c2) :: Column UnknownReference (key, val)


-- ******** INSTANCES ***********


instance Show (GroupData key val) where
  show gd = T.unpack s where
    s = sformat ("GroupData[key="%sh%", val="%sh%"]") (_gdKey gd) (_gdValue gd)

-- ******** PRIVATE METHODS ********

_mapStructuredTransform :: ColOp -> LogicalGroupData -> GroupTry LogicalGroupData
_mapStructuredTransform = undefined

_mapAggTransform :: AggTransform -> LogicalGroupData -> GroupTry LogicalGroupData
_mapAggTransform = undefined

_pError :: T.Text -> PipedTrans
_pError = PipedError

_unrollTransform :: PipedTrans -> NodeId -> UntypedNode -> PipedTrans
_unrollTransform start nid un | nodeId un == nid = start
_unrollTransform start nid un =
  let op = nodeOp un
      parents = nodeParents un
  in case parents of
    [p] ->
      let pt' = _unrollTransform start nid p in _unrollStep pt' op parents
    l ->
      _pError $ sformat (sh%": operations with multiple parents cannot be used in groups yet.") un

_unrollStep :: PipedTrans -> NodeOp -> [UntypedNode] -> PipedTrans
_unrollStep (PipedGroup g) (NodeReduction at) [_] =
  PipedDataset ds where
    ds = missing "_unrollStep 1"
_unrollStep (PipedGroup g) (NodeStructuredTransform co) [_] =
  PipedGroup g' where g' = missing "_unrollStep 2"
_unrollStep (PipedGroup g) (NodeGroupedReduction co ao) [_] =
  PipedGroup g' where g' = missing "_unrollStep 3"
_unrollStep _ no _ = _pError $ sformat (sh%": Operation not supported") no

_aggKey :: UntypedGroupData -> (UntypedColumnData -> Try UntypedLocalData) -> Try UntypedDataset
_aggKey ugd f =
  let inputDt = unSQLType . colType . _gdValue $ ugd
      p = placeholder inputDt :: UntypedDataset
      start = PipedDataset p
      startNid = nodeId p in do
  uld <- f (_unsafeCastColData (asCol p))
  case _unrollTransform start startNid (untyped uld) of
    PipedError t -> tryError t
    PipedGroup g ->
      -- This is a programming error
      tryError $ sformat ("Expected a dataframe at the output but got a group: "%sh) g
    PipedDataset ds -> pure ds

_unsafeCastColData :: Column ref a -> Column ref' a'
_unsafeCastColData c = c { _cType = _cType c }

{-| Checks that the group can be cast.
-}
_castGroup ::
  SQLType key -> SQLType val -> UntypedGroupData -> Try (GroupData key val)
_castGroup (SQLType keyType) (SQLType valType) ugd =
  let keyType' = unSQLType . colType . _gdKey $ ugd
      valType' = unSQLType . colType . _gdValue $ ugd in
  if keyType == keyType'
  then if valType == valType'
    then
      pure ugd { _gdRef = _gdRef ugd }
    else
      tryError $ sformat ("The value column (of type "%sh%") cannot be cast to type "%sh) valType' valType
  else
    tryError $ sformat ("The value column (of type "%sh%") cannot be cast to type "%sh) keyType' keyType

_groupByKey :: UntypedColumnData -> UntypedColumnData -> LogicalGroupData
_groupByKey keys vals =
  if nodeId (colOrigin keys) == nodeId (colOrigin vals)
  then
    pure GroupData {
      _gdRef = colOrigin keys,
      _gdKey = keys,
      _gdValue = vals
    }
  else
    tryError $ sformat ("The columns have different origin: "%sh%" and "%sh) keys vals
