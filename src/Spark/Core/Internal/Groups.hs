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

import qualified Data.Text as T
import qualified Data.Vector as V
import Formatting

import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.ColumnFunctions(untypedCol, colType, colOp, iUntypedColData, colOrigin, castTypeCol, dropColReference)
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.LocalDataFunctions()
import Spark.Core.Internal.FunctionsInternals
import Spark.Core.Internal.TypesFunctions(tupleType)
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.RowStructures(Cell)
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
-- magGroupGen :: (forall ref. Column ref val -> Dataset val') -> GroupData key val -> GroupData key val'
-- magGroupGen _ _ = undefined

{-| Given a group and an aggregation function, aggregates the data.

Note: not all the reduction functions may be used in this case. The analyzer
will fail if the function is not universal.
-}
-- TODO: it should be a try, this can fail
aggKey :: (HasCallStack) => GroupData key val -> (forall ref. Column ref val -> LocalData val') -> Dataset (key, val')
aggKey gd f =
  let ugd = _untypedGroup gd
      keyt = mapGroupKeys gd colType
      valt = mapGroupValues gd colType
      -- We call the function twice: the first one to recover the type info,
      -- and the second time to perform the unrolling.
      -- TODO we should be able to do it in one pass instead.
      fOut = f $ mapGroupValues gd dropColReference
      valt' = nodeType fOut
      t = tupleType keyt valt'
      f' c = untypedLocalData . f <$> castTypeCol valt c
      tud = _aggKey ugd f'
      g = castType t
      res = g tud
  in forceRight res

{-| Creates a group by 'expanding' a value into a potentially large collection.

Note on performance: this function is optimized to work at any scale and may not
be the most efficient when the generated collections are small (a few elements).
-}
-- TODO: it should be a try, this can fail
-- expand :: Column ref key -> Column ref val -> (LocalData val -> Dataset val') -> GroupData key val'
-- expand = undefined

{-| Builds groups within groups.

This function allows groups to be constructed from each collections inside a
group.

This function is usually not used directly by the user, but rather as part of
more complex pipelines that may involve multiple levels of nesting.
-}
-- groupInGroup :: GroupData key val -> (forall ref. Column ref val -> GroupData key' val') -> GroupData (key', key) val'
-- groupInGroup _ _ = undefined

{-| Reduces a group in group into a single group.
-}
-- aggGroup :: GroupData (key, key') val -> (forall ref. LocalData key -> Column ref val -> LocalData val') -> GroupData key val
-- aggGroup _ _ = undefined

{-| Returns the collapsed representation of a grouped dataset, discarding group
information.
-}
groupAsDS :: forall key val. GroupData key val -> Dataset (key, val)
groupAsDS g = pack s where
  c1 = _unsafeCastColData (_gdKey g) :: Column UnknownReference key
  c2 = _unsafeCastColData (_gdValue g) :: Column UnknownReference val
  s = struct (c1, c2) :: Column UnknownReference (key, val)

mapGroupKeys :: GroupData key val -> (forall ref. Column ref key -> a) -> a
mapGroupKeys gd f =
  f (_unsafeCastColData (_gdKey gd))

mapGroupValues :: GroupData key val -> (forall ref. Column ref val -> a) -> a
mapGroupValues gd f =
  f (_unsafeCastColData (_gdValue gd))

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
_unrollTransform start nid un = case nodeParents un of
    [p] ->
      let pt' = _unrollTransform start nid p in _unrollStep pt' un
    _ ->
      _pError $ sformat (sh%": operations with multiple parents cannot be used in groups yet.") un

_unrollStep :: PipedTrans -> UntypedNode -> PipedTrans
_unrollStep pt un =
  let op = nodeOp un
      dt = unSQLType (nodeType un) in case nodeParents un of
    [p] ->
      case (pt, op) of
        (PipedError e, _) -> PipedError e
        (PipedDataset ds, NodeStructuredTransform _) ->
          -- This is simply dointg a DS -> DS transform.
          -- TODO: this breaks the encapsulation of ComputeNode
          let ds' = updateNode un (\un' -> un' { _cnParents = V.singleton (untyped ds)})
          in PipedDataset ds'
        (PipedGroup g, NodeStructuredTransform co) ->
          _unrollGroupTrans g co
        (PipedGroup g, NodeAggregatorReduction uao) ->
          case uaoInitialOuter uao of
            OpaqueAggTransform x -> _pError $ sformat ("Cannot apply opaque transform in the context of an aggregation: "%sh) x
            InnerAggOp ao ->
              PipedDataset $ _applyAggOp dt ao g
            InnerAggStruct v ->
              PipedDataset $ _applyAggOp dt (AggStruct v) g
        _ -> _pError $ sformat (sh%": Operation not supported with trans="%sh%" and parents="%sh) op pt p
    l -> _pError $ sformat (sh%": expected one parent but got "%sh) un l

_applyAggOp :: (HasCallStack) => DataType -> AggOp -> UntypedGroupData -> UntypedDataset
_applyAggOp dt ao ugd =
  -- Reset the names to make sure there are no collision.
  let c1 = untypedCol (_gdKey ugd) @@ T.unpack "_1"
      c2 = untypedCol (_gdValue ugd) @@ T.unpack "_2"
      s = struct' [c1, c2]
      p = pack1 <$> s
      ds = forceRight p
      ds2 = emptyDataset (NodeGroupedReduction ao) (SQLType dt) `parents` [untyped ds]
  in ds2

_unrollGroupTrans :: UntypedGroupData -> ColOp -> PipedTrans
_unrollGroupTrans ugd co = case _combineColOp (colOp (_gdValue ugd)) co of
  -- TODO: this is ugly, we are loosing the error structure.
  Left x -> _pError $ "_unrollGroupTrans: failure with " <> show' x
  Right co' -> PipedGroup $ ugd { _gdValue = _transformCol co' (_gdValue ugd) }


-- TODO: this should be moved to ColumnFunctions
_transformCol :: ColOp -> UntypedColumnData -> UntypedColumnData
-- TODO: at this point, it should be checked for correctness (the fields
-- being extracted should exist)
_transformCol co ucd = ucd { _cOp = co }

-- Takes a column operation and chain it with another column operation.
_combineColOp :: ColOp -> ColOp -> Try ColOp
_combineColOp _ (x @ (ColLit _ _)) = pure x
_combineColOp x (ColFunction fn v) =
  ColFunction fn <$> sequence (_combineColOp x <$> v)
_combineColOp x (ColExtraction fp) = _extractColOp x (V.toList (unFieldPath fp))
_combineColOp x (ColStruct v) =
  ColStruct <$> sequence (f <$> v) where
    f (TransformField n val) = TransformField n <$> _combineColOp x val

_extractColOp :: ColOp -> [FieldName] -> Try ColOp
_extractColOp x [] = pure x
_extractColOp (ColStruct s) (fn : t) =
  case V.find (\x -> tfName x == fn) s of
    Just (TransformField _ co) ->
      _extractColOp co t
    Nothing ->
      tryError $ sformat ("Expected to find field "%sh%" in structure "%sh) fn s
_extractColOp x y =
  tryError $ sformat ("Cannot perform extraction "%sh%" on column operation "%sh) y x

_aggKey :: UntypedGroupData -> (UntypedColumnData -> Try UntypedLocalData) -> Try UntypedDataset
_aggKey ugd f =
  let inputDt = unSQLType . colType . _gdValue $ ugd
      p = placeholder inputDt :: UntypedDataset
      startNid = nodeId p in do
  uld <- f (_unsafeCastColData (asCol p))
  case _unrollTransform (PipedGroup ugd) startNid (untyped uld) of
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

_untypedGroup :: GroupData key val -> UntypedGroupData
_untypedGroup gd = gd { _gdRef = _gdRef gd }

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
