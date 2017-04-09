{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- A number of utilities related to data sets and dataframes.

module Spark.Core.Internal.FunctionsInternals(
  DynColPackable,
  StaticColPackable2,
  NameTuple(..),
  TupleEquivalence(..),
  asCol,
  asCol',
  pack1,
  pack,
  pack',
  struct',
  struct,
  -- Developer tools
  checkOrigin,
  projectColFunction,
  projectColFunction',
  projectColFunction2',
  colOpNoBroadcast
) where

import Control.Arrow
import Data.Aeson(toJSON)
import qualified Data.Vector as V
import qualified Data.Map.Strict as M
import qualified Data.List.NonEmpty as N
import qualified Data.Text as T
import Formatting

import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.ColumnFunctions
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.TypesFunctions
import Spark.Core.Internal.LocalDataFunctions
import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.Projections
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.TypesGenerics(SQLTypeable, buildType)
import Spark.Core.StructuresInternal
import Spark.Core.Try

{-| The class of pairs of types that express the fact that some type a can
be converted to a dataset of type b.

This class is only inhabited by some internal types: lists, tuples, etc.
-}
class DynColPackable a where
  -- Returns (possibly) some form of the type a packed into a single column.
  -- This implementation must make sure that the final column is either a
  -- failure or is well-formed (no name duplicates, etc.)
  _packAsColumn :: a -> DynColumn

{-| The class of pairs of types that express the fact that some type a can
be converted to a dataset of type b.

This class is meant to be extended by users to create converters associated
to their data types.
-}
class StaticColPackable2 ref a b | a -> ref where
  _staticPackAsColumn2 :: a -> Column ref b

data NameTuple to = NameTuple [String]

{-| A class that expresses the fact that a certain type (that is well-formed)
is equivalent to a tuple of points.

Useful for auto conversions between tuples of columns and data structures.
-}
class TupleEquivalence to tup | to -> tup where
  tupleFieldNames :: NameTuple to

-- Here is the basic algorithm:
--  - datasets can only contain rows of things
--  - columns and observables contain cells (which may be empty)
--  - a strict struct cell is equivalent to a row
--  - a non-strict or non-struct cell is equivalent to a row with a single item
--  - as a consequence, there is no "row with a unique field". This is equivalent
--    to the element inside the field

-- Invariants to respect in terms of types (not in terms of values)
--   untypedCol . asCol == asCol'
--   pack1 . asCol == asCol . pack1
--   for single columns, pack = Right . pack1

-- The typed function
-- This only works for inner types that are known to the Haskell type system
-- fun :: (SQLTypeable a, HasCallStack) => Column a -> Column a -> Column a
-- fun = undefined

-- The untyped equivalent
-- Each of the inputs can be either a column or a try, and the final outcome is always a try
-- When both types are known to the type system, the 2 calls are equivalent
-- fun' :: (ColumnLike a1, ColumnLike a2, HasCallStack) => a1 -> a2 -> Try DynColumn
-- fun' = undefined

-- | Represents a dataframe as a single column.
asCol :: Dataset a -> Column a a
asCol ds =
  -- Simply recast the dataset as a column.
  -- The empty path indicates that we are wrapping the whole thing.
  iEmptyCol ds (unsafeCastType $ nodeType ds) (FieldPath V.empty)

asCol' :: DataFrame -> DynColumn
asCol' = ((iUntypedColData . asCol) <$>)

-- | Packs a single column into a dataframe.
pack1 :: Column ref a -> Dataset a
pack1 = _pack1

{-| Packs a number of columns into a single dataframe.

This operation is checked for same origin and no duplication of columns.

This function accepts columns, list of columns and tuples of columns (both
typed and untyped).
-}
pack' :: (DynColPackable a) => a -> DataFrame
-- Pack the columns and check that they have the same origin.
pack' z = pack1 <$> _packAsColumn z

{-| Packs a number of columns with the same references into a single dataset.

The type of the dataset must be provided in order to have proper type inference.

TODO: example.
-}
pack :: forall ref a b. (StaticColPackable2 ref a b) => a -> Dataset b
pack z =
  let c = _staticPackAsColumn2 z :: ColumnData ref b
  in pack1 c

{-| Packs a number of columns into a single column (the struct construct).

Columns must have different names, or an error is returned.
-}
struct' :: [DynColumn] -> DynColumn
struct' cols = do
  l <- sequence cols
  let fields = (colFieldName &&& id) <$> l
  _buildStruct fields

{-| Packs a number of columns into a single structure, given a return type.

The field names of the columns are discarded, and replaced by the field names
of the structure.
-}
struct :: forall ref a b. (StaticColPackable2 ref a b) => a -> Column ref b
struct = _staticPackAsColumn2


checkOrigin :: [DynColumn] -> Try [UntypedColumnData]
checkOrigin x = _checkOrigin =<< sequence x

{-| Takes a typed function that operates on columns and projects this function
onto a similar operation for type observables.

This function is not very smart and may throw an error for complex cases such
as broadcasting, joins, etc.
-}
-- TODO: we do not need technically the typeable constraint.
-- It is an additional check.
projectColFunction :: forall x y.
  (HasCallStack, SQLTypeable y, SQLTypeable x) =>
  (forall ref. Column ref x -> Column ref y) -> LocalData x -> LocalData y
projectColFunction f o =
  let o' = untypedLocalData o
      sqltx = buildType :: SQLType x
      sqlty = buildType :: SQLType y
      f' :: UntypedColumnData -> Try UntypedColumnData
      f' x = dropColType . f <$> castTypeCol sqltx x
      o2 = projectColFunctionUntyped (f' =<<) o'
      o3 = castType sqlty =<< o2
  in forceRight o3

projectColFunctionUntyped ::
  (DynColumn -> DynColumn) -> UntypedLocalData -> LocalFrame
projectColFunctionUntyped f obs = do
  -- Create a placeholder dataset and a corresponding column.
  let dt = unSQLType (nodeType obs)
  -- Pass them to the function.
  let no = NodeDistributedLit dt V.empty
  let ds = emptyDataset no (SQLType dt)
  let c = asCol ds
  colRes <- f (pure (dropColType c))
  let dtOut = unSQLType $ colType colRes
  -- This will fail if there is a broadcast.
  co <- _replaceObservables M.empty (colOp colRes)
  let op = NodeStructuredTransform co
  return $ emptyLocalData op (SQLType dtOut)
              `parents` [untyped obs]

{-| Takes a function that operates on columns, and projects this
function onto the same operations for observables.

This is not very smart at the moment and will miss the more
complex operations such as broadcasting, etc.
-}
-- TODO: use for the numerical transforms instead of special stuff.
projectColFunction' ::
    (DynColumn -> DynColumn) ->
    LocalFrame -> LocalFrame
projectColFunction' f obs = projectColFunctionUntyped f =<< obs

projectColFunction2' ::
  (DynColumn -> DynColumn -> DynColumn) ->
  LocalFrame ->
  LocalFrame ->
  LocalFrame
projectColFunction2' f o1' o2' = do
  let f2 :: DynColumn -> DynColumn
      f2 dc = f (dc /- "_1") (dc /- "_2")
  o1 <- o1'
  o2 <- o2'
  let o = iPackTupleObs $ o1 N.:| [o2]
  projectColFunctionUntyped f2 o

colOpNoBroadcast :: GeneralizedColOp -> Try ColOp
colOpNoBroadcast = _replaceObservables M.empty


_checkOrigin :: [UntypedColumnData] -> Try [UntypedColumnData]
_checkOrigin [] = pure []
_checkOrigin l =
  case _columnOrigin l of
    [_] -> pure l
    l' -> tryError $ sformat ("Too many distinct origins: "%sh) l'


instance forall x. (DynColPackable x) => DynColPackable [x] where
  _packAsColumn = struct' . (_packAsColumn <$>)

instance DynColPackable DynColumn where
  _packAsColumn = id

instance forall ref a. DynColPackable (Column ref a) where
  _packAsColumn = pure . iUntypedColData

instance forall z1 z2. (DynColPackable z1, DynColPackable z2) => DynColPackable (z1, z2) where
  _packAsColumn (c1, c2) = struct' [_packAsColumn c1, _packAsColumn c2]

-- ******** Experimental ************
instance forall ref a. StaticColPackable2 ref (Column ref a) a where
  _staticPackAsColumn2 = id

-- Tuples are equivalent to tuples
instance forall a1 a2. TupleEquivalence (a1, a2) (a1, a2) where
  tupleFieldNames = NameTuple ["_1", "_2"]


-- The equations that bind column packable stuff through their tuple equivalents
instance forall ref b a1 a2 z1 z2. (
          TupleEquivalence b (a1, a2),
          StaticColPackable2 ref z1 a1,
          StaticColPackable2 ref z2 a2) =>
  StaticColPackable2 ref (z1, z2) b where
    _staticPackAsColumn2 (c1, c2) =
      let
        x1 = iUntypedColData (_staticPackAsColumn2 c1 :: Column ref a1)
        x2 = iUntypedColData (_staticPackAsColumn2 c2 :: Column ref a2)
        names = tupleFieldNames :: NameTuple b
      in _unsafeBuildStruct [x1, x2] names

instance forall ref b a1 a2 a3 z1 z2 z3. (
          TupleEquivalence b (a1, a2, a3),
          StaticColPackable2 ref z1 a1,
          StaticColPackable2 ref z2 a2,
          StaticColPackable2 ref z3 a3) =>
  StaticColPackable2 ref (z1, z2, z3) b where
    _staticPackAsColumn2 (c1, c2, c3) =
      let
        x1 = iUntypedColData (_staticPackAsColumn2 c1 :: Column ref a1)
        x2 = iUntypedColData (_staticPackAsColumn2 c2 :: Column ref a2)
        x3 = iUntypedColData (_staticPackAsColumn2 c3 :: Column ref a3)
        names = tupleFieldNames :: NameTuple b
      in _unsafeBuildStruct [x1, x2, x3] names



_unsafeBuildStruct :: [UntypedColumnData] -> NameTuple x -> Column ref x
_unsafeBuildStruct cols (NameTuple names) =
  if length cols /= length names
    then failure $ sformat ("The number of columns and names differs:"%sh%" and "%sh) cols names
    else
      let fnames = unsafeFieldName . T.pack <$> names
          uc = _buildStruct (fnames `zip` cols)
          z = forceRight uc
      in z { _cOp = _cOp z }

_buildTuple :: [UntypedColumnData] -> Try UntypedColumnData
_buildTuple l = _buildStruct (zip names l) where
  names = (:[]) . unsafeFieldName . ("_" <> ) . show' $ [0..(length l)]

_buildStruct :: [(FieldName, UntypedColumnData)] -> Try UntypedColumnData
_buildStruct cols = do
  let fields = GenColStruct $ (uncurry GeneralizedTransField . (fst &&& colOp . snd)) <$> V.fromList cols
  st <- structTypeFromFields $ (fst &&& unSQLType . colType . snd) <$> cols
  let name = structName st
  case _columnOrigin (snd <$> cols) of
    [ds] ->
      pure ColumnData {
                  _cOrigin = ds,
                  _cType = StrictType (Struct st),
                  _cOp = fields,
                  _cReferingPath = Just $ unsafeFieldName name
                }
    l -> tryError $ sformat ("_buildStruct: Too many distinct origins: "%sh) l

_columnOrigin :: [UntypedColumnData] -> [UntypedDataset]
_columnOrigin l =
  let
    groups = myGroupBy' (nodeId . colOrigin) l
  in (colOrigin . head . snd) <$> groups

-- The packing algorithm
-- It eliminates the broadcast variables into joins and then wraps the
-- remaining transform into structured transform.
-- TODO: the data structure and the algorithms use unsafe operations
-- It should be transfromed to safe operations eventually.
_pack1 :: (HasCallStack) => Column ref a -> Dataset a
_pack1 ucd =
  let gco = colOp ucd
      ulds = _collectObs gco
  in case ulds of
    [] -> let co = forceRight $ colOpNoBroadcast gco in
       _packCol1 ucd co
    (h : t) -> forceRight $ _packCol1WithObs ucd (h N.:| t)

_packCol1WithObs :: Column ref a -> N.NonEmpty UntypedLocalData -> Try (Dataset a)
_packCol1WithObs c ulds = do
  let packedObs = iPackTupleObs ulds
  -- Retrieve the field names in the pack structure.
  let st = structTypeTuple (unSQLType . nodeType <$> ulds)
  let names = V.toList $ structFieldName <$> structFields st
  let paths = FieldPath . V.fromList . (unsafeFieldName "_2" : ) . (:[]) <$> names
  let m = M.fromList ((nodeId <$> N.toList ulds) `zip` paths)
  let joined = broadcastPair (colOrigin c) packedObs
  co <- _replaceObservables m (colOp c)
  let no = NodeStructuredTransform co
  let f = emptyDataset no (colType c) `parents` [untyped joined]
  return f


_replaceObservables :: M.Map NodeId FieldPath -> GeneralizedColOp -> Try ColOp
-- Special case for when there is nothing in the dictionary
_replaceObservables m (GenColExtraction fp) | M.null m = pure $ ColExtraction fp
_replaceObservables _ (GenColExtraction (FieldPath v)) =
  -- It is a normal extraction, prepend the suffix of the data structure.
  pure (ColExtraction (FieldPath v')) where
    v' = V.cons (unsafeFieldName "_1") v
_replaceObservables _ (GenColLit dt c) = pure (ColLit dt (toJSON c))
_replaceObservables m (GenColFunction n v) =
  ColFunction n <$> sequence (_replaceObservables m <$> v)
_replaceObservables m (GenColStruct v) = ColStruct <$> sequence (_replaceField m <$> v)
_replaceObservables m (BroadcastColOp uld) =
   case M.lookup (nodeId uld) m of
     Just p -> pure $ ColExtraction p
     Nothing -> tryError $ "_replaceObservables: error: missing key " <> show' uld <> " in " <> show' m

_replaceField :: M.Map NodeId FieldPath -> GeneralizedTransField -> Try TransformField
_replaceField m (GeneralizedTransField n v) = TransformField n <$> _replaceObservables m v

-- Unconditionally packs the column into a dataset.
_packCol1 :: Column ref a -> ColOp -> Dataset a
-- Special case for column operations that are no-ops: return the dataset itself.
_packCol1 c (ColExtraction (FieldPath v)) | V.null v =
  -- TODO: we should not need to force this operation.
  forceRight $ castType (colType c) (colOrigin c)
_packCol1 c op =
  emptyDataset (NodeStructuredTransform op) (colType c)
      `parents` [untyped (colOrigin c)]

_collectObs :: GeneralizedColOp -> [UntypedLocalData]
_collectObs (GenColFunction _ v) = concat (_collectObs <$> V.toList v)
_collectObs (BroadcastColOp uld) = [uld]
_collectObs (GenColStruct v) = concat (_collectObs . gtfValue <$> V.toList v)
_collectObs _ = [] -- Anything else has no broadcast info.
