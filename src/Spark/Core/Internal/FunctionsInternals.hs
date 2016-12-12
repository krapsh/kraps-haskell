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
  pack1,
  pack,
  pack',
  struct',
  struct,
) where

import Control.Arrow
import qualified Data.Vector as V
import qualified Data.Text as T
import Data.List(sort, nub)
import Formatting

import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.ColumnFunctions
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.TypesGenerics
import Spark.Core.Internal.TypesFunctions
import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.OpStructures
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
asCol :: (HasCallStack) => Dataset a -> Column a a
asCol ds =
  -- Simply recast the dataset as a column.
  -- The empty path indicates that we are wrapping the whole thing.
  iEmptyCol ds (unsafeCastType $ nodeType ds) (FieldPath V.empty)

-- | Packs a single column into a dataframe.
pack1 :: (HasCallStack) => Column ref a -> Dataset a
pack1 c =
  emptyDataset (NodeStructuredTransform (colOp c)) (colType c)
      `parents` [untyped (colOrigin c)]

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
pack :: forall ref a b. (StaticColPackable2 ref a b, HasCallStack) => a -> Dataset b
pack z =
  let c = _staticPackAsColumn2 z :: ColumnData ref b
  in pack1 c

{-| Packs a number of columns into a single column (the struct construct).

Columns must have different names, or an error is returned.
-}
struct' :: (HasCallStack) => [DynColumn] -> DynColumn
struct' cols = do
  l <- sequence cols
  let fields = (colFieldName &&& id) <$> l
  _buildStruct fields

{-| Packs a number of columns into a single structure, given a return type.

The field names of the columns are discarded, and replaced by the field names
of the structure.
-}
struct :: forall ref a b. (StaticColPackable2 ref a b, HasCallStack) => a -> Column ref b
struct = _staticPackAsColumn2


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
          SQLTypeable b,
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
          SQLTypeable b,
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

_unsafeBuildStruct :: (HasCallStack, SQLTypeable x) =>
  [UntypedColumnData] -> NameTuple x -> Column ref x
_unsafeBuildStruct cols (NameTuple names) =
  if length cols /= length names
    then failure $ sformat ("The number of columns and names differs:"%sh%" and "%sh) cols names
    else
      let fnames = unsafeFieldName . T.pack <$> names
          uc = _buildStruct (fnames `zip` cols)
          z = forceRight uc
      in z { _cOp = _cOp z }


_buildStruct :: [(FieldName, UntypedColumnData)] -> Try UntypedColumnData
_buildStruct [] = tryError "You cannot build an empty structure"
_buildStruct ((hfn, hcol):t) =
  let cols = ((hfn, hcol):t)
      cols' = V.fromList cols
      fields = ColStruct $ (uncurry TransformField .(fst &&& colOp . snd)) <$> cols'
      ct = StructType $ (uncurry StructField . (fst &&& unSQLType . colType . snd)) <$> cols'
      name = "struct(" <> T.intercalate "," (unFieldName . fst <$> cols) <> ")"
      names = fst <$> cols
      numNames = length names
      numDistincts = length . nub $ names
      origins = _columnOrigin (snd <$> cols)
  in case (origins, numNames == numDistincts) of
    ([_], True) ->
        pure ColumnData {
                    _cOrigin = _cOrigin hcol,
                    _cType = StrictType $ Struct ct,
                    _cOp = fields,
                    _cReferingPath = Just $ unsafeFieldName name
                  }
    (l, True) -> tryError $ sformat ("Too many distinct origins: "%sh) l
    (_, False) -> tryError $ sformat ("Duplicate field names when building the struct: "%sh) (sort names)

_columnOrigin :: [UntypedColumnData] -> [UntypedDataset]
_columnOrigin l =
  let
    groups = myGroupBy' (nodeId . colOrigin) l
  in (colOrigin . head . snd) <$> groups
