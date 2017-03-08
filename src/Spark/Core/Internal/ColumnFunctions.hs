{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

-- Implements a DSL for extracting some columns from dataframes and datasets.

module Spark.Core.Internal.ColumnFunctions(
  -- Accessors
  colType,
  colOrigin,
  colOp,
  colFieldName,
  -- Standard functions
  broadcast,
  broadcast',
  -- Internal API
  iUntypedColData,
  iEmptyCol,
  -- Developer API (projections)
  -- unsafeStaticProjection,
  dropColReference,
  dropColType,
  extractPathUnsafe,
  colExtraction,
  unsafeProjectCol,
  -- -- Developer API (projection builders)
  -- dynamicProjection,
  -- stringToDynColProj,
  -- pathToDynColProj,
  -- colStaticProjToDynProj,
  -- -- Developer API (projection transformers)
  -- projectDSDyn,
  -- projectDFDyn,
  -- projectDsCol,
  -- projectColCol,
  -- projectColDynCol,
  -- projectDColDCol,
  -- Public functions
  untypedCol,
  colFromObs,
  colFromObs',
  castTypeCol,
  castCol,
  castCol',
  colRef
) where

import qualified Data.Text as T
import qualified Data.Text.Format as TF
import qualified Data.Vector as V
import Data.String(IsString(fromString))
import Data.Text.Lazy(toStrict)
import Data.Maybe(fromMaybe)
import Data.List(find)
import Formatting

import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.TypesStructures
import Spark.Core.Try
import Spark.Core.StructuresInternal
import Spark.Core.Internal.TypesFunctions
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.OpFunctions(prettyShowColOp)
import Spark.Core.Internal.AlgebraStructures
import Spark.Core.Internal.Utilities

-- ********** Public methods ********


{-| The type of a column.
-}
colType :: Column ref a -> SQLType a
colType = SQLType . _cType

{-| Converts a type column to an antyped column.
-}
untypedCol :: Column ref a -> DynColumn
untypedCol = pure . dropColType . dropColReference

{-| Drops the type information, but kees the reference.
-}
dropColType :: Column ref a -> GenericColumn ref
dropColType = _unsafeCastColData

{-| Casts a dynamic column to a statically typed column.

In this case, one must supply the reference (which can be obtained from
another column with colRef, or from a dataset), and a type (which can be
built using the buildType function).
-}
castCol :: ColumnReference ref -> SQLType a -> DynColumn -> Try (Column ref a)
castCol r sqlt dc =
  dc >>= castTypeCol sqlt >>= _checkedCastRefColData r

{-| Casts a dynamic column to a statically typed column, but does not attempt
to enforce a single origin at the type level.

This is useful when building a dataset from a dataframe: the origin information
cannot be conveyed since it is not available in the first place.
-}
castCol' :: SQLType a -> DynColumn -> Try (Column UnknownReference a)
castCol' = castCol ColumnReference


-- | (internal)
castTypeCol :: SQLType b -> ColumnData ref a -> Try (ColumnData ref b)
castTypeCol sqlt cd =
  if unSQLType sqlt == unSQLType (colType cd)
    then pure (_unsafeCastColData cd)
    else tryError $ sformat ("Cannot cast column "%sh%" to type "%sh) cd sqlt

{-| Takes some local data (contained in an observable) and broadacasts it along
a reference column.
-}
broadcast :: LocalData a -> Column ref b -> Column ref a
broadcast ld c = ColumnData {
    _cOrigin = colOrigin c,
    _cType = unSQLType (nodeType ld),
    _cObsJoin = Just (untypedLocalData ld),
    _cOp = BroadcastColFunction,
    _cReferingPath = Nothing
  }

broadcast' :: LocalFrame -> DynColumn -> DynColumn
broadcast' lf dc = do
  ld <- lf
  c <- dc
  return $ broadcast ld c

-- (internal)
colOrigin :: Column ref a -> UntypedDataset
colOrigin = _cOrigin

-- (internal)
colOp :: Column ref a -> ColOp
colOp = _cOp

{-| A tag with the reference of a column.

This is useful when casting dynamic columns to typed columns.
-}
colRef :: Column ref a -> ColumnReference ref
colRef _ = ColumnReference

-- | Takes an observable and makes it available as a column of the same type.
colFromObs :: (HasCallStack) => LocalData a -> Column (LocalData a) a
colFromObs = missing "colFromObs"

-- | Takes a dynamic observable and makes it available as a dynamic column.
colFromObs' :: (HasCallStack) => LocalFrame -> DynColumn
colFromObs' = missing "colFromObs'"

-- | (internal)
colFieldName :: ColumnData ref a -> FieldName
colFieldName c =
  fromMaybe (unsafeFieldName . prettyShowColOp . _cOp $ c)
    (_cReferingPath c)

-- ********* Projection operations ***********


-- ****** Functions that build projections *******


iUntypedColData :: Column ref a -> UntypedColumnData
iUntypedColData = _unsafeCastColData . dropColReference

-- Recasts the column, trusting the user knows that the type is going to be compatible.
_unsafeCastColData :: Column ref a -> Column ref b
_unsafeCastColData c = c { _cType = _cType c }

_checkedCastColData :: SQLType b -> ColumnData ref a -> Try (ColumnData ref b)
_checkedCastColData sqlt cd =
  if unSQLType sqlt == unSQLType (colType cd)
    then pure (_unsafeCastColData cd)
    else tryError $ sformat ("Cannot cast column "%sh%" to type "%sh) cd sqlt

_checkedCastRefColData :: ColumnReference ref2 -> ColumnData ref a -> Try (ColumnData ref2 a)
_checkedCastRefColData _ cd =
  -- TODO: do some dynamic checks on the origin.
  pure $ cd { _cType = _cType cd }



-- Performs the data projection. This is unsafe, it does not check that the
-- field path is valid in this case, nor that the final type is valid either.
unsafeProjectCol :: ColumnData ref a -> FieldPath -> DataType -> ColumnData ref b
unsafeProjectCol cd (FieldPath p) dtTo =
  -- If the column is already a projection, flatten it.
  case colOp cd of
    -- No previous parent on an extraction -> we can safely append to that one.
    ColExtraction (FieldPath p') ->
      cd { _cOp = ColExtraction (FieldPath (p V.++ p')),
           _cType = dtTo}
    _ ->
      -- Extract from the previous column.
      cd { _cOp = ColExtraction (FieldPath p),
          _cType = dtTo}


extractPathUnsafe :: SQLType from -> FieldPath -> Maybe (SQLType to)
extractPathUnsafe sqlt (FieldPath v) = _extractPath0 sqlt (V.toList v)

_extractPath0 :: SQLType from -> [FieldName] -> Maybe (SQLType to)
_extractPath0 sqlt [] = Just (unsafeCastType sqlt)
_extractPath0 sqlt (field : l) = do
  inner <- _extractField sqlt field
  _extractPath0 inner l

_extractField :: SQLType from -> FieldName -> Maybe (SQLType to)
_extractField (SQLType (StrictType (Struct (StructType fields)))) f =
  -- There is probably a way to make it shorter...
  let z = find (\x -> structFieldName x == f) fields in
  SQLType . structFieldType <$> z
_extractField (SQLType (NullableType (Struct (StructType fields)))) f =
  -- There is probably a way to make it shorter...
  let z = find (\x -> structFieldName x == f) fields in
  SQLType . structFieldType <$> z
_extractField _ _ = Nothing

dropColReference :: ColumnData ref a -> ColumnData UnknownReference a
dropColReference c = c {_cOp = _cOp c}

-- | (internal) creates a new column with some empty data
iEmptyCol :: Dataset a -> SQLType b -> FieldPath -> Column a b
iEmptyCol = _emptyColData

-- | (internal) Creates a new column with a dynamic type.
colExtraction :: Dataset a -> DataType -> FieldPath -> DynColumn
colExtraction ds dt fp = Right $ dropColReference $ _emptyColData ds (SQLType dt) fp

-- A new column data structure.
_emptyColData :: Dataset a -> SQLType b -> FieldPath -> ColumnData a b
_emptyColData ds sqlt path = ColumnData {
  _cOrigin = untypedDataset ds,
  _cType = unSQLType sqlt,
  _cObsJoin = Nothing,
  _cOp = ColExtraction path,
  _cReferingPath = Nothing
}

-- Homogeneous operation betweet 2 columns.
_homoColOp2 :: T.Text -> Column ref x -> Column ref x -> Column ref x
_homoColOp2 opName c1 c2 =
  let co = ColFunction opName (V.fromList (colOp <$> [c1, c2]))
  in ColumnData {
      _cOrigin = _cOrigin c1,
      _cType = _cType c1,
      _cObsJoin = Nothing,
      _cOp = co,
      _cReferingPath = Nothing }

_homoColOp2' :: T.Text -> DynColumn -> DynColumn -> DynColumn
_homoColOp2' opName c1' c2' = do
  c1 <- c1'
  c2 <- c2'
  -- TODO check same origin
  return $ _homoColOp2 opName c1 c2

-- ******** Displaying and pretty printing ************

instance forall ref a. Show (Column ref a) where
  show c =
    let
      name = case _cReferingPath c of
        Just fn -> show' fn
        Nothing -> prettyShowColOp . colOp $ c
      txt = fromString "{}{{}}->{}" :: TF.Format
      -- path = T.pack . show . _cReferingPath $ c
      -- no = prettyShowColOp . colOp $ c
      fields = T.pack . show . colType $ c
      nn = unNodeName . nodeName . _cOrigin $ c
    in T.unpack $ toStrict $ TF.format txt (name, fields, nn)

-- *********** Arithmetic operations **********


instance forall a. HomoBinaryOp2 a a a where
  _liftFun = BinaryOpFun id id

instance forall ref a. HomoBinaryOp2 (Column ref a) DynColumn DynColumn where
  _liftFun = BinaryOpFun untypedCol id

instance forall ref a. HomoBinaryOp2 DynColumn (Column ref a) DynColumn where
  _liftFun = BinaryOpFun id untypedCol


instance (Num x) => Num (Column ref x) where
  (+) = _homoColOp2 "sum"
  (*) _ _ = missing "Num (Column x): *"
  abs _ = missing "Num (Column x): abs"
  signum _ = missing "Num (Column x): signum"
  fromInteger _ = missing "Num (Column x): fromInteger"
  negate _ = missing "Num (Column x): negate"

instance Num DynColumn where
  (+) = _homoColOp2' "sum"
  (*) _ _ = missing "Num (DynColumn x): *"
  abs _ = missing "Num (DynColumn x): abs"
  signum _ = missing "Num (DynColumn x): signum"
  fromInteger _ = missing "Num (DynColumn x): fromInteger"
  negate _ = missing "Num (DynColumn x): negate"
