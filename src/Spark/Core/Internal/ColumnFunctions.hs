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
  -- Internal API
  iUntypedColData,
  iEmptyCol,
  -- Developer API (projections)
  unsafeStaticProjection,
  dynamicProjection,
  -- Public functions
  untypedCol,
  colFromObs,
  colFromObs',
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

{-| Lets the users define their own static projections.

Throws an error if the type cannot be found, so should be used with caution.

String has to be used because of type inferrence issues
-}
unsafeStaticProjection :: forall from to. (HasCallStack) =>
  SQLType from     -- ^ The start type
  -> String        -- ^ The name of a field assumed to be found in the start type.
                   --   This only has to be valid for Spark purposes, not
                   --   internal Haskell representation.
  -> StaticColProjection from to
unsafeStaticProjection sqlt field =
  let
    f = forceRight . fieldPath . T.pack $ field
    sqlt' = fromMaybe
      (failure $ sformat ("unsafeStaticProjection: Cannot find the field "%sh%" in type "%sh) field sqlt)
      (_extractPath sqlt f)
  in StaticColProjection (sqlt, f, sqlt')

-- Returns a projection from a path (even if invalid data)
dynamicProjection :: String -> DynamicColProjection
dynamicProjection txt = case fieldPath (T.pack txt) of
  Left msg -> DynamicColProjection $ \_ ->
    tryError $ sformat ("dynamicProjection: invalid syntax for path "%shown%": "%shown) txt msg
  Right fpath -> _dynamicProjection fpath

{-| The type of a column.
-}
colType :: Column ref a -> SQLType a
colType = SQLType . _cType

{-| Converts a type column to an antyped column.
-}
untypedCol :: Column ref a -> DynColumn
untypedCol = pure . _unsafeCastColData . _dropReference

{-| Casts a dynamic column to a statically typed column.

In this case, one must supply the reference (which can be obtained from
another column with colRef, or from a dataset), and a type (which can be
built using the buildType function).
-}
castCol :: ColumnReference ref -> SQLType a -> DynColumn -> Try (Column ref a)
castCol r sqlt dc =
  dc >>= _checkedCastColData sqlt >>= _checkedCastRefColData r

{-| Casts a dynamic column to a statically typed column, but does not attempt
to enforce a single origin at the type level.

This is useful when building a dataset from a dataframe: the origin information
cannot be conveyed since it is not available in the first place.
-}
castCol' :: SQLType a -> DynColumn -> Try (Column UnknownReference a)
castCol' = castCol ColumnReference

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

-- dataset -> static projection -> column
instance forall a to. Projection (Dataset a) (StaticColProjection a to) (Column a to) where
  _performProjection = _project

-- dataset -> dynamic projection -> DynColumn
instance forall a. Projection (Dataset a) DynamicColProjection DynColumn where
  _performProjection = _projectDyn

-- dataset -> string -> DynColumn
instance forall a . Projection (Dataset a) String DynColumn where
  _performProjection ds s = _projectDyn ds (_strToDynProj s)

-- dataframe -> dynamic projection -> dyncolumn
instance Projection DataFrame DynamicColProjection DynColumn where
  _performProjection = _projectDF

-- dataframe -> static projection -> dyncolumn
-- This is a relaxation as we could return Try (Column to) intead.
-- It makes more sense from an API perspective to just return a dynamic result.
instance forall a to. Projection DataFrame (StaticColProjection a to) DynColumn where
  _performProjection df proj = _projectDF df (_colStaticProjToDynProj proj)

-- dataframe -> string -> dyncolumn
instance Projection DataFrame String DynColumn where
  _performProjection df s = _projectDF df (_strToDynProj s)

-- column -> static projection -> column
instance forall ref a to. Projection (Column ref a) (StaticColProjection a to) (Column ref to) where
  _performProjection = _projectCol


-- dyncolumn -> dynamic projection -> dyncolumn
instance Projection DynColumn DynamicColProjection DynColumn where
  _performProjection = _projectDynCol

instance forall a to. Projection DynColumn (StaticColProjection a to) DynColumn where
  _performProjection dc proj = _projectDynCol dc (_colStaticProjToDynProj proj)

-- dyncolumn -> string -> dyncolumn
instance Projection DynColumn String DynColumn where
  _performProjection dc s = _performProjection dc (_strToDynProj s)

_strToDynProj :: String -> DynamicColProjection
_strToDynProj s =
  let
    fun dt =
      case fieldPath (T.pack s) of
        Right fp -> _dynProjTry (_dynamicProjection fp) dt
        Left msg -> tryError (T.pack msg)
  in DynamicColProjection fun

-- | Converts a static project to a dynamic projector.
_colStaticProjToDynProj :: forall from to. StaticColProjection from to -> DynamicColProjection
_colStaticProjToDynProj (StaticColProjection (SQLType dtFrom, fp, SQLType dtTo)) =
  DynamicColProjection $ \dt ->
    -- TODO factorize this as a projection on types.
    if dt /= dtFrom then
      tryError $ sformat ("Cannot convert type "%shown%" into type "%shown) dt dtFrom
    else pure (fp, dtTo)

iUntypedColData :: Column ref a -> UntypedColumnData
iUntypedColData = _unsafeCastColData . _dropReference

-- Recasts the column, trusting the user knows that the type is going to be compatible.
_unsafeCastColData :: Column ref a -> Column ref b
_unsafeCastColData c = c { _cType = _cType c }

_checkedCastColData :: SQLType b -> ColumnData ref a -> Try (ColumnData ref b)
_checkedCastColData sqlt cd =
  if (unSQLType sqlt) == (unSQLType (colType cd))
    then pure (_unsafeCastColData cd)
    else tryError $ sformat ("Cannot cast column "%sh%" to type "%sh) cd sqlt

_checkedCastRefColData :: ColumnReference ref2 -> ColumnData ref a -> Try (ColumnData ref2 a)
_checkedCastRefColData _ cd =
  -- TODO: do some dynamic checks on the origin.
  pure $ cd { _cType = _cType cd }

_dynamicProjection :: FieldPath -> DynamicColProjection
_dynamicProjection fpath =
  let
    fun dt = case _extractPath (SQLType dt) fpath of
        Just (SQLType dt') -> pure (fpath, dt') -- TODO(kps) I have a doubt
        Nothing ->
          tryError $ sformat ("unsafeStaticProjection: Cannot find the field "%shown%" in type "%shown) fpath dt
   in DynamicColProjection fun

-- TODO: take a compute node instead
_projectDyn :: Dataset from -> DynamicColProjection -> DynColumn
_projectDyn ds proj = do
  (p, dt) <- _dynProjTry proj (unSQLType . nodeType $ ds)
  _emptyDynCol ds dt p

_projectDF :: DataFrame -> DynamicColProjection -> DynColumn
_projectDF df proj = do
  node <- df
  _projectDyn node proj

_project :: Dataset from -> StaticColProjection from to -> Column from to
_project ds proj = let (_, p, sqlt) = _staticProj proj in
  iEmptyCol ds sqlt p

_projectCol :: Column ref from -> StaticColProjection from to -> Column ref to
_projectCol c (StaticColProjection (_, fp, SQLType dt)) =
  _projectColData0 c fp dt

-- Performs the data projection. This is unsafe, it does not check that the
-- field path is valid in this case, nor that the final type is valid either.
_projectColData0 :: ColumnData ref a -> FieldPath -> DataType -> ColumnData ref b
_projectColData0 cd (FieldPath p) dtTo =
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

_projectDynColData :: ColumnData ref a -> DynamicColProjection -> DynColumn
_projectDynColData cd proj =
  _dynProjTry proj (_cType cd) <&> uncurry (_projectColData0 . _dropReference $ cd)

_projectDynCol :: DynColumn -> DynamicColProjection -> DynColumn
_projectDynCol c proj = do
  cd <- c
  _projectDynColData cd proj

_extractPath :: SQLType from -> FieldPath -> Maybe (SQLType to)
_extractPath sqlt (FieldPath v) = _extractPath0 sqlt (V.toList v)

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

_dropReference :: ColumnData ref a -> ColumnData UnknownReference a
_dropReference c = c {_cOp = _cOp c}

-- | (internal) creates a new column with some empty data
iEmptyCol :: Dataset a -> SQLType b -> FieldPath -> Column a b
iEmptyCol = _emptyColData

-- | (internal) Creates a new column with a dynamic type.
_emptyDynCol :: Dataset a -> DataType -> FieldPath -> DynColumn
_emptyDynCol ds dt fp = Right $ _dropReference $ _emptyColData ds (SQLType dt) fp

-- A new column data structure.
_emptyColData :: Dataset a -> SQLType b -> FieldPath -> ColumnData a b
_emptyColData ds sqlt path = ColumnData {
  _cOrigin = untypedDataset ds,
  _cType = unSQLType sqlt,
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

-- *********** Renaming instances **************

instance forall ref a. CanRename (ColumnData ref a) FieldName where
  c @@ fn = c { _cReferingPath = Just fn }


instance forall ref a. CanRename (Column ref a) String where
  c @@ str = case fieldName (T.pack str) of
    Right fp -> c @@ fp
    Left msg ->
      -- The syntax check here is pretty lenient, so it fails, it has
      -- some good reasons. We stop here.
      failure $ sformat ("Could not make a field path out of string "%shown%" for column "%shown%":"%shown) str c msg

instance CanRename DynColumn FieldName where
  (Right cd) @@ fn = Right (cd @@ fn)
  -- TODO better error handling
  x @@ _ = x

instance CanRename DynColumn String where
  -- An error could happen when making a path out of a string.
  (Right cd) @@ str = case fieldName (T.pack str) of
    Right fp -> Right $ cd @@ fp
    Left msg ->
      -- The syntax check here is pretty lenient, so it fails, it has
      -- some good reasons. We stop here.
      tryError $ sformat ("Could not make a field path out of string "%shown%" for column "%shown%":"%shown) str cd msg
  -- TODO better error handling
  x @@ _ = x

-- *********** Arithmetic operations **********


instance forall a. HomoBinaryOp2 a a a where
  _liftFun f = BinaryOpFun id id f

instance forall ref a. HomoBinaryOp2 (Column ref a) DynColumn DynColumn where
  _liftFun f = BinaryOpFun untypedCol id f

instance forall ref a. HomoBinaryOp2 DynColumn (Column ref a) DynColumn where
  _liftFun f = BinaryOpFun id untypedCol f


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
