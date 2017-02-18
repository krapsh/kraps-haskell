{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Spark.Core.Internal.TypesFunctions(
  isNullable,
  iInnerStrictType,
  columnType,
  unsafeCastType,
  intType,
  arrayType,
  compatibleTypes,
  arrayType',
  frameTypeFromCol,
  colTypeFromFrame,
  canNull,
  structField,
  structType,
  structTypeFromFields,
  tupleType,
  structName,
  iSingleField,
  -- cellType,
) where

import qualified Data.Text as T
import Data.List(sort, nub, sortBy)
import Control.Monad((>=>))
import Data.Function(on)
import Debug.Trace(trace)
import qualified Data.Vector as V
import Data.Text(Text, intercalate)
import qualified Data.Map.Strict as M
import Control.Monad.Except
import Formatting


import Spark.Core.Internal.TypesStructures
import Spark.Core.StructuresInternal
import Spark.Core.Internal.RowGenericsFrom(FromSQL(..), TryS)
import Spark.Core.Internal.RowStructures
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.TypesStructuresRepr(DataTypeRepr, DataTypeElementRepr)
import qualified Spark.Core.Internal.TypesStructuresRepr as DTR
import Spark.Core.Try

-- Performs a cast of the type.
-- This may throw an error if the required type b is not
-- compatible with the type embedded in a.
unsafeCastType :: SQLType a -> SQLType b
-- TODO add more error checking here.
unsafeCastType (SQLType dt) = SQLType dt


-- Given a sql type tag, returns the equivalent data type for a column or a blob
-- (internal)
columnType :: SQLType a -> DataType
columnType (SQLType dt) = dt

-- (internal)
isNullable :: DataType -> Bool
isNullable (StrictType _) = False
isNullable (NullableType _) = True


-- *** Creation of data types ***


-- Takes a data type (assumed to be that of a column or cell) and returns the
-- corresponding dataset type.
-- This should only be used when talking to Spark.
-- All visible operations in Krapsh use Cell types instead.
-- TODO should it use value or _1? Both seem to be used in Spark.
frameTypeFromCol :: DataType -> StructType
frameTypeFromCol (StrictType (Struct struct)) = struct
frameTypeFromCol dt = _structFromUnfields [("value", dt)]

-- Given the structural type for a dataframe or a dataset, returns the
-- equivalent column type.
colTypeFromFrame :: StructType -> DataType
colTypeFromFrame st @ (StructType fs) = case V.toList fs of
  [StructField {
    structFieldName = fname,
    structFieldType = (StrictType dt)}] | fname == "value" ->
      StrictType dt
  _ -> StrictType (Struct st)


-- The strict int type

compatibleTypes :: DataType -> DataType -> Bool
compatibleTypes (StrictType sdt) (StrictType sdt') = _compatibleTypesStrict sdt sdt'
compatibleTypes (NullableType sdt) (NullableType sdt') = _compatibleTypesStrict sdt sdt'
compatibleTypes _ _ = False

-- ***** INSTANCES *****

_x :: Int -> Cell -> Cell
_x 0 x = x
_x n (RowArray v) = _x (n-1) (V.head v)

-- In the case of source introspection, datatypes may be returned.
instance FromSQL DataType where
  _cellToValue c = trace ("FromSQL DataType: c=" ++ show c) $
    let x = traceHint "FromSQL DataType: x:" $ _x 2 c
        x1t = traceHint ("FromSQL DataType: x1t:") $ _cellToValue x :: TryS DataTypeElementRepr
    in do
      x1 <- x1t
      (_, x2) <- _sToTreeRepr [x1]
      return x2
  -- _cellToValue = _cellToValue >=> _sDataTypeFromRepr

_sDataTypeFromRepr :: DataTypeRepr -> TryS DataType
_sDataTypeFromRepr (DTR.DataTypeRepr l) = snd <$> _sToTreeRepr l

_sToTreeRepr :: [DataTypeElementRepr] -> TryS (Int, DataType)
_sToTreeRepr [] = throwError $ sformat ("_sToTreeRepr: empty list")
_sToTreeRepr [dtr] | null (DTR.fieldPath dtr) =
  -- We are at a leaf, decode the leaf
  _decodeLeaf dtr []
_sToTreeRepr l = do
  let f dtr = case DTR.fieldPath dtr of
                [] -> []
                (h : t) -> [(h, dtr')] where dtr' = dtr { DTR.fieldPath = t }
  let hDtrt = case filter (null . DTR.fieldPath) l of
          [dtr] -> pure dtr
          _ ->
            throwError $ sformat ("_decodeList: invalid top with "%sh) l
  let withHeads = concatMap f l
  let g = myGroupBy withHeads
  let groupst = (M.toList g) <&> \(h, l') ->
         _sToTreeRepr l' <&> \(idx, dt) -> (idx, StructField (FieldName h) dt)
  groups <- sequence groupst
  checkedGroups <- _packWithIndex groups
  hDtr <- hDtrt
  _decodeLeaf hDtr checkedGroups

_packWithIndex :: (Show t) => [(Int, t)] -> TryS [t]
_packWithIndex l = _check $ sortBy (compare `on` fst) l

_check :: (Show t) => [(Int, t)] -> TryS [t]
_check [] = pure []
_check [(0, x)] = pure [x]
_check [(p, x)] = throwError $ sformat ("_check: should be zero "%sh) (p, x)
_check ((idx1, x1) : (idx2, x2) : t) =
  if idx1 == idx2 + 1
  then (x1 : ) <$> _check ((idx2, x2) : t)
  else
    throwError $ sformat ("_check: could not match arguments for "%sh) ((idx1, x1) : (idx2, x2) : t)


_decodeLeaf :: DataTypeElementRepr -> [StructField] -> TryS (Int, DataType)
_decodeLeaf dtr l = _decodeLeafStrict dtr l <&> \sdt ->
  if DTR.isNullable dtr
  then (DTR.fieldIndex dtr, NullableType sdt)
  else (DTR.fieldIndex dtr, StrictType sdt)

_decodeLeafStrict :: DataTypeElementRepr -> [StructField] -> TryS StrictDataType
-- The array type
_decodeLeafStrict dtr [sf] | DTR.typeId dtr == 11 =
  pure $ ArrayType (structFieldType sf)
-- Structure types
_decodeLeafStrict dtr l | DTR.typeId dtr == 10 =
  pure . Struct . StructType . V.fromList $ l
_decodeLeafStrict dtr [] =  case DTR.typeId dtr of
        0 -> pure IntType
        1 -> pure StringType
        2 -> pure BoolType
        n -> throwError $ sformat ("_decodeLeafStrict: unknown type magic id "%sh) n
_decodeLeafStrict dtr l =
  throwError $ sformat ("_decodeLeafStrict: cannot interpret dtr="%sh%" and fields="%sh) dtr l

_compatibleTypesStrict :: StrictDataType -> StrictDataType -> Bool
_compatibleTypesStrict IntType IntType = True
_compatibleTypesStrict StringType StringType = True
_compatibleTypesStrict (ArrayType et) (ArrayType et') = compatibleTypes et et'
_compatibleTypesStrict (Struct (StructType v)) (Struct (StructType v')) =
  (length v == length v') &&
    and (V.zipWith compatibleTypes (structFieldType <$> v) (structFieldType <$> v'))
_compatibleTypesStrict _ _ = False

tupleType :: SQLType a -> SQLType b -> SQLType (a, b)
tupleType (SQLType dt1) (SQLType dt2) =
  SQLType $ structType [structField "_1" dt1, structField "_2" dt2]

intType :: DataType
intType = StrictType IntType

-- a string
structField :: T.Text -> DataType -> StructField
structField txt = StructField (FieldName txt)

-- The strict structure type
structType :: [StructField] -> DataType
structType = StrictType . Struct . StructType . V.fromList

-- The strict array type
arrayType' :: DataType -> DataType
arrayType' = StrictType . ArrayType

-- Returns the equivalent data type that may be nulled.
canNull :: DataType -> DataType
canNull = NullableType . iInnerStrictType

-- Given a type, returns the corresponding array type.
-- This is preferred to using directly buildType, as it may encounter some
-- overlapping instances.
arrayType :: SQLType a -> SQLType [a]
arrayType (SQLType dt) = SQLType (arrayType' dt)

iInnerStrictType :: DataType -> StrictDataType
iInnerStrictType (StrictType st) = st
iInnerStrictType (NullableType st) = st

iSingleField :: DataType -> Maybe DataType
iSingleField (StrictType (Struct (StructType fields))) = case V.toList fields of
  [StructField _ dt] -> Just dt
  _ -> Nothing
iSingleField _ = Nothing


structName :: StructType -> Text
structName (StructType fields) =
  "struct(" <> intercalate "," (unFieldName . structFieldName <$> V.toList fields) <> ")"

structTypeFromFields :: [(FieldName, DataType)] -> Try StructType
structTypeFromFields [] = tryError "You cannot build an empty structure"
structTypeFromFields ((hfn, hdt):t) =
  let fs = (hfn, hdt) : t
      ct = StructType $ uncurry StructField <$> V.fromList fs
      names = fst <$> fs
      numNames = length names
      numDistincts = length . nub $ names
  in if numNames == numDistincts
    then return ct
    else tryError $ sformat ("Duplicate field names when building the struct: "%sh) (sort names)




_structFromUnfields :: [(T.Text, DataType)] -> StructType
_structFromUnfields l = StructType . V.fromList $ x where
   x = [StructField (FieldName name) dt | (name, dt) <- l]
