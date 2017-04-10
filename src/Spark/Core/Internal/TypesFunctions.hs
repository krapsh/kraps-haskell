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
  structTypeTuple,
  structTypeTuple',
  tupleType,
  structName,
  iSingleField,
  -- cellType,
) where

import Control.Monad.Except
import qualified Data.List.NonEmpty as N
import Control.Arrow(second)
import Data.Function(on)
import Data.List(sort, nub, sortBy)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Data.Text(Text, intercalate)
import qualified Data.Vector as V
import Formatting


import Spark.Core.Internal.TypesStructures
import Spark.Core.StructuresInternal
import Spark.Core.Internal.RowGenericsFrom(FromSQL(..), TryS)
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
-- All visible operations in Karps use Cell types instead.
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

-- In the case of source introspection, datatypes may be returned.
instance FromSQL DataType where
  _cellToValue = _cellToValue >=> _sDataTypeFromRepr

_sDataTypeFromRepr :: DataTypeRepr -> TryS DataType
_sDataTypeFromRepr (DTR.DataTypeRepr l) = snd <$> _sToTreeRepr l

_sToTreeRepr :: [DataTypeElementRepr] -> TryS (Int, DataType)
_sToTreeRepr [] = throwError $ sformat "_sToTreeRepr: empty list"
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
  let groupst = M.toList g <&> \(h, l') ->
         _sToTreeRepr l' <&> second (StructField (FieldName h))
  groups <- sequence groupst
  checkedGroups <- _packWithIndex groups
  hDtr <- hDtrt
  _decodeLeaf hDtr checkedGroups

_packWithIndex :: (Show t) => [(Int, t)] -> TryS [t]
_packWithIndex l = _check 0 $ sortBy (compare `on` fst) l

-- Checks that all the elements are indexed in order by their value.
-- It works by running a counter along each element and seeing that it is here.
_check :: (Show t) => Int -> [(Int, t)] -> TryS [t]
_check _ [] = pure []
_check n ((n', x):t) =
  if n == n'
  then (x : ) <$> _check (n+1) t
  else
    throwError $ sformat ("_check: could not match arguments at index "%sh%" for argument "%sh) n ((n', x):t)


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
_compatibleTypesStrict DoubleType DoubleType = True
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

{-| Builds a type that is a tuple of all the given types.

Following the Spark and SQL convention, the indexing starts at 1.
-}
structTypeTuple :: N.NonEmpty DataType -> StructType
structTypeTuple dts =
  let numFields = length dts
      rawFieldNames = ("_" <> ) . show' <$> (1 N.:| [2..numFields])
      fieldNames = N.toList $ unsafeFieldName <$> rawFieldNames
      fieldTypes = N.toList dts
      -- Unsafe call, but we know it is going to be all different fields
  in forceRight $ structTypeFromFields (zip fieldNames fieldTypes)

{-| Returns a data type instead (the most common use case)

Note that unlike Spark and SQL, the indexing starts from 0.
 -}
structTypeTuple' :: N.NonEmpty DataType -> DataType
structTypeTuple' = StrictType . Struct . structTypeTuple

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
