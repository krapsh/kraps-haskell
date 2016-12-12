{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.Internal.TypesFunctions(
  isNullable,
  iInnerStrictType,
  columnType,
  unsafeCastType,
  intType,
  arrayType,
  arrayType',
  frameTypeFromCol,
  colTypeFromFrame,
  canNull,
  structField,
  structType,
  iSingleField,
  -- cellType,
) where

import Data.Text as T
import qualified Data.Vector as V

import Spark.Core.Internal.TypesStructures
import Spark.Core.StructuresInternal

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


_structFromUnfields :: [(T.Text, DataType)] -> StructType
_structFromUnfields l = StructType . V.fromList $ x where
   x = [StructField (FieldName name) dt | (name, dt) <- l]
