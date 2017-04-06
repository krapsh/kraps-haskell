{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Spark.Core.Internal.TypesGenerics where

import qualified Data.Vector as V
import qualified Data.Text as T
import GHC.Generics
import Formatting

import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.TypesFunctions
import Spark.Core.Internal.Utilities
import Spark.Core.StructuresInternal(FieldName(..), unsafeFieldName)
import Spark.Core.Internal.TypesStructuresRepr(DataTypeRepr, DataTypeElementRepr)

-- The 3rd attempt to get generics conversions.

-- Given a tag on a type, returns the equivalent SQL type.
-- This is the type for a cell, not for a row.
-- TODO(kps) more documentation
buildType :: (HasCallStack, SQLTypeable a) => SQLType a
buildType = _buildType


-- The class of all the types for which the SQL type can be inferred
-- from the Haskell type only.
-- Two notable exceptions are Row and Cell, which are the dynamic types
-- used by Spark.
-- See also buildType on how to use it.
class SQLTypeable a where
  _genericTypeFromValue :: (HasCallStack) => a -> GenericType
  default _genericTypeFromValue :: (HasCallStack, Generic a, GenSQLTypeable (Rep a)) => a -> GenericType
  _genericTypeFromValue x = genTypeFromProxy (from x)

-- Generic SQLTypeable
class GenSQLTypeable f where
  genTypeFromProxy :: (HasCallStack) => f a -> GenericType


-- | The only function that should matter for users in this file.
-- Given a type, returns the SQL representation of this type.
_buildType :: forall a. (HasCallStack, SQLTypeable a) => SQLType a
_buildType =
  let dt = _genericTypeFromValue (undefined :: a)
  in SQLType dt

type GenericType = DataType

instance SQLTypeable Int where
  _genericTypeFromValue _ = StrictType IntType

instance SQLTypeable Double where
  _genericTypeFromValue _ = StrictType DoubleType

instance SQLTypeable T.Text where
  _genericTypeFromValue _ = StrictType StringType

instance SQLTypeable Bool where
  _genericTypeFromValue _ = StrictType BoolType

instance SQLTypeable DataTypeRepr
instance SQLTypeable DataTypeElementRepr

instance SQLTypeable DataType where
  _genericTypeFromValue _ = _genericTypeFromValue (undefined :: DataTypeRepr)


-- instance {-# INCOHERENT #-} SQLTypeable String where
--   _genericTypeFromValue _ = StrictType StringType

instance SQLTypeable a => SQLTypeable (Maybe a) where
  _genericTypeFromValue _ = let SQLType dt = buildType :: (SQLType a) in
    (NullableType . iInnerStrictType) dt

instance {-# OVERLAPPABLE #-} SQLTypeable a => SQLTypeable [a] where
  _genericTypeFromValue _ =
    let SQLType dt = buildType :: (SQLType a) in
      (StrictType . ArrayType) dt


instance forall a1 a2. (
    SQLTypeable a2,
    SQLTypeable a1) => SQLTypeable (a1, a2) where
  _genericTypeFromValue _ =
    let
      SQLType t1 = buildType :: SQLType a1
      SQLType t2 = buildType :: SQLType a2
    in _buildTupleStruct [t1, t2]

_buildTupleStruct :: [GenericType] -> GenericType
_buildTupleStruct dts =
  let fnames = unsafeFieldName . T.pack. ("_" ++) . show <$> ([1..] :: [Int])
      fs = uncurry StructField <$> zip fnames dts
  in StrictType . Struct . StructType $ V.fromList fs

-- instance (SQLTypeable a, SQLTypeable b) => SQLTypeable (a,b) where
--   _genericTypeFromValue _ = _genericTypeFromValue (undefined :: a) ++ _genericTypeFromValue (undefined :: b)

instance (GenSQLTypeable f) => GenSQLTypeable (M1 D c f) where
  genTypeFromProxy m = genTypeFromProxy (unM1 m)

instance (GenSQLTypeable f, Constructor c) => GenSQLTypeable (M1 C c f) where
  genTypeFromProxy m
    | conIsRecord m =
        let x = unM1 m
            dt = genTypeFromProxy x in
          dt
    | otherwise =
        -- It is assumed to be a newtype and we are going to unwrap it
        let !dt1 = genTypeFromProxy (unM1 m)
        in case iSingleField dt1 of
          Just dt -> dt
          Nothing ->
            failure $ sformat ("M1 C "%sh%" dt1="%sh) n dt1
              where n = conName m

-- Selector Metadata
instance (GenSQLTypeable f, Selector c) => GenSQLTypeable (M1 S c f) where
  genTypeFromProxy m =
    let st = genTypeFromProxy (unM1 m)
        n = selName m
        field = StructField { structFieldName = FieldName $ T.pack n, structFieldType = st }
        st2 = StructType (V.singleton field) in
      StrictType $ Struct st2

instance (SQLTypeable a) => GenSQLTypeable (K1 R a) where
  genTypeFromProxy m = _genericTypeFromValue (unK1 m)

-- Sum branch
instance (GenSQLTypeable a, GenSQLTypeable b) => GenSQLTypeable (a :+: b) where
  genTypeFromProxy (L1 x) = genTypeFromProxy x
  genTypeFromProxy (R1 x) = genTypeFromProxy x

-- Product branch
instance (GenSQLTypeable a, GenSQLTypeable b) => GenSQLTypeable (a :*: b) where
  genTypeFromProxy z =
    -- Due to optimizations that I do not understand, the decomposition has to
    -- be done inside the function.
    -- Otherwise, the value (which is undefined) gets to be evaluated, and breaks
    -- the code.
    let (x1 :*: x2) = z
        y1 = genTypeFromProxy x1
        y2 = genTypeFromProxy x2 in case (y1, y2) of
        (StrictType (Struct s1), StrictType (Struct s2)) ->
          (StrictType . Struct) s where
            fs = structFields s1 V.++ structFields s2
            s = StructType fs
        _ -> failure $ sformat ("should not happen: left="%sh%" right="%sh) y1 y2

-- Void branch
instance GenSQLTypeable U1 where
  genTypeFromProxy _ = failure "U1"
