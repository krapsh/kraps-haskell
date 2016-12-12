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
import Data.Text(Text, pack)
import Data.Proxy
import GHC.Generics
import Formatting
import Debug.Trace

import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.TypesFunctions
import Spark.Core.Internal.Utilities
import Spark.Core.StructuresInternal(FieldName(..), unsafeFieldName)

-- The 3rd attempt to get generics conversions.

-- Given a tag on a type, returns the equivalent SQL type.
-- This is the type for a cell, not for a row.
-- TODO(kps) more documentation
buildType :: (SQLTypeable a) => SQLType a
buildType = _buildType


-- The class of all the types for which the SQL type can be inferred
-- from the Haskell type only.
-- Two notable exceptions are Row and Cell, which are the dynamic types
-- used by Spark.
-- See also buildType on how to use it.
class SQLTypeable a where
  _genericTypeFromValue :: a -> GenericType
  default _genericTypeFromValue :: (Generic a, GenSQLTypeable (Rep a)) => a -> GenericType
  _genericTypeFromValue _ = genBuildType (Proxy :: Proxy a)

  -- | The only function that should matter for users in this file.
  -- Given a type, returns the SQL representation of this type.
  _buildType :: SQLType a
  _buildType =
    let !dt = _genericTypeFromValue (undefined :: a)
        SQLType u = dt in SQLType u

-- These a private types that should not be used elsewhere.
data GenericRow
type GenericType = SQLType GenericRow


-- Generic building type.
genBuildType :: forall a. (Generic a, GenSQLTypeable (Rep a)) => Proxy a -> GenericType
genBuildType _ = genTypeFromProxy (Proxy :: Proxy (Rep a))


instance SQLTypeable Int where
  _genericTypeFromValue _ = SQLType (StrictType IntType)

instance SQLTypeable Text where
  _genericTypeFromValue _ = SQLType (StrictType StringType)

instance {-# INCOHERENT #-} SQLTypeable String where
  _genericTypeFromValue _ = SQLType (StrictType StringType)

instance SQLTypeable a => SQLTypeable (Maybe a) where
  _genericTypeFromValue _ = let SQLType dt = buildType :: (SQLType a) in
    (SQLType . NullableType . iInnerStrictType) dt

instance {-# OVERLAPPABLE #-} SQLTypeable a => SQLTypeable [a] where
  _genericTypeFromValue _ =
    let SQLType dt = buildType :: (SQLType a) in
      (SQLType . StrictType . ArrayType) dt

instance forall a1 a2. (
    SQLTypeable a2,
    SQLTypeable a1) => SQLTypeable (a1, a2) where
  _genericTypeFromValue _ =
    let
      SQLType t1 = buildType :: SQLType a1
      SQLType t2 = buildType :: SQLType a2
    in _buildTupleStruct [t1, t2]

_buildTupleStruct :: [DataType] -> SQLType x
_buildTupleStruct dts =
  let fnames = unsafeFieldName . pack. ("_" ++) . show <$> ([1..] :: [Int])
      fs = uncurry StructField <$> zip fnames dts
  in SQLType . StrictType . Struct . StructType $ V.fromList fs

-- instance (SQLTypeable a, SQLTypeable b) => SQLTypeable (a,b) where
--   _genericTypeFromValue _ = _genericTypeFromValue (undefined :: a) ++ _genericTypeFromValue (undefined :: b)

-- Generic SQLTypeable
class GenSQLTypeable a where
  genTypeFromProxy :: Proxy a -> GenericType

-- Datatype
instance GenSQLTypeable f => GenSQLTypeable (M1 D x f) where
  genTypeFromProxy _ = genTypeFromProxy (Proxy :: Proxy f)

-- Constructor Metadata
instance (GenSQLTypeable f, Constructor c) => GenSQLTypeable (M1 C c f) where
  genTypeFromProxy _
    | conIsRecord (undefined :: t c f a) =
        let !dt = genTypeFromProxy (Proxy :: Proxy f) in
          dt
    | otherwise =
        -- It is assumed to be a newtype and we are going to unwrap it
        let !dt1 = genTypeFromProxy (Proxy :: Proxy f)
        in case iSingleField (unSQLType dt1) of
          Just dt -> SQLType dt
          Nothing ->
            failure $ sformat ("M1 C "%sh%" dt1="%sh) n dt1
              where m = undefined :: t c f a
                    n = conName m

-- Selector Metadata
instance (GenSQLTypeable f, Selector c) => GenSQLTypeable (M1 S c f) where
  genTypeFromProxy _ =
    let !st = genTypeFromProxy (Proxy :: Proxy f)
        m = undefined :: t c f a
        n = selName m
        SQLType innerdt = st
        field = StructField { structFieldName = FieldName $ pack n, structFieldType = innerdt }
        st2 = StructType (V.singleton field) in
      SQLType (StrictType $ Struct st2)

-- Constructor Paramater
instance (GenSQLTypeable (Rep f), SQLTypeable f) => GenSQLTypeable (K1 R f) where
  genTypeFromProxy _ = _genericTypeFromValue (undefined :: f)

-- Sum branch
instance (GenSQLTypeable a, GenSQLTypeable b) => GenSQLTypeable (a :+: b) where
  genTypeFromProxy _ =
    let !y1 = genTypeFromProxy (Proxy :: Proxy a)
        !y2 = genTypeFromProxy (Proxy :: Proxy b) in
      -- TODO: need to prune the branch and throw an error here
      trace ("SUM: y1=" ++ show y1 ++ " y2=" ++ show y2) y1

-- Product branch
instance (GenSQLTypeable a, GenSQLTypeable b) => GenSQLTypeable (a :*: b) where
  genTypeFromProxy _ =
    let y1 = genTypeFromProxy (Proxy :: Proxy a)
        y2 = genTypeFromProxy (Proxy :: Proxy b) in case (y1, y2) of
        (SQLType (StrictType (Struct s1)), SQLType (StrictType (Struct s2))) ->
          (SQLType . StrictType . Struct) s where
            fs = structFields s1 V.++ structFields s2
            s = StructType fs
        _ -> failure $ sformat ("should not happen: left="%sh%" right="%sh) y1 y2

-- Void branch
instance GenSQLTypeable U1 where
  genTypeFromProxy _ = failure "U1"
