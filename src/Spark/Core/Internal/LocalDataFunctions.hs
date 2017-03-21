{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- A number of functions related to local data.

module Spark.Core.Internal.LocalDataFunctions(
  constant,
  iPackTupleObs
) where

import Data.Aeson(toJSON, Value(Null))
import qualified Data.Text as T
import qualified Data.List.NonEmpty as N
import Control.Exception.Base(assert)

import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.TypesFunctions
import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.TypesGenerics(SQLTypeable, buildType)
import Spark.Core.Row

constant :: (ToSQL a, SQLTypeable a) => a -> LocalData a
constant cst =
  let
    sqlt = buildType
    dt = unSQLType sqlt
  in emptyLocalData (NodeLocalLit dt (toJSON (valueToCell cst))) sqlt

{-| (developer API)

This function takes a non-empty list of observables and puts them
into a structure. The names of each element is _0 ... _(n-1)
-}
iPackTupleObs :: N.NonEmpty UntypedLocalData -> UntypedLocalData
iPackTupleObs ulds =
  let dt = structTypeTuple' (unSQLType . nodeType <$> ulds)
      so = StandardOperator {
                soName = "org.spark.LocalPack",
                soOutputType = dt,
                soExtra = Null }
      op = NodeLocalOp so
  in emptyLocalData op (SQLType dt)
        `parents` (untyped <$> N.toList ulds)

instance (Num a, ToSQL a, SQLTypeable a) => Num (LocalData a) where
  -- TODO: convert all that to use column operations
  (+) = _binOp "org.spark.LocalPlus"
  (-) = _binOp "org.spark.LocalMinus"
  (*) = _binOp "org.spark.LocalMult"
  abs = _unaryOp "org.spark.LocalAbs"
  signum = _unaryOp "org.spark.LocalSignum"
  fromInteger x = constant (fromInteger x :: a)
  negate = _unaryOp "org.spark.LocalNegate"

instance forall a. (ToSQL a, Enum a, SQLTypeable a) => Enum (LocalData a) where
  toEnum x = constant (toEnum x :: a)
  fromEnum = failure "Cannot use fromEnum against a local data object"
  -- TODO(kps) some of the others are still available for implementation

instance (Num a, Ord a) => Ord (LocalData a) where
  compare = failure "You cannot compare instances of LocalData. (yet)."
  min = _binOp "org.spark.LocalMin"
  max = _binOp "org.spark.LocalMax"

instance forall a. (Real a, ToSQL a, SQLTypeable a) => Real (LocalData a) where
  toRational = failure "Cannot convert LocalData to rational"

instance (ToSQL a, Integral a, SQLTypeable a) => Integral (LocalData a) where
  quot = _binOp "org.spark.LocalQuotient"
  rem = _binOp "org.spark.LocalReminder"
  div = _binOp "org.spark.LocalDiv"
  mod = _binOp "org.spark.LocalMod"
  quotRem = failure "quotRem is not implemented (yet). Use quot and rem."
  divMod = failure "divMod is not implemented (yet). Use div and mod."
  toInteger = failure "Cannot convert LocalData to integer"

instance (ToSQL a, SQLTypeable a, Fractional a) => Fractional (LocalData a) where
  fromRational x = constant (fromRational x :: a)
  (/) = _binOp "org.spark.LocalDiv"


_unaryOp :: T.Text -> LocalData a -> LocalData a
_unaryOp optxt ld =
  let so = StandardOperator {
            soName = optxt,
            soOutputType = unSQLType $ nodeType ld,
            soExtra = Null }
      op = NodeLocalOp so in
  emptyLocalData op (nodeType ld)
    `parents` [untyped ld]

_binOp :: T.Text -> LocalData a -> LocalData a -> LocalData a
_binOp optxt ld1 ld2 = assert (nodeType ld1 == nodeType ld2) $
  let so = StandardOperator {
          soName = optxt,
          soOutputType = unSQLType $ nodeType ld1,
          soExtra = Null }
      op = NodeLocalOp so in
  emptyLocalData op (nodeType ld1)
    `parents` [untyped ld1, untyped ld2]

-- TODO(kps) more input tests
_binOp' :: StandardOperator -> LocalData a -> LocalData a -> LocalData a
_binOp' so ld1 ld2 = assert (nodeType ld1 == nodeType ld2) $
  let op = NodeLocalOp so in
  emptyLocalData op (nodeType ld1)
    `parents` [untyped ld1, untyped ld2]

_intOperator :: T.Text -> StandardOperator
_intOperator optxt = StandardOperator {
  soName = optxt,
  soOutputType = intType,
  soExtra = Null
}
