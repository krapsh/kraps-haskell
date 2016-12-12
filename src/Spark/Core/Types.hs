{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Spark.Core.Types(
  DataType,
  Nullable(..),
  TupleEquivalence(..),
  NameTuple(..),
  -- intType,
  -- canNull,
  -- noNull,
  -- stringType,
  -- arrayType',
  -- cellType,
  -- structField,
  -- structType,
  -- arrayType,
  SQLType,
  columnType,
  SQLTypeable,
  buildType,
  StructField,
  StructType,
  castType,
  catNodePath
) where

import Formatting

import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.TypesGenerics
import Spark.Core.Internal.TypesFunctions
import Spark.Core.StructuresInternal
import Spark.Core.Internal.FunctionsInternals(TupleEquivalence(..), NameTuple(..))
import Spark.Core.Try

-- | Description of types supported in DataSets
-- Krapsh supports a restrictive subset of Algebraic Datatypes that is amenable to SQL
-- transformations. This file contains the description of all the supported types, and some
-- conversion tools.


-- -- Converts an (untyped) datatype to a generic tagged SQLType.
-- cellType :: DataType -> CellType
-- cellType = SQLType

-- Takes a given type and attempts to cast it to another type,
-- which is known by the type system.
castType :: forall a b. (SQLTypeable b) => SQLType a -> Try (SQLType b)
castType sqlt =
  let sqlt' = buildType :: SQLType b in
    if unSQLType sqlt == unSQLType sqlt' then Right sqlt'
      else tryError $ sformat ("castType: tried to cast "%shown%" into incompatible type "%shown) sqlt sqlt'
