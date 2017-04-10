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
  -- castType,
  catNodePath,
  unSQLType
) where

import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.TypesGenerics
import Spark.Core.Internal.TypesFunctions
import Spark.Core.StructuresInternal
import Spark.Core.Internal.FunctionsInternals(TupleEquivalence(..), NameTuple(..))

-- | Description of types supported in DataSets
-- Karps supports a restrictive subset of Algebraic Datatypes that is amenable to SQL
-- transformations. This file contains the description of all the supported types, and some
-- conversion tools.
