{-# LANGUAGE FlexibleContexts #-}

{- |
Module      : Spark.Core.Column
Description : Column types and basic operations.

Operations on columns.
-}
module Spark.Core.Column(
  -- * Types
  Column,
  DynColumn,
  GenericColumn,
  -- * Extractions and collations
  asCol,
  asCol',
  pack1,
  pack,
  pack',
  struct,
  struct',
  castCol,
  castCol',
  colRef,
  (//),
  (/-),
  -- ToStaticProjectable,
  StaticColProjection,
  DynamicColProjection,
  unsafeStaticProjection,
  -- * Column type manipulations
  dropColType,
  -- * Column functions
  colType,
  untypedCol,
  colFromObs,
  colFromObs',
  applyCol1,
  ) where

import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.ColumnFunctions
import Spark.Core.Internal.FunctionsInternals
import Spark.Core.Internal.Projections
