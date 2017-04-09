
{- |
Module      : Spark.Core.Dataset
Description : Dataset types and basic operations.

This module describes the core data types (Dataset, DataFrame,
Observable and DynObservable), and some basic operations to relate them.
-}
module Spark.Core.Dataset(
  -- * Common data structures
  -- TODO Should it be hidden?
  ComputeNode,
  LocLocal,
  LocDistributed,
  LocUnknown,
  UntypedNode,
  -- * Distributed data structures
  Dataset,
  DataFrame,
  -- * Local data structures
  LocalData,
  LocalFrame,
  -- * Conversions
  asDF,
  asDS,
  asLocalObservable,
  castType,
  castType',
  -- * Relations
  parents,
  untyped,
  untyped',
  depends,
  logicalParents,
  logicalParents',
  -- * Attributes
  nodeLogicalParents,
  nodeLogicalDependencies,
  nodeParents,
  nodeOp,
  nodeId,
  nodeName,
  nodeType,
  ) where

import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.Projections()
