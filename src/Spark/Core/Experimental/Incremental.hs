{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.Experimental.Incremental where

import Spark.Core.Dataset
import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.DatasetStructures
-- import Spark.Core.Types
import Spark.Core.StructuresInternal
-- import Spark.Core.Types
-- import Spark.Core.Try
import Spark.Core.Row
-- import Spark.Core.Internal.Utilities
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.Utilities(failure, HasCallStack)

import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.HashMap.Strict as HM
import Data.HashMap.Strict(HashMap)
import Data.Sequence(Seq)

-- A DAG of computation nodes.
-- At a high level, it is a total function with a number of inputs and a number
-- of outputs.
data ComputeDAG v e = ComputeDAG {
  -- The edges that make up the DAG
  cdEdges :: !(AdjacencyMap v e),
  -- All the vertices of the graph
  -- Sorted by lexicographic order + node id for uniqueness
  cdVertices :: !(Seq (Vertex v)),
  -- The sources of the graph (a subset of the vertices)
  -- Sorted by node id
  cdSources :: !(Seq (Vertex v)),
  -- The sinks of the graph (a subset of the vertices)
  -- sorted by node id
  cdSinks :: !(Seq (Vertex v)),
  -- Some mapping between a vertex id and some reference id.
  -- This is useful to keep track of a node through various transforms.
  cdReferences :: !(HashMap NodeId NodeId)
}

data EdgeType =
    DirectDependency
  | LogicalDependency
  | ScopingDependency

-- A compute DAG, without typing information.
type UComputeDag = ComputeDAG UntypedNode EdgeType

-- | The return types for this module.
type IncTry a = Either T.Text a

-- Given a DAG, returns the incremental DAG, in which the inputs are all
-- the vertices from the first graph as well as a new fringe for the
-- source vertices.
incrementalExact ::
  UComputeDag ->
  M.Map NodeId UntypedNode ->
  IncTry UComputeDag
incrementalExact = undefined

-- Extracts the DAG of a compute graph.
_computeGraphToDag :: ComputeDAG v e -> Graph v e

-- Given
_recoverComputeGraph :: ComputeDAG v e -> Graph v e -> ComputeDAG v e

-- The rules that are implemented for the differentiation.
data TransType =
    DFUnion UntypedDataset (UntypedDataset, UntypedDataset)
  | DFOrderPreservingTrans StandardOperator UntypedDataset UntypedDataset
  | DFPartialUnivAgg UniversalAggregatorOp UntypedLocalData UntypedDataset
  | DFFullUnivAgg UniversalAggregatorOp UntypedLocalData UntypedLocalData
  | OpaqueTrans -- We failed to identify a known case
  deriving (Show)

-- Given a DAG, in which some top nodes are unions of datasets,
-- tries to unsplice these unions down the graph to build a more optimal
-- graph, while preserving the semantics.
-- The node ids
_unsplice :: UComputeDag -> UComputeDag
_unsplice ucd = error "_unsplice"

-- Tries to build a new incremental node from fringe and the current node to
-- process. That node may expand into multiple nodes if necessary.
_nodeIncrementalExact :: (HasCallStack) =>
  UntypedNode -> [UntypedNode] -> IncTry UntypedNode
_nodeIncrementalExact node new_fringe =
  _nodeTransType node new_fringe >>= \t -> case t of
    DFUnion ds1 (ds2, ds3) -> Right . untyped $ union ds1 (ds2 `union` ds3)
    DFPartialUnivAgg uao ld ds ->
      let sqlt = nodeType ld
          outer :: UntypedDataset -> UntypedLocalData
          outer = nodeOpToFun1Typed sqlt (NodeLocalOp (uaoInitialOuter uao))
          merge :: UntypedLocalData -> UntypedLocalData -> UntypedLocalData
          merge = nodeOpToFun2Typed sqlt (NodeLocalOp (uaoMergeBuffer uao))
      in Right . untyped $ merge ld (outer ds)
    x -> failure $ "Case not implemented: " ++ show x


_nodeTransType :: (HasCallStack) =>
  UntypedNode -> [UntypedNode] -> IncTry TransType
_nodeTransType node new_fringe =
  case nodeOp node of
    NodeDistributedOp so | soName so == _opnameUnion ->
      case new_fringe of
        [ds1, ds2] ->
          let p = (_unsafeCastNode ds1, _unsafeCastNode ds2) in
            Right $ DFUnion (_unsafeCastNode node) p
        x -> failure $ "Expected pair, got " ++ show x
    NodeUniversalAggregator uao ->
      case new_fringe of
        [ds] ->
          Right $ DFPartialUnivAgg uao (_unsafeCastNode node) (_unsafeCastNode ds)
        x -> failure $ "Expected one element, got " ++ show x
    _ -> Right OpaqueTrans





--
--
-- data StreamingComputeNode loc a =
--     FirstElement (ComputeNode loc a)
--   | Increment (ComputeNode loc a) (StreamingComputeNode loc a)
--
-- type UntypedStream = StreamingComputeNode LocUnknown Cell
--
-- unrollStream :: StreamingComputeNode loc a -> [ComputeNode loc a]
-- unrollStream (FirstElement n) = [n]
-- unrollStream (Increment n s) = n : unrollStream s
--
-- incrementalExact :: ComputeNode loc a -> StreamingComputeNode loc a
-- incrementalExact = undefined
--
-- incrementalExactFun1 :: (ComputeNode loc a -> ComputeNode loc' a') ->
--   StreamingComputeNode loc a -> StreamingComputeNode loc' a'
-- incrementalExactFun1 = undefined
--
-- -- performs the streaming transform, with no types.
-- incrementalExactFun1' :: UntypedNode -> [UntypedStream] -> UntypedStream
-- incrementalExactFun1' = undefined
