{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

{-| Methods to prune the computation graph.
-}
module Spark.Core.Internal.Pruning(
  NodeCacheStatus(..),
  NodeCacheInfo(..),
  NodeCache,
  pruneGraph,
  pruneGraphDefault,
  emptyNodeCache
) where

import Data.HashMap.Strict as HM

import Spark.Core.StructuresInternal(NodeId, NodePath, ComputationID)
import Spark.Core.Internal.DatasetStructures(UntypedNode, StructureEdge)
import Spark.Core.Internal.DAGFunctions
import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.OpStructures


{-| The status of a node being computed.

On purpose, it does not store data. This is meant to be
only the control plane of the compuations.
-}
data NodeCacheStatus =
    NodeCacheRunning
  | NodeCacheError
  | NodeCacheSuccess
  deriving (Eq, Show)

{-| This structure describes the last time a node was observed by the
controller, and the state it was in.

This information is used to do smart computation pruning, by assuming
that the observables are kept by the Spark processes.
-}
data NodeCacheInfo = NodeCacheInfo {
  nciStatus :: !NodeCacheStatus,
  nciComputation :: !ComputationID,
  nciPath :: !NodePath
} deriving (Eq, Show)

type NodeCache = HM.HashMap NodeId NodeCacheInfo

emptyNodeCache :: NodeCache
emptyNodeCache = HM.empty

{-| It assumes a compute graph, NOT a dependency dag.
-}
pruneGraph :: (Show v) =>
  -- The current cache
  NodeCache ->
  (v -> NodeId) ->
  -- A function to create a node replacement
  (v -> NodeCacheInfo -> v) ->
  -- The graph
  Graph v StructureEdge ->
  Graph v StructureEdge
pruneGraph c getNodeId f g =
  -- Prune the node that we do not want
  let depGraph = reverseGraph g
      fop v = if HM.member (getNodeId v) c
              then CutChildren
              else Keep
      filtered = graphFilterVertices fop depGraph
      -- Bring back to normal flow.
      comFiltered = reverseGraph filtered
      -- Replace the nodes in the cache by place holders.
      -- This is done on the compute graph.
      repOp v = case HM.lookup (getNodeId v) c of
                  Just nci -> f v nci
                  Nothing -> v
      g' = graphMapVertices' repOp comFiltered
  in g'

pruneGraphDefault ::
  NodeCache -> Graph UntypedNode StructureEdge -> Graph UntypedNode StructureEdge
pruneGraphDefault c = pruneGraph c nodeId _createNodeCache

_createNodeCache :: UntypedNode -> NodeCacheInfo -> UntypedNode
_createNodeCache n nci =
  let name = "org.spark.PlaceholderCache"
      no = NodePointer (Pointer (nciComputation nci) (nciPath nci))
      n2 = emptyNodeStandard (nodeLocality n) (nodeType n) name
             `updateNodeOp` no
  in n2
