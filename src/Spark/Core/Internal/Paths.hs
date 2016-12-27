{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

module Spark.Core.Internal.Paths(
  HasNodeName(..),
  PathEdge(..),
  computePaths,
  assignPaths',
  -- For testing:
  Scopes,
  ParentSplit(..),
  mergeScopes,
  gatherPaths,
  iGetScopes0,
) where

import qualified Data.Map.Strict as M
import qualified Data.Set as S
import qualified Data.Vector as V
import Data.List(sort)
import Data.Maybe(fromMaybe, catMaybes)
import Data.Foldable(foldr', foldl', toList)
import Formatting

import Spark.Core.Try
import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.ComputeDag
import Spark.Core.StructuresInternal

class HasNodeName v where
  -- Retrieves the name of the node
  getNodeName :: v -> NodeName
  -- Assigns a path to the node
  assignPath :: v -> NodePath -> v

{-| The types of edges for the calculation of paths.
 - same level parent -> the node should have the same prefix as its parents
 - inner edge -> the parent defines the scope of this node
 -}
data PathEdge = SameLevelEdge | InnerEdge deriving (Show, Eq)

-- Assigns paths in a graph.
--
computePaths :: (HasNodeName v) =>
  ComputeDag v PathEdge -> Try (M.Map VertexId NodePath)
computePaths cd =
  let nodecg = mapVertexData getNodeName cd
  in _computePaths nodecg

assignPaths' :: (HasNodeName v) =>
  M.Map VertexId NodePath -> ComputeDag v e -> ComputeDag v e
assignPaths' m cd =
  let f vx =
        let old = NodePath . V.singleton $ getNodeName (vertexData vx)
            new = M.findWithDefault old (vertexId vx) m
        in assignPath (vertexData vx) new
  in mapVertices f cd

-- The main function to perform the pass assignments.
-- It starts from the graph of dependencies and from the local name info,
-- and computes the complete paths (if possible), starting from the fringe.
_computePaths :: ComputeDag NodeName PathEdge -> Try (M.Map VertexId NodePath)
_computePaths cg =
  let
    scopes = iGetScopes0 (toList . cdOutputs $ cg) (_splitParents' (cdEdges cg))
    paths = gatherPaths scopes
    nodeNames = M.fromList [(vertexId vx, vertexData vx)| vx <- toList . cdVertices $ cg]
    lookup' nid = M.lookup nid nodeNames
    f :: VertexId -> [[VertexId]] -> Try NodePath
    f nid ls = case ls of
      [l] ->
        return . NodePath . V.fromList . catMaybes $ lookup' <$> (l ++ [nid])
      x ->
        tryError $ sformat ("Node has too many paths: node="%shown%" discovered paths ="%shown) nid x
    nodePaths = M.traverseWithKey f paths
  in nodePaths

-- (private)
-- The top-level scope may not have an ID associated to it
type Scopes = M.Map (Maybe VertexId) (S.Set VertexId)


-- (internal)
-- The separation of parents into logical and inner parents
data ParentSplit a = ParentSplit {
  psLogical :: ![Vertex a],
  psInner :: ![Vertex a]
} deriving (Show)

_lookupOrEmpty :: Scopes -> Maybe VertexId -> [VertexId]
_lookupOrEmpty scopes mnid =
  S.toList $ fromMaybe S.empty (M.lookup mnid scopes)

mergeScopes :: Scopes -> Scopes -> Scopes
mergeScopes = M.unionWith S.union

_singleScope :: Maybe VertexId -> VertexId -> Scopes
_singleScope mKey nid = M.singleton mKey (S.singleton nid)

-- For each node, finds the one, or more than one if possible, path(s)
-- from the root to the node (which is itself not included at the end)
-- The gathering of paths may not be exaustive.
gatherPaths :: Scopes -> M.Map VertexId [[VertexId]]
gatherPaths scopes = M.map sort $ _gatherPaths0 scopes start where
  start = _lookupOrEmpty scopes Nothing

_gatherPaths0 :: Scopes -> [VertexId] -> M.Map VertexId [[VertexId]]
_gatherPaths0 _ [] = M.empty
_gatherPaths0 scopes (nid : t) =
  let
    inner = _lookupOrEmpty scopes (Just nid)
    innerPaths = _gatherPaths0 scopes inner
    innerWithHead = M.map (\l -> (nid : ) <$> l) innerPaths
    thisPaths = M.singleton nid [[]]
    innerPaths2 = M.unionWith (++) innerWithHead thisPaths
  in M.unionWith (++) innerPaths2 (_gatherPaths0 scopes t)


iGetScopes0 :: forall a. (Show a) =>
  [Vertex a] ->
  (Vertex a -> ParentSplit a) ->
  Scopes
iGetScopes0 [] _splitter = M.empty
iGetScopes0 (h : t) splitter =
  let
    startScope = _singleScope Nothing (vertexId h)
    folder :: Scopes -> Vertex a -> Scopes
    folder current un =
      if M.member (Just (vertexId un)) current then
        current
      else
        let split = _getScopes' splitter Nothing S.empty un current
        in mergeScopes split current
  -- Important here to use a left folder, as we want to start with the head
  -- and move down the list.
  in foldl' folder startScope (h : t)

_splitParents' :: AdjacencyMap v PathEdge -> Vertex v -> ParentSplit v
_splitParents' m vx =
  let ves = V.toList $ M.findWithDefault V.empty (vertexId vx) m
      scope = [veEndVertex ve | ve <- ves, edgeData (veEdge ve) == SameLevelEdge]
      parents' = [veEndVertex ve | ve <- ves, edgeData (veEdge ve) == InnerEdge]
  in ParentSplit { psLogical = scope, psInner = parents' }


-- TODO(kps) this recursive code is most probably going to explode for deep stacks
_getScopes' :: forall a. (Show a) =>
  (Vertex a -> ParentSplit a) -> -- The expansion of a node into logical and inner nodes
  Maybe VertexId -> -- the current parent (if any)
  S.Set VertexId -> -- the current boundary to respect
  Vertex a -> -- the current node to expand
  Scopes -> -- the scopes seen so far
  Scopes
_getScopes' splitter mScopeId boundary un scopes =
  if S.member (vertexId un) boundary then
    scopes
  else
    let
      split = splitter un
      logParents = psLogical split
      innerParents = psInner split
      -- A fold on the parents
      parF :: Vertex a -> Scopes -> Scopes
      parF =
        -- Same boundary and parent, but update the scopes
        _getScopes' splitter mScopeId boundary
      scopesPar = foldr' parF scopes logParents
      -- Now work on the inner nodes:
      vid = vertexId un
      boundary' = S.fromList (vertexId <$> logParents)
      inF :: Vertex a -> Scopes -> Scopes
      inF =
        -- parent is current, boundary is current logical
        _getScopes' splitter (Just vid) boundary'
      scopesIn = foldr' inF scopesPar innerParents
      scopesFinal = scopesIn
          `mergeScopes` _singleScope mScopeId vid
          `mergeScopes` M.singleton (Just vid) S.empty
    in scopesFinal
