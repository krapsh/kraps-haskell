{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

{- Methods related to checking caching in the graph.
-}

module Spark.Core.Internal.Caching(
  NodeCachingType(..),
  CachingFailure(..),
  CacheTry,
  CacheGraph,
  AutocacheGen(..),
  checkCaching,
  fillAutoCache
) where

import Control.Monad.Identity
import qualified Data.Set as S
import qualified Data.Map.Strict as M
import qualified Data.Vector as V
import Control.Arrow((&&&))
import Data.Foldable
import Data.Set(Set)
import Data.Maybe(mapMaybe)
import Debug.Trace(trace)
import Data.Text(Text)
import Formatting
-- import Debug.Trace

import Spark.Core.Internal.DAGFunctions
import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.Utilities
-- import Spark.Core.StructuresInternal(NodeId)

data NodeCachingType =
    -- Hinted caching. Will be fullfilled by the algorithm below.
    -- The node id is that of the node being cached
    AutocacheOp VertexId
    -- Unconditional caching
    -- The node id is that of the node being cached
  | CacheOp VertexId
    -- First one is the node id
    -- the second is the id of the matching cache or autocache node
  | UncacheOp VertexId VertexId
  | Through
  | Stop deriving (Show, Eq)

data CachingFailure = CachingFailure {
  cachingNode :: !VertexId,
  uncachingNode :: !VertexId,
  escapingNode :: !VertexId
} deriving (Show, Eq)

type CacheTry t = Either Text t

type CacheGraph v = Graph (v, NodeCachingType) StructureEdge

data AutocacheGen v = AutocacheGen {
  -- Generates an uncaching node to insert at the final location of uncaching
  deriveUncache :: Vertex v -> Vertex v,
  -- A function that given a node, generates an identity node (and a new node
  -- id) that can be inserted in place. The generated node id will be used with
  -- the identity node newly generated; the previous node will be moved around
  -- along with its identity.
  deriveIdentity :: Vertex v -> Vertex v
}

checkCaching :: (Show v) =>
  Graph v StructureEdge ->
  (v -> CacheTry NodeCachingType) ->
  CacheTry [CachingFailure]
checkCaching g fun = _cacheGraph g fun >>= _checkCaching

fillAutoCache :: (Show v) =>
  (v -> CacheTry NodeCachingType) ->
  AutocacheGen v ->
  Graph v StructureEdge ->
  -- The final graph being constructed.
  DagTry (Graph v StructureEdge)
fillAutoCache cacheFun acGen g = do
  cg <- graphMapVertices g $ \vx _ -> (const vx &&& id) <$> cacheFun vx
  acg <- _fillAutoCache acGen cg
  let acg' = graphMapVertices' fst acg
  return acg'


-- Some internal types to guarantee more correctness
newtype AutocacheVertex v = AutocacheVertex (Vertex v)
newtype StopVertex v = StopVertex { unStopVertex :: Vertex v }
newtype IdentityVertex v = IdentityVertex (Vertex v)
data UncacheVertex v = UncacheVertex (Vertex v) VertexId deriving (Show)
newtype AnyCacheOp = AnyCacheOp { unAnyCacheOp :: VertexId } deriving (Show, Ord, Eq)

-- The result of creating a vertex
type CreateUncache v =
  Either (UncacheVertex v) (UncacheVertex v, Edge StructureEdge)

-- This performs a graph transform:
-- For each autocache node, it finds the transitive closure of Stop nodes.
-- Then it replaces the sink nodes by a layer of identity nodes and sink nodes
-- and intercalates the uncache node between the two layers, through some
-- logical dependencies.
--
-- If it does not find the closuure, it leaves these
-- autocache nodes alone and does not attempt to remove them: they will be
-- considered as being unconditional caching without checks.
--
-- Note that it works on the reverse of the graph (flow instead of dependencies)
_fillAutoCache :: forall v. (Show v) =>
  AutocacheGen v ->
  -- The graph, already annotated with caching information
  Graph (v, NodeCachingType) StructureEdge ->
  -- The final graph being constructed.
  DagTry (Graph (v, NodeCachingType) StructureEdge)
_fillAutoCache acGen cg =
  -- Find the auto nodes.
  -- Compute the closure for each of them
  -- Perform the insertion.
  -- TODO: this function is too big, split or build subfunctions.
  let
    vxMap = M.fromList ((vertexId &&& id) <$> toList (gVertices cg))
    -- TODO: mark if the result was already in the graph
    findOrCreateIdentity :: StopVertex v -> IdentityVertex v
    findOrCreateIdentity (StopVertex vx) =
      let uvx = deriveIdentity acGen vx
      in case M.lookup (vertexId uvx) vxMap of
        Just vx' -> IdentityVertex $ fst <$> vx' -- Already created
        Nothing -> IdentityVertex uvx
    acNodesAndScopes = _autoCachingCandidates cg
    -- Add the uncaching nodes
    findOrCreateUncache' (acv, l) = case _findOrCreateUncache vxMap acGen acv of
      Left x -> trace ("findOrCreateUncache: dropping autocache node " ++ show x) Nothing
      Right (ucv, ed) -> Just (ed, (acv, ucv, l))
    acWithUncache' = mapMaybe findOrCreateUncache' acNodesAndScopes
    acWithUncache = snd <$> acWithUncache'
    acEdges = fst <$> acWithUncache'
    -- Now group by stop vertex, so that each stop vertex has a list of
    -- associated cache and uncache nodes.
    -- Not sure if they may be several, but it just sounds like good practice.
    tups = myGroupBy [(vertexId (unStopVertex svx), (cvx, uvx, svx)) | (cvx, uvx, l) <- acWithUncache,
                                              svx <- l]
    -- Just in this case, it should work because of the construction above
    -- TODO: put a lot more documentation here, it is tricky code
    group ((_, uvx, svx) : t) = (svx, findOrCreateIdentity svx, uvx : [uvx' | ( _, uvx', _) <- t])
    group [] = failure "_fillAutoCache:group: empty: should not happen"
    stopsWithCachingSteps :: [(StopVertex v, IdentityVertex v, [UncacheVertex v])]
    stopsWithCachingSteps = (group . snd) <$> M.toList tups
    tups2 = [(svx, ivx, uvx) | (svx, ivx, l) <- stopsWithCachingSteps, uvx <- l]
    folder eds (svx, ivx, uvx) = _performEdgeTransform svx ivx uvx eds
    startEdges = veEdge <$> [ve | (_, v) <- M.toList (gEdges cg), ve <- V.toList v]
    edges = acEdges ++ foldl' folder startEdges tups2
    -- Gather all the vertices and edges, and remove duplicates
    startVertices = V.toList (gVertices cg)
    ucVertices = acWithUncache <&> \(_, UncacheVertex vx cacheVid, _) ->
      -- TODO: propagate the cache vertexId with UncacheVertex
      let op = UncacheOp (vertexId vx) cacheVid
      in (id &&& const op) <$> vx
    idVertices = tups2 <&> \(_, IdentityVertex vx, _) ->
      (id &&& const Stop) <$> vx
    allVertices = startVertices ++ ucVertices ++ idVertices
    -- Make a new graph
  in buildGraphFromList allVertices edges

-- TODO: should be a try to perform extra check operations
_findOrCreateUncache :: (HasCallStack, Show v) =>
  M.Map VertexId (Vertex (v, NodeCachingType)) ->
  AutocacheGen v ->
  AutocacheVertex v -> CreateUncache v
_findOrCreateUncache vxMap acGen (AutocacheVertex acv) =
  let uvx = deriveUncache acGen acv
      acVid = vertexId acv
      uVid = vertexId uvx
      look = vertexData <$> M.lookup uVid vxMap
  in case look of
    Just (x, UncacheOp _ _) ->
      -- That vertex already exists, we will not try to create
      -- an uncaching node then
      Left $ UncacheVertex (Vertex uVid x) uVid
    Just _ ->
      -- That vertex already exists, but it is not the proper type.
      -- This is a programming error in AutocacheGen: we abort here.
      failure $ sformat ("_findOrCreateUncache:"%sh%"->"%sh) acv look
    Nothing ->
      -- The uncache node does not exist, we are going to create one.
      let ed' = Edge uVid acVid ParentEdge
      in Right (UncacheVertex uvx uVid, ed')


-- FIXME: duplicated work on the stop and identity: pass all the uncache vertexes to process them in one go
_performEdgeTransform ::
  StopVertex v -> IdentityVertex v -> UncacheVertex v -> [Edge StructureEdge] -> [Edge StructureEdge]
_performEdgeTransform (StopVertex svx) (IdentityVertex ivx) (UncacheVertex uvx _) eds =
  let stopVid = vertexId svx
      idenVid = vertexId ivx
      ucVid = vertexId uvx
      -- Rewrite the edges incoming to the stop node so that they point to the
      -- id node instead.
      f ed | edgeTo ed == stopVid = ed { edgeTo = idenVid }
      f ed = ed
      joinEd = Edge { edgeFrom = idenVid, edgeTo = stopVid, edgeData = ParentEdge }
      id1Ed = Edge { edgeFrom = idenVid, edgeTo = ucVid, edgeData = LogicalEdge }
      id2Ed = Edge { edgeFrom = ucVid, edgeTo = stopVid, edgeData = LogicalEdge }
  in id1Ed : id2Ed : joinEd : (f <$> eds)

-- The list of nodes that do autocaching, and the fringes for each of these
-- nodes.
-- Returns a list of caching node -> [stop node]
_autoCachingCandidates :: forall v. (Show v) =>
  Graph (v, NodeCachingType) StructureEdge ->
  [(AutocacheVertex v, [StopVertex v])]
_autoCachingCandidates cg =
  let
    cg' = graphMapVertices' snd cg
    exps = gVertices $ _expansions cg'
    extractAutocache vx = case snd (vertexData vx) of
      AutocacheOp _ -> [AutocacheVertex (fst <$> vx)]
      _ -> []
    acVxs = concatMap extractAutocache (gVertices cg)
    -- All the stop nodes for each caching vertex id
    extractFringe vx = case vertexData vx of
      (Stop, set) -> (id &&& const (vertexId vx)) <$> toList set
      _ -> []
    -- cache vid -> Stop vertex id
    acWithFringe = myGroupBy $ concatMap extractFringe (toList exps)
    vmap = vertexMap cg
    vmap' = vertexMap cg'
    -- TODO: should be a try and it should not fail
    findStop :: VertexId -> Maybe (StopVertex v)
    findStop vid = do
      vx <- M.lookup vid vmap
      _ <- M.lookup vid vmap'
      return $ StopVertex (Vertex vid (fst vx))
    -- TODO: it should be a try, although it is a programming error here
    combineWithFringe :: AutocacheVertex v -> (AutocacheVertex v, [StopVertex v])
    combineWithFringe acv @ (AutocacheVertex vx) =
      let vids = M.findWithDefault [] (AnyCacheOp (vertexId vx)) acWithFringe
      in (acv, mapMaybe findStop vids)
    -- Remove the nodes that do not have a fringe.
    -- In this case, they are passed through without uncaching operation.
    acWithFringeVx = filter (not.null.snd) $ combineWithFringe <$> acVxs
  in acWithFringeVx

_checkCaching :: Graph NodeCachingType StructureEdge -> CacheTry [CachingFailure]
_checkCaching cg =
  let
    expands = snd <$> vertexMap (_expansions cg)
    removals = vertexMap $ _removals cg
    f :: NodeCachingType -> [(VertexId, VertexId)]
    f (UncacheOp uncacheNid cacheNid) = [(cacheNid, uncacheNid)]
    f _ = []
    -- cacheNID -> uncacheNID
    removedNodes = M.fromList $ concatMap f (vertexData <$> gVertices cg)
    removedNodeSet = S.fromList $ M.keys removedNodes
    checkErrors :: VertexId -> [CachingFailure]
    checkErrors nid =
      let rems = S.intersection
                  removedNodeSet
                  (M.findWithDefault S.empty nid removals)
          exps = S.intersection
                   removedNodeSet
                   (unAnyCacheOp `S.map` M.findWithDefault S.empty nid expands)
          bad = S.toList $ S.difference exps rems
          badWithUncache = flip mapMaybe bad $ \ cacheNid ->
            M.lookup cacheNid removedNodes <&> \uncacheNid ->
              CachingFailure cacheNid uncacheNid nid
      in badWithUncache
  in return $ concatMap checkErrors (vertexId <$> gVertices cg)

_cacheGraph :: (Show v) => Graph v StructureEdge ->
  (v -> CacheTry NodeCachingType) ->
  CacheTry (Graph NodeCachingType StructureEdge)
_cacheGraph g f =
  graphMapVertices g f' where
    f' vx _ = f vx

-- The set of node caching operations at each step.
-- This includes both regular cache and autocache.
_expansions :: (Show e) =>
  Graph NodeCachingType e ->
  Graph (NodeCachingType, Set AnyCacheOp) e
_expansions g = runIdentity (graphMapVertices g f) where
  f x l = return (x, S.union seta parentSet) where
    filt ((Stop, _), _) = S.empty
    -- Uncaching drops the caching node from the expansions
    filt ((UncacheOp _ cacheVid, s), _) = S.delete (AnyCacheOp cacheVid) s
    filt ((_, s), _) = s
    parentSet :: S.Set AnyCacheOp
    parentSet = S.unions (filt <$> l)
    seta = case x of
      CacheOp nid -> S.singleton (AnyCacheOp nid)
      AutocacheOp nid -> S.singleton (AnyCacheOp nid)
      _ -> S.empty

_removals :: (Show e) =>
  Graph NodeCachingType e -> Graph (Set VertexId) e
_removals g = runIdentity (graphMapVertices (reverseGraph g) f) where
  f x l = return $ S.union seta (S.unions (fst <$> l)) where
    seta = case x of
      UncacheOp _ cacheNid -> S.singleton cacheNid
      _ -> S.empty
