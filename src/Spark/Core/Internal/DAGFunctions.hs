{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}

{-| A set of utility functions to build and transform DAGs.

Because I could not find a public library for such transforms.

Most Karps manipulations are converted into graph manipulations.
-}
module Spark.Core.Internal.DAGFunctions(
  DagTry,
  FilterOp(..),
  -- Building
  buildGraph,
  buildVertexList,
  buildGraphFromList,
  -- Queries
  graphSinks,
  graphSources,
  -- Transforms
  graphMapVertices,
  graphMapVertices',
  vertexMap,
  graphFlatMapEdges,
  graphMapEdges,
  reverseGraph,
  verticesAndEdges,
  graphFilterVertices,
  pruneLexicographic
) where

import qualified Data.Set as S
import qualified Data.Map.Strict as M
import qualified Data.Vector as V
import Data.List(sortBy)
import Data.Maybe
import Data.Foldable(toList)
import Data.Text(Text)
import Control.Arrow((&&&))
import Control.Monad.Except
import Formatting
import Control.Monad.Identity

import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.Utilities

-- | Separate type of error to make it more general and easier
-- to test.
type DagTry a = Either Text a

{-| The different filter modes when pruning a graph.

Keep: keep the current node.
CutChildren: keep the current node, but do not consider the children.
Remove: remove the current node, do not consider the children.
-}
data FilterOp = Keep | Remove | CutChildren

{-| Starts from a vertex and expands the vertex to reach all the transitive
closure of the vertex.

Returns a list in lexicographic order of dependencies: the graph corresponding
to this list of elements has one sink (the start element) and potentially
multiple sources. The nodes are ordered so that all the parents are visited
before the node itself.
-}
buildVertexList :: (GraphVertexOperations v, Show v) => v -> DagTry [v]
buildVertexList x = buildVertexListBounded x []

{-| Builds the list of vertices, up to a boundary.
-}
buildVertexListBounded :: (GraphVertexOperations v, Show v) =>
  v -> [v] -> DagTry [v]
buildVertexListBounded x boundary =
  let
    boundaryIds = S.fromList $ vertexToId <$> boundary
    traversals = toList $ _buildList boundaryIds [x] M.empty
    lexico = _lexicographic vertexToId traversals in lexico

-- | Builds a graph by expanding a start vertex.
buildGraph :: forall v e. (GraphOperations v e, Show v) =>
  v -> DagTry (Graph v e)
buildGraph start = buildVertexList start <&> \vxData ->
  let vertices = [Vertex (vertexToId vx) vx | vx <- vxData]
      -- The edges and vertices are already in the right order, no need
      -- to do further checks
      f :: v -> (VertexId, V.Vector (VertexEdge e v))
      f x =
        let vid = vertexToId x
            g :: (e, v) -> VertexEdge e v
            g (ed, x') =
              let toId = vertexToId x'
                  v' = Vertex toId x'
                  e = Edge vid toId ed
              in VertexEdge v' e
            vedges = g <$> expandVertex x
        in (vid, V.fromList vedges)
      vxs = V.fromList vertices
      edges = f <$> vxData
      adj = M.fromList edges
  in Graph adj vxs

{-| Attempts to build a graph from a collection of vertices and edges.

This collection may be invalid (cycles, etc.) and the vertices need not
be in topological order.

All the vertices referred by edges must be present in the list of vertices.
-}
buildGraphFromList :: forall v e. (Show v) =>
  [Vertex v] -> [Edge e] -> DagTry (Graph v e)
buildGraphFromList vxs eds = do
  -- 1. Group the edges by start point
  -- 2. Find the lexicgraphic order (if possible)
  vxById <- _vertexById vxs
  -- The topological information
  let edTopo = myGroupBy $ (edgeFrom &&& edgeTo) <$> eds
  let vertexById :: VertexId -> DagTry (Vertex v)
      vertexById vid = case M.lookup vid vxById of
        Nothing -> throwError $ sformat ("buildGraphFromList: vertex id found in edge but not in vertices: "%sh) vid
        Just vx -> pure vx
  let f :: Vertex v -> DagTry (Vertex v, [Vertex v])
      f vx =
        let links = M.findWithDefault [] (vertexId vx) edTopo
        in sequence (vertexById <$> links) <&> \l -> (vx, l)
  verticesWithEnds <- sequence $ f <$> vxs
  let indexedVertices = zip [1..] verticesWithEnds <&> \(idx, (vx, l)) -> (idx, vx, l)
  -- The nodes in lexicographic order.
  lexico <- _lexicographic vertexId indexedVertices
  -- Build the edge map:
  -- vertexFromId -> vertexEdge
  let vertexEdge :: Edge e -> DagTry (VertexId, VertexEdge e v)
      vertexEdge e = do
        vxTo <- vertexById (edgeTo e)
        -- Used to confirm that the start vertex is here
        _ <- vertexById (edgeFrom e)
        return (edgeFrom e, VertexEdge vxTo e)
  vEdges <- sequence $ vertexEdge <$> eds
  let edgeMap = M.map V.fromList (myGroupBy vEdges)
  return $ Graph edgeMap (V.fromList lexico)

_vertexById :: (Show v) => [Vertex v] -> DagTry (M.Map VertexId (Vertex v))
_vertexById vxs =
  -- This is probably not the most pretty, but it works.
  let vxById = myGroupBy $ (vertexId &&& id) <$> vxs
      f (vid, [vx]) = pure (vid, vx)
      f (vid, l) = throwError $ sformat ("_VertexById: Multiple vertices with the same id: "%sh%" in "%sh) vid l
  in M.fromList <$> sequence (f <$> M.toList vxById)

-- This implementation is not very efficient and is probably a performance
-- bottleneck.
-- Int is the traversal order. It is just used to break the ties.
-- VertexId is the node id of the vertex.
_lexicographic :: (v -> VertexId) -> [(Int, v, [v])] -> DagTry [v]
_lexicographic _ [] = return []
_lexicographic f m =
  -- We use the traversal ordering to separate the ties.
  -- The first nodes traversed get priority.
  let fcmp (idx, _, []) (idx', _, []) = compare idx idx'
      fcmp (_, _, []) (_, _, _) = LT
      fcmp (_, _, _) (_, _, []) = GT
      fcmp (_, _, _) (_, _, _) = EQ -- This one does not matter
  in case sortBy fcmp m of
    [] -> throwError "_lexicographic: there is a cycle"
    ((_, v, _) : t) ->
      let currentId = f v
          removeCurrentId l = [v' | v' <- l, f v' /= currentId]
          m' = t <&> \(idx, v', l) -> (idx, v', removeCurrentId l)
          tl = _lexicographic f m'
      in (v :) <$> tl


_buildList :: (Show v, GraphVertexOperations v) =>
  S.Set VertexId -> -- boundary ids, they will not be traversed
  [v] -> -- fringe ids
  M.Map VertexId (Int, v, [v]) -> -- all seen ids so far (the intermediate result)
  M.Map VertexId (Int, v, [v])
_buildList boundary fringe =
  _buildListGeneral boundary fringe expandVertexAsVertices

-- (internal)
-- Gathers the list of all the nodes connected through this graph
--
-- The expansion function in that case can be controlled.
--
-- The expansion is done in a DFS manner (the order of the node is unique).
_buildListGeneral :: (Show v, GraphVertexOperations v) =>
  S.Set VertexId -> -- boundary ids, they will not be traversed
  [v] -> -- fringe ids: the nodes that have been touched but not expanded.
  (v -> [v]) -> -- The expansion function. They will be the next nodes to expand.
  -- all seen ids so far (the intermediate result)
  -- along with the index of the node during the traversal, and the
  -- node itself.
  M.Map VertexId (Int, v, [v]) ->
  M.Map VertexId (Int, v, [v])
_buildListGeneral _ [] _ allSeen = allSeen
_buildListGeneral boundaryIds (x : t) expand allSeen =
  let vid = vertexToId x in
  if M.member vid allSeen || S.member vid boundaryIds then
    _buildListGeneral boundaryIds t expand allSeen
  else
    let nextVertices = expand x
        currIdx = M.size allSeen
        allSeen2 = M.insert vid (currIdx, x, nextVertices) allSeen
        filterFun y = not $ M.member (vertexToId y) allSeen2
        nextVertices2 = filter filterFun nextVertices
    in _buildListGeneral boundaryIds (nextVertices2 ++ t) expand allSeen2

{-| The sources of a DAG (nodes with no parent).
-}
graphSources :: Graph v e -> [Vertex v]
graphSources g =
  let hasParent = do
        vedges <- toList (gEdges g)
        edge <- toList vedges
        return . vertexId . veEndVertex $ edge
      hasPSet = S.fromList hasParent
      -- false iff the vertex has an incoming edge
      filt vx = not (S.member (vertexId vx) hasPSet)
  in filter filt (toList (gVertices g))

{-| The sinks of a graph (nodes with no descendant).
-}
graphSinks :: Graph v e -> [Vertex v]
graphSinks g =
  let f vx = V.null (M.findWithDefault V.empty (vertexId vx) (gEdges g))
  in filter f (toList (gVertices g))

-- | Flips the edges of this graph (it is also a DAG)
reverseGraph :: forall v e. Graph v e -> Graph v e
reverseGraph g =
  let
    vxMap = M.fromList ((vertexId &&& id) <$> toList (gVertices g))
    flipVEdge :: (VertexId, V.Vector (VertexEdge e v)) -> [(VertexId, VertexEdge e v)]
    flipVEdge (fromNid, vec) = case M.lookup fromNid vxMap of
      Nothing -> [] -- Should be a programming error
      Just endVx ->
        toList vec <&> \ve ->
          let ed = veEdge ve
              oldEndVx = veEndVertex ve
              oldEndVid = vertexId oldEndVx
              ed' = Edge {
                edgeFrom = oldEndVid,
                edgeTo = fromNid,
                edgeData = edgeData ed }
          in (oldEndVid, VertexEdge { veEdge = ed', veEndVertex = endVx })
    edges = myGroupBy $ concat $ flipVEdge <$> M.toList (gEdges g)
  in Graph (V.fromList <$> edges) (V.reverse (gVertices g))

-- | A generic transform over the graph that may account for potential failures
-- in the process.
graphMapVertices :: forall m v e v2. (HasCallStack, Show v2, Monad m) =>
  Graph v e -> -- The start graph
  (v -> [(v2,e)] -> m v2) -> -- The transform
  m (Graph v2 e)
graphMapVertices g f =
  let
    fun :: M.Map VertexId v2 -> [Vertex v] -> m [Vertex v2]
    fun _ [] = return []
    fun done (vx : t) =
      let
        vid = vertexId vx
        parents = V.toList $ fromMaybe V.empty $ M.lookup vid (gEdges g)
        parentEdges = veEdge <$> parents
        getPairs :: Edge e -> (v2, e)
        getPairs ed =
          let vidTo = edgeTo ed
              msg = sformat ("graphMapVertices: Could not locate "%shown%" in "%shown)vidTo done
              -- The edges are flowing from child -> parent so
              -- to == parent
              vert = fromMaybe (failure msg) (M.lookup vidTo done)
            in (vert, edgeData ed)
        parents2 = [getPairs ed | ed <- parentEdges]
        -- parents2 = [fromJust $ M.lookup vidFrom done | vidFrom <- parentVids]
        merge0 :: v2 -> m [Vertex v2]
        merge0 vx2Data =
          let done2 = M.insert vid vx2Data done
              vx2 = vx { vertexData = vx2Data }
              rest = fun done2 t in
            (vx2 : ) <$> rest
      in
        f (vertexData vx) parents2 >>= merge0
  in do
    verts2 <- fun M.empty (toList (gVertices g))
    let
      idxs2 = M.fromList [(vertexId vx2, vx2) | vx2 <- verts2]
      trans :: Vertex v -> Vertex v2
      trans vx = fromJust $ M.lookup (vertexId vx) idxs2
      conv :: VertexEdge e v -> VertexEdge e v2
      conv (VertexEdge vx1 e1) = VertexEdge (trans vx1) e1
      adj2 = M.map (conv <$>) (gEdges g)
    return Graph { gEdges = adj2, gVertices = V.fromList verts2 }

-- | (internal) Maps the edges
graphMapEdges :: Graph v e -> (e -> e') -> Graph v e'
graphMapEdges g f = graphFlatMapEdges g ((:[]) . f)

-- | (internal) Maps and the edges, and may create more or less.
graphFlatMapEdges :: Graph v e -> (e -> [e']) -> Graph v e'
graphFlatMapEdges g f = g { gEdges = edges } where
  fun (VertexEdge vx ed) =
    f (edgeData ed) <&> \ed' -> VertexEdge vx (ed { edgeData = ed' })
  edges = (V.fromList . concatMap fun) <$> gEdges g

-- | (internal) Maps the vertices.
graphMapVertices' :: (Show v, Show e, Show v') => (v -> v') -> Graph v e -> Graph v' e
graphMapVertices' f g =
  runIdentity (graphMapVertices g f') where
    f' v _ = return $ f v

{-| Given a graph, prunes out a subset of vertices.

All the corresponding edges and the unreachable chunks of the graph are removed.
-}
graphFilterVertices :: (Show v, Show e) =>
  (v -> FilterOp) -> Graph v e -> Graph v e
graphFilterVertices f g =
  -- Tag all the vertices that we are going to remove first.
  let f' v l = return $ _transFilter f v l
      g' = runIdentity (graphMapVertices g f')
      -- In a second step, directly remove all these elements from the graph.
      -- TODO: use more recent version of Vector.
      vxs = V.fromList $ mapMaybe _filt (V.toList (gVertices g'))
      keptIds = S.fromList $ V.toList (vertexId <$> vxs)
      eds = M.mapMaybeWithKey (_filtEdge keptIds) (gEdges g)
  -- We are guaranteed that the result is still a DAG.
  in Graph eds vxs


-- | The map of vertices, by vertex id.
vertexMap :: Graph v e -> M.Map VertexId v
vertexMap g =
  M.fromList . toList $ gVertices g <&> (vertexId &&& vertexData)

-- (internal)
-- The vertices in lexicographic order, and the originating edges for these
-- vertices.
verticesAndEdges :: Graph v e -> [([(v, e)],v)]
verticesAndEdges g =
  toList (gVertices g) <&> \vx ->
    let n = vertexData vx
        l = V.toList $ M.findWithDefault V.empty (vertexId vx) (gEdges g)
        lres = [(vertexData vx', edgeData e') | (VertexEdge vx' e') <- l]
    in (lres, n)

{-| Given a list of elements with vertex/edge information and a start vertex,
builds the graph from all the reachable vertices in the list.

It returns the vertices in a DAG traversal order.

Note that this function is robust and silently drops the missing vertices.
-}
pruneLexicographic :: VertexId -> [(VertexId, [VertexId], a)] -> [a]
pruneLexicographic hvid l =
  let f (vid, l', a) = (vid, (l', a))
      allVertices = myGroupBy (f <$> l)
      allVertices' = M.map head allVertices
  in reverse $ _pruneLexicographic allVertices' S.empty [hvid]

-- Recursive traversal of the graph, dropping everything that looks suspiscious.
_pruneLexicographic ::
  M.Map VertexId ([VertexId], a) ->
  S.Set VertexId ->
  [VertexId] ->
  [a]
_pruneLexicographic _ _ [] = []
_pruneLexicographic vertices visited (hvid : t) =
  if S.member hvid visited
  then _pruneLexicographic vertices visited t
  else case M.lookup hvid vertices of
    Just (l, x) ->
      x : _pruneLexicographic vertices (S.insert hvid visited) (l ++ t)
    Nothing ->
      _pruneLexicographic vertices visited t

_transFilter :: (v -> FilterOp) -> v -> [(FilterVertex v, e)] -> FilterVertex v
_transFilter filt vx l =
  let f (KeepVertex _, _) = True
      f (DropChildren _, _) = False
      f (RemoveVertex _, _) = False
      -- If the current node is reachable:
      -- If the node has no child, we do not make checks on the parents.
      -- (it is considered to be reachable)
      reachableChildren = null l || or (f <$> l)
  in if reachableChildren
      then case filt vx of
          Keep -> KeepVertex vx
          CutChildren -> DropChildren vx
          Remove -> RemoveVertex vx
      -- The node is unreachable, just drop
     else RemoveVertex vx

_filt :: Vertex (FilterVertex v) -> Maybe (Vertex v)
_filt (Vertex vid (KeepVertex v)) = Just (Vertex vid v)
_filt (Vertex vid (DropChildren v)) = Just (Vertex vid v)
_filt (Vertex _ (RemoveVertex _)) = Nothing


_filtEdge :: S.Set VertexId -> VertexId -> V.Vector (VertexEdge e v) -> Maybe (V.Vector (VertexEdge e v))
-- The start vertex has been pruned out.
_filtEdge s vid _ | not (S.member vid s) = Nothing
_filtEdge s _ v =
  let f ve = S.member (vertexId . veEndVertex $ ve) s
      v' = V.filter f v
  in if V.null v'
     then Nothing
     else Just v'

data FilterVertex v = KeepVertex !v | DropChildren !v | RemoveVertex !v deriving (Show)
