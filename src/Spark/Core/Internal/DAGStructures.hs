{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

{-| Data structures to represent Directed Acyclic Graphs (DAGs).

-}
module Spark.Core.Internal.DAGStructures where

import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Data.ByteString(ByteString)
import Data.Vector(Vector)
import Data.Foldable(toList)
import Data.Hashable(Hashable)
import GHC.Generics(Generic)
import Formatting

import Spark.Core.Internal.Utilities

-- | The unique ID of a vertex.
newtype VertexId = VertexId { unVertexId :: ByteString } deriving (Eq, Ord, Generic)


-- | An edge in a graph, parametrized by some payload.
data Edge e = Edge {
  edgeFrom :: !VertexId,
  edgeTo :: !VertexId,
  edgeData :: !e
}

-- | A vertex in a graph, parametrized by some payload.
data Vertex v = Vertex {
  vertexId :: !VertexId,
  vertexData :: !v
}

{-| An edge, along with its end node.
-}
data VertexEdge e v = VertexEdge {
    veEndVertex :: !(Vertex v),
    veEdge :: !(Edge e) }

{-| The adjacency map of a graph.

The node Id corresponds to the start node, the pairs are the end node and
and the edge to reach to the node. There may be multiple edges leading to the
same node.
-}
type AdjacencyMap v e = M.Map VertexId (Vector (VertexEdge e v))

-- | The representation of a graph.
--
-- In all the project, it is considered as a DAG.
data Graph v e = Graph {
  gEdges :: !(AdjacencyMap v e),
  gVertices :: !(Vector (Vertex v))
}

-- | Graph operations on types that are supposed to
-- represent vertices.
class GraphVertexOperations v where
  vertexToId :: v -> VertexId
  expandVertexAsVertices :: v -> [v]

-- | Graph operations on types that are supposed to represent
-- edges.
class (GraphVertexOperations v) => GraphOperations v e where
  expandVertex :: v -> [(e,v)]

instance Functor Vertex where
  fmap f vx = vx { vertexData = f (vertexData vx) }

instance Functor Edge where
  fmap f ed = ed { edgeData = f (edgeData ed) }

instance (Show v) => Show (Vertex v) where
  show vx = "Vertex(vId=" ++ show (vertexId vx) ++ " v=" ++ show (vertexData vx) ++ ")"

instance (Show e) => Show (Edge e) where
  show ed = "Edge(from=" ++ show (edgeFrom ed) ++ " to=" ++ show (edgeTo ed) ++ " e=" ++ show (edgeData ed) ++ ")"

instance (Show v, Show e) => Show (VertexEdge e v) where
  show (VertexEdge v e) = "(" ++ show v ++ ", " ++ show e ++ ")"

instance (Show v, Show e) => Show (Graph v e) where
  show g =
    let vxs = toList $ gVertices g <&> \(Vertex vid x) ->
          sformat (sh%":"%sh) vid x
        vedges = foldMap toList (M.elems (gEdges g))
        edges = (veEdge <$> vedges) <&> \(Edge efrom eto x) ->
          sformat (sh%"->"%sh%"->"%sh) efrom x eto
        -- eds = (M.elems (gEdges g)) `foldMap` \v ->
        --   (toList v) <&>
        vxs' = T.intercalate "," vxs
        eds' = T.intercalate " " edges
        str = T.concat ["Graph{", vxs', ", ", eds', "}"]
    in T.unpack str

instance Hashable VertexId

instance Show VertexId where
  show (VertexId bs) = let s = show bs in
    if length s > 9 then
      (drop 1 . take 6) s ++ ".."
    else
      s
