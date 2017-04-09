

module Spark.Core.Internal.ComputeDag where

import Data.Foldable(toList)
import qualified Data.Map.Strict as M
import qualified Data.Vector as V
import Data.Vector(Vector)

import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.DAGFunctions

{-| A DAG of computation nodes.

At a high level, it is a total function with a number of inputs and a number
of outputs.

Note about the edges: the edges flow along the path of dependencies:
the inputs are the start points, and the outputs are the end points of the
graph.

-}
data ComputeDag v e = ComputeDag {
  -- The edges that make up the DAG
  cdEdges :: !(AdjacencyMap v e),
  -- All the vertices of the graph
  -- Sorted by lexicographic order + node id for uniqueness
  cdVertices :: !(Vector (Vertex v)),
  -- The inputs of the computation graph. These correspond to the
  -- sinks of the dependency graph.
  cdInputs :: !(Vector (Vertex v)),
  -- The outputs of the computation graph. These correspond to the
  -- sources of the dependency graph.
  cdOutputs :: !(Vector (Vertex v))
} deriving (Show)


-- | Conversion
computeGraphToGraph :: ComputeDag v e -> Graph v e
computeGraphToGraph cg =
  Graph (cdEdges cg) (cdVertices cg)

-- | Conversion
graphToComputeGraph :: Graph v e -> ComputeDag v e
graphToComputeGraph g =
  ComputeDag {
    cdEdges = gEdges g,
    cdVertices = gVertices g,
    -- We work on the graph of dependencies (not flows)
    -- The sources correspond to the outputs.
    cdInputs = V.fromList $ graphSinks g,
    cdOutputs = V.fromList $ graphSources g
  }

_mapVerticesAdj :: (Vertex v -> v') -> AdjacencyMap v e -> AdjacencyMap v' e
_mapVerticesAdj f m =
  let f1 ve =
        let vx = veEndVertex ve
            d' = f vx in
          ve { veEndVertex = vx { vertexData = d' } }
      f' v = f1 <$> v
  in M.map f' m

mapVertices :: (Vertex v -> v') -> ComputeDag v e -> ComputeDag v' e
mapVertices f cd =
  let f' vx = vx { vertexData = f vx }
  in ComputeDag {
      cdEdges = _mapVerticesAdj f (cdEdges cd),
      cdVertices = f' <$> cdVertices cd,
      cdInputs = f' <$> cdInputs cd,
      cdOutputs = f' <$> cdOutputs cd
    }

mapVertexData :: (v -> v') -> ComputeDag v e -> ComputeDag v' e
mapVertexData f = mapVertices (f . vertexData)

buildCGraph :: (GraphOperations v e, Show v) =>
  v -> DagTry (ComputeDag v e)
buildCGraph n = graphToComputeGraph <$> buildGraph n

graphDataLexico :: ComputeDag v e -> [v]
graphDataLexico cd = vertexData <$> toList (cdVertices cd)
