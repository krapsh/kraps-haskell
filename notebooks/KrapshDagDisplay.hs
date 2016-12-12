{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}

-- This module is meant to be loaded from the IHaskell REPL.
-- TODO move this into a subpackage
module KrapshDagDisplay where

import qualified Data.Text as T
import qualified Data.Text.Lazy as L
import qualified Data.Map.Strict as M
import qualified Data.Vector as V
import Data.Hashable
import Formatting

import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.DAGFunctions
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.OpFunctions
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.ContextStructures
import Spark.Core.Internal.ComputeDag
import Spark.Core.Internal.ContextInternal
import Spark.Core.StructuresInternal
import Spark.Core.Try


data DisplayEdge = DirectEdge | DependencyEdge deriving (Eq, Show)

data DisplayNode = DisplayNode {
  dnPath :: !NodePath,
  dnOp :: !T.Text,
  dnAttributes :: !(M.Map T.Text T.Text)
  } deriving (Eq, Show)

type DisplayGraph = Graph DisplayNode DisplayEdge

data ExportNode = ExportNode {
  enName :: !T.Text,
  enOp :: !T.Text,
  enDeps :: ![T.Text],
  enLogicalDeps :: ![T.Text],
  enAttributes :: ![(T.Text, T.Text)]
} deriving (Show)

newtype DisplayComputeNode = DisplayComputeNode UntypedNode

instance Show DisplayComputeNode where
  show (DisplayComputeNode cn) = show . simpleShowOp . nodeOp $ cn

instance GraphVertexOperations DisplayComputeNode where
  vertexToId (DisplayComputeNode n) = VertexId . unNodeId . _cnNodeId $ n
  expandVertexAsVertices (DisplayComputeNode n) =
    DisplayComputeNode <$> (V.toList . _cnParents $ n)

instance GraphOperations DisplayComputeNode DisplayEdge where
  expandVertex (DisplayComputeNode n) =
    [(DirectEdge, DisplayComputeNode n') | n' <- V.toList (_cnParents n)]

nodeToDisplayGraph :: ComputeNode loc a -> Try DisplayGraph
nodeToDisplayGraph cn =
  let cg = buildComputationGraph cn
  in computeGraphToDisplayGraph <$> cg

nodeToDisplayNode :: UntypedNode -> DisplayNode
nodeToDisplayNode cn' =
  let nm = nodePath cn'
      op = simpleShowOp . nodeOp $ cn'
      tp = T.pack . show . nodeType $ cn'
      loc = if _cnLocality cn' == Local then "local" else "distributed"
      dct = M.fromList [("sqlType", tp),("locality", loc)]
  in DisplayNode nm op dct

computeGraphToDisplayGraph :: ComputeGraph -> DisplayGraph
computeGraphToDisplayGraph cg =
  let g = computeGraphToGraph cg
      f :: StructureEdge -> DisplayEdge
      f ParentEdge = DirectEdge
      f LogicalEdge = DependencyEdge
      g1 = graphMapEdges g f
    in (graphMapVertices' nodeToDisplayNode) g1


exportNodes :: DisplayGraph -> [ExportNode]
exportNodes g =
  verticesAndEdges g <&> \(l, n) ->
    let name = catNodePath (dnPath n)
        deps = [catNodePath (dnPath np) | (np, e) <- l, e == DirectEdge]
        logical = [catNodePath (dnPath np) | (np, e) <- l, e == DependencyEdge]
    in ExportNode {
         enName = name,
         enOp = dnOp n,
         enDeps = deps,
         enLogicalDeps = logical,
         enAttributes = M.toList $ dnAttributes n
       }

_attributes :: [(T.Text, T.Text)] -> L.Text
_attributes dct =
  let f (k,v) = L.concat [
          "  attr {\n",
          "    key: \"", L.fromStrict k, "\"\n",
          "    value {\n",
          "      val: ", L.pack (show v), "\n",
          "    }\n",
          "  }\n"
        ]
  in L.concat $ f <$> dct


_exportNodeData :: [ExportNode] -> T.Text
_exportNodeData l = L.toStrict . L.concat $ l <&> \n ->
  L.concat [
    "node {", "\n",
    "  name: ", L.pack . show . enName $ n, "\n",
    "  op: ", L.pack . show . enOp $ n, "\n",
    L.concat $ enDeps n <&> \d ->
      L.concat [ "  input: \"", L.fromStrict d, "\"\n"],
    L.concat $ enLogicalDeps n <&> \d ->
      L.concat [ "  input: \"^", L.fromStrict d, ":0\"", "\n"],
    _attributes $ enAttributes n,
    "}", "\n"
  ]


_script :: (Show a) => String -> a -> T.Text
_script elemid pbtxt =
  sformat ("<script>\
          \function load() {{\
          \  document.getElementById(\""%string%"\").pbtxt = "%string%";\
          \}}\
        \</script>\
        \<link rel=\"import\" href=\"https://tensorboard.appspot.com/tf-graph-basic.build.html\" onload=load()>\
        \<div style=\"height:600px\">\
          \<tf-graph-basic id=\""%string%"\"></tf-graph-basic>\
        \</div>") key (show pbtxt) key where
          key = elemid


tfIFrame :: [ExportNode] -> T.Text
tfIFrame ns =
  let txt = _exportNodeData ns
      h = hash txt
  in T.concat [
  "<iframe seamless style='width:900px;height:620px;border:0' srcdoc='"
  , _script (show h) txt
  , "'></iframe>"
  ]
