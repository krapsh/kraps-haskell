{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}

-- This module is meant to be loaded from the IHaskell REPL.
-- TODO move this into a subpackage
module KarpsDagDisplay where

import qualified Data.Text as T
import qualified Data.Text.Lazy as L
import qualified Data.Map.Strict as M
import qualified Data.Vector as V
import qualified Data.Set as S
import qualified Data.ByteString.Char8 as C8
import Data.Maybe(mapMaybe, catMaybes)
import Data.Hashable
import IHaskell.Display
import Formatting
import Control.Arrow((&&&))

import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.DAGFunctions
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.Client
import Spark.Core.Internal.ContextInteractive(computationStatsDef)
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
      nid = show' $ nodeId cn'
      dct = M.fromList [("sqlType", tp),("locality", loc), ("id", nid)]
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

-- TODO: this code uses unsafe functions...
statsToExportNodes :: BatchComputationResult -> [ExportNode]
statsToExportNodes (BatchComputationResult _ l) =
  -- Just get the RDD info
  let
    f (np, _, NodeFinishedSuccess _ (Just (SparkComputationItemStats l'))) = [(np, x) | x <- l']
    f _ = []
    xs = concat $ f <$> l
    -- Find the mapping from RDD id -> path
    f2 (np, rddinfo) =
      let id' = unRDDId (rddiId rddinfo)
          rddName = rddiClassName rddinfo <> "-" <> show' id'
          p = catNodePath np <> "/" <> rddName
      in (rddiId rddinfo, p)
    paths = M.map head $ myGroupBy (f2 <$> xs)
    -- Find a start point for each of the observed nodes
    isObserved (np, _, NodeFinishedSuccess (Just _) (Just (SparkComputationItemStats l'))) = [(np, rddiId x) | x <- l']
    isObserved _ = []
    observedPathsWithRDDIds = myGroupBy $ concat $ isObserved <$> l
    -- Use the maximum RDD id as the start point (which corresponds to the last created RDD)
    observedBestMap = traceHint "observedBestMap=" $ M.map maximum observedPathsWithRDDIds
    observedBestIds = snd <$> M.toList observedBestMap
    observedBestPaths = catMaybes [M.lookup rid paths | rid <- observedBestIds]
    makeVertexId = VertexId . C8.pack . T.unpack
    f3 rinfo =
      let p' = paths M.! rddiId rinfo
          ps = catMaybes $ rddiParents rinfo <&> \rid -> M.lookup rid paths
      in ExportNode {
           enName = p',
           enOp = rddiClassName rinfo,
           enDeps = ps,
           enLogicalDeps = [],
           enAttributes = [("name", rddiRepr rinfo)]
          }
    toNode en = (makeVertexId (enName en), makeVertexId <$> enDeps en, en)
    -- Build all the export nodes
    nodes = f3 . snd <$> xs
    ens = toNode <$> nodes
    -- Post-processing 1: keep only the nodes that are tied to observations
    tiedPaths = S.fromList $ enName <$> concat (observedBestPaths <&> \p ->
      pruneLexicographic (makeVertexId p) ens)
    reachableNodes = traceHint "reachableNodes" $ filter (\x -> S.member (enName x) tiedPaths) nodes
    -- Post-processing 2: add logical dependency edges between blocks that
    -- seem unconnected, but for which we know there exists a dependency.
    -- The observables break naturally by their very nature of collecting.
    -- Compute the existing dependencies between nodes.

    -- TODO this is an approximation: these ids may be dropped during
    -- the pruning (unlike the best ids above)
    observedFirstIds = traceHint "observedFirstIds" $ M.map minimum observedPathsWithRDDIds
    txtToPath :: T.Text -> (NodePath, RDDId)
    txtToPath x =
      let s = T.splitOn "/" x
          x2 = read (T.unpack . last $ T.splitOn "-" (last s)) :: Int
          p = NodePath . V.fromList $ (NodeName <$> init s)
      in (p, RDDId x2)
    existingDeps = S.fromList $ concat $ reachableNodes <&> \n ->
      let from' = fst . txtToPath . enName $ n
          to' = fst . txtToPath <$> enDeps n
      in [(from', to'') | to'' <- to']
    allDeps = traceHint "allDeps=" $ concat $ l <&> \(np, depsNp, _) -> (const np &&& id) <$> depsNp
    allTransDepsMap = traceHint "allTransDeps=" $ _subsetTransitive (myGroupBy allDeps) (M.keysSet observedPathsWithRDDIds)
    allTransDeps = [(f',t) | (f', ts) <- M.toList allTransDepsMap, t <- ts]
    missingDeps = traceHint "missingDeps=" $ filter (\z -> not (S.member z existingDeps)) allTransDeps
    missingDepsMap = traceHint "missingDepsMap=" $ myGroupBy $ missingDeps <&> \(f', t) ->
      let startId = observedFirstIds M.! f'
          endId = observedBestMap M.! t
          fp = paths M.! startId
          tp = paths M.! endId
      in (fp, tp)
    reachableNodesWithDeps = reachableNodes <&> \n ->
      case M.lookup (enName n) missingDepsMap of
        Just l' -> n { enLogicalDeps = l' }
        Nothing -> n
  in reachableNodesWithDeps

-- Given a set of nodes forming dependencies and a subset of these nodes,
-- makes the graph that corresponds to pruning out all the other nodes
-- and following the transitive dependencies.
_subsetTransitive :: (Ord a) => M.Map a [a] -> S.Set a -> M.Map a [a]
_subsetTransitive m stops =
  M.fromList $ S.toList stops <&> \x ->
      let deps = _expand m stops S.empty (M.findWithDefault [] x m)
      in (x, S.toList deps)


_expand :: (Ord a) => M.Map a [a] -> S.Set a -> S.Set a -> [a] -> S.Set a
_expand _ _ seen [] = seen
_expand deps stops seen (h : t) =
  if S.member h seen
  then _expand deps stops seen t
  else
    if S.member h stops
    then _expand deps stops (S.insert h seen) t
    else _expand deps stops seen (M.findWithDefault [] h deps ++ t)

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
  \function mycleanup1() {\
    \var docs = document.getElementsByClassName(\"side tf-graph-basic\");\
    \var n = docs.length;\
    \for (var i=0; i < n; i++) {\
      \var x = docs[i];\
      \if (x.style.display === \"\") {\
        \x.style.display = \"none\";\
      \}\
    \}\
  \};\
  \function mycleanup2() {\
    \var docs = document.getElementsByClassName(\"main tf-graph-basic\");\
    \var n = docs.length;\
    \for (var i=0; i < n; i++) {\
      \var x = docs[i];\
      \if (x.style.left !== 0) {\
        \x.style.left = 0;\
      \}\
    \}\
  \};\
          \function load() {{\
          \  document.getElementById(\""%string%"\").pbtxt = "%string%";\
          \  setInterval(mycleanup1, 500);\
          \  setInterval(mycleanup2, 500);\
          \}}\
        \</script>\
        \<link rel=\"import\" href=\"https://tensorboard.appspot.com/tf-graph-basic.build.html\" onload=load() onscroll=scroll()>\
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

-- | A small function to display the RDD content of a node.
displayRDD cid = do
    stats <- computationStatsDef (ComputationID cid)
    let ns = statsToExportNodes stats
    let c = T.unpack (tfIFrame ns)
    return $ Display [html c]
