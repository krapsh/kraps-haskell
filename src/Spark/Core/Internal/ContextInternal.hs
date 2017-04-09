{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TupleSections #-}

-- Functions to build the graph of computations.
-- The following steps are performed:
--  - typing checking
--  - caching checks
--  - building the final json
--
-- All the functions in this module are pure and use SparkStatePure for transforms.

module Spark.Core.Internal.ContextInternal(
  FinalResult,
  inputSourcesRead,
  prepareComputation,
  buildComputationGraph,
  performGraphTransforms,
  getTargetNodes,
  getObservables,
  insertSourceInfo,
  updateCache
) where

import Control.Monad.State(get, put)
import Control.Monad(when)
import Data.Text(pack)
import Data.Maybe(mapMaybe, catMaybes)
import Data.Either(isRight)
import Data.Foldable(toList)
import Control.Arrow((&&&))
import Formatting
import qualified Data.Map.Strict as M
import qualified Data.HashMap.Strict as HM

import Spark.Core.Dataset
import Spark.Core.Try
import Spark.Core.Row
import Spark.Core.Types
import Spark.Core.StructuresInternal(NodeId, NodePath, ComputationID(..))
import Spark.Core.Internal.Caching
import Spark.Core.Internal.CachingUntyped
import Spark.Core.Internal.ContextStructures
import Spark.Core.Internal.Client
import Spark.Core.Internal.ComputeDag
import Spark.Core.Internal.PathsUntyped
import Spark.Core.Internal.Pruning
import Spark.Core.Internal.OpFunctions(hdfsPath, updateSourceStamp)
import Spark.Core.Internal.OpStructures(HdfsPath(..), DataInputStamp)
-- Required to import the instances.
import Spark.Core.Internal.Paths()
import Spark.Core.Internal.DAGFunctions(buildVertexList)
import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.Utilities

-- The result from querying the status of a computation
type FinalResult = Either NodeComputationFailure NodeComputationSuccess

{-| Given a context for the computation and a graph of computation, builds a
computation object.
-}
prepareComputation ::
  ComputeGraph ->
  SparkStatePure (Try Computation)
prepareComputation cg = do
  session <- get
  let compt = do
        cg2 <- performGraphTransforms session cg
        -- Build the computation
        _buildComputation session cg2
  when (isRight compt) _increaseCompCounter
  return compt

{-| Exposed for debugging -}
insertSourceInfo :: ComputeGraph -> [(HdfsPath, DataInputStamp)] -> Try ComputeGraph
insertSourceInfo cg l = do
  let m = M.fromList l
      -- Only transform the sinks, since there is no writing.
  let inputs = cdInputs cg
  inputs' <- sequence $ _updateVertex m <$> inputs
  return $ cg { cdInputs = inputs' }



{-| A list of file sources that are being requested by the compute graph -}
inputSourcesRead :: ComputeGraph -> [HdfsPath]
inputSourcesRead cg =
  -- TODO: make unique elements
  mapMaybe (hdfsPath.nodeOp.vertexData) (toList (cdVertices cg))

-- Here are the steps being run
--  - node collection + cycle detection
--  - naming:
--    -> everything after that can be done with names, and on server
--    -> for convenience, the vertex ids will be still the hash ids
--  - verification of cache/uncache
--  - deconstruction of unions and aggregations
--  - caching swap
--
-- There is a lot more that could be done (merging the aggregations, etc.)
-- but it is outside the scope of this MVP.
-- TODO: should graph pruning be moved before naming?

{-| Builds the computation graph by expanding a single node until a transitive
closure is reached.

It performs the naming, node deduplication and cycle detection.

TODO(kps) use the caching information to have a correct fringe
-}
buildComputationGraph :: ComputeNode loc a -> Try ComputeGraph
buildComputationGraph ld = do
  cg <- tryEither $ buildCGraph (untyped ld)
  assignPathsUntyped cg

{-| Performs all the operations that are done on the compute graph:

- fullfilling autocache requests
- checking the cache/uncache pairs
- pruning of observed successful computations
- deconstructions of the unions (in the future)

This could all be done on the server side at this point.
-}
performGraphTransforms :: SparkSession -> ComputeGraph -> Try ComputeGraph
performGraphTransforms session cg = do
  let g = computeGraphToGraph cg -- traceHint "_performGraphTransforms g=" $
  let conf = ssConf session
  let pruned = if confUseNodePrunning conf
               then pruneGraphDefault (ssNodeCache session) g
               else g
  let acg = fillAutoCache cachingType autocacheGen pruned -- traceHint "_performGraphTransforms: After autocaching:" $
  g' <- tryEither acg
  failures <- tryEither $ checkCaching g' cachingType
  case failures of
    [] -> return (graphToComputeGraph g')
    _ -> tryError $ sformat ("Found some caching errors: "%sh) failures

_buildComputation :: SparkSession -> ComputeGraph -> Try Computation
_buildComputation session cg =
  let sid = ssId session
      cid = (ComputationID . pack . show . ssCommandCounter) session
      tiedCg = tieNodes cg
      allNodes = vertexData <$> toList (cdVertices tiedCg)
      terminalNodes = vertexData <$> toList (cdOutputs tiedCg)
      terminalNodePaths = nodePath <$> terminalNodes
      terminalNodeIds = nodeId <$> terminalNodes
  -- TODO it is missing the first node here, hoping it is the first one.
  in case terminalNodePaths of
    [p] ->
      return $ Computation sid cid allNodes [p] p terminalNodeIds
    _ -> tryError $ sformat ("Programming error in _build1: cg="%sh) cg

_updateVertex :: M.Map HdfsPath DataInputStamp -> Vertex UntypedNode -> Try (Vertex UntypedNode)
_updateVertex m v =
  let un = vertexData v
      no = nodeOp un in case hdfsPath no of
    Just p -> case M.lookup p m of
      Just dis -> updateSourceStamp no dis <&> \no' ->
          v { vertexData = updateNodeOp un no' }
      Nothing -> tryError $ "_updateVertex: Expected to find path " <> show' p
    Nothing -> pure v

_increaseCompCounter :: SparkStatePure ()
_increaseCompCounter = get >>= \session ->
  let
    curr = ssCommandCounter session
    session2 = session { ssCommandCounter =  curr + 1 }
  in put session2

-- Given an end point, gathers all the nodes reachable from there.
_gatherNodes :: LocalData a -> Try [UntypedNode]
_gatherNodes = tryEither . buildVertexList . untyped

-- Given a result, tries to build the corresponding object out of it
_extract1 :: FinalResult -> DataType -> Try Cell
_extract1 (Left nf) _ = tryError $ sformat ("got an error "%shown) nf
_extract1 (Right ncs) dt = tryEither $ jsonToCell dt (ncsData ncs)

-- Gets the relevant nodes for this computation from this spark session.
-- The computation is assumed to be correct and to contain all the nodes
-- already.
-- TODO: make it a total function
-- TODO: this is probably not needed anymore
getTargetNodes :: (HasCallStack) => Computation -> [UntypedLocalData]
getTargetNodes comp =
  let
    fun2 :: (HasCallStack) => UntypedNode -> UntypedLocalData
    fun2 n = case asLocalObservable <$> castLocality n of
      Right (Right x) -> x
      err -> failure $ sformat ("_getNodes:fun2: err="%shown%" n="%shown) err n
    finalNodeNames = cTerminalNodes comp
    dct = M.fromList $ (nodePath &&& id) <$> cNodes comp
    untyped2 = finalNodeNames <&> \n ->
      let err = failure $ sformat ("Could not find "%sh%" in "%sh) n dct
      in M.findWithDefault err n dct
  in fun2 <$> untyped2

{-| Retrieves all the observables from a computation.
-}
getObservables :: Computation -> [UntypedLocalData]
getObservables comp =
  let fun n = case asLocalObservable <$> castLocality n of
          Right (Right x) -> Just x
          _ -> Nothing
  in catMaybes $ fun <$> cNodes comp

{-| Updates the cache, and returns the updates if there are any.

The updates are split into final results, and general update status (scheduled,
running, etc.)
-}
updateCache :: ComputationID -> [(NodeId, NodePath, DataType, PossibleNodeStatus)] -> SparkStatePure ([(NodeId, Try Cell)], [(NodePath, NodeCacheStatus)])
updateCache c l = do
  l' <- sequence $ _updateCache1 c <$> l
  return (catMaybes (fst <$> l'), catMaybes (snd <$> l'))

_updateCache1 :: ComputationID -> (NodeId, NodePath, DataType, PossibleNodeStatus) -> SparkStatePure (Maybe (NodeId, Try Cell), Maybe (NodePath, NodeCacheStatus))
_updateCache1 cid (nid, p, dt, status) =
  case status of
    (NodeFinishedSuccess (Just s) _) -> do
      updated <- _insertCacheUpdate cid nid p NodeCacheSuccess
      let res2 = _extract1 (pure s) dt
      return (Just (nid, res2), (p, ) <$> updated)
    (NodeFinishedFailure e) -> do
      updated <- _insertCacheUpdate cid nid p NodeCacheError
      let res2 = _extract1 (Left e) dt
      return (Just (nid, res2), (p, ) <$> updated)
    NodeRunning -> do
      updated <- _insertCacheUpdate cid nid p NodeCacheRunning
      return (Nothing, (p, ) <$> updated)
    _ -> return (Nothing, Nothing)

-- Returns true if the cache is updated
_insertCacheUpdate :: ComputationID -> NodeId -> NodePath -> NodeCacheStatus -> SparkStatePure (Maybe NodeCacheStatus)
_insertCacheUpdate cid nid p s = do
  session <- get
  let m = ssNodeCache session
  let currentStatus = nciStatus <$> HM.lookup nid m
  if currentStatus == Just s
  then return Nothing
  else do
    let v = NodeCacheInfo {
              nciStatus = s,
              nciComputation = cid,
              nciPath = p }
    let m' = HM.insert nid v m
    let session' = session { ssNodeCache = m' }
    put session'
    return $ Just s
