{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

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
  storeResults,
  updateCache
) where

import Control.Monad.State(get, put)
import Control.Monad(forM, when)
import Data.Text(pack)
import Data.Maybe(mapMaybe, catMaybes)
import Data.Either(isRight)
import Debug.Trace(trace)
import Data.Foldable(toList)
import Control.Arrow((&&&))
import Formatting
import qualified Data.Map.Strict as M
import qualified Data.HashMap.Strict as HM

import Spark.Core.Dataset
import Spark.Core.Try
import Spark.Core.Row
import Spark.Core.Types
import Spark.Core.StructuresInternal(NodeId, NodePath)
import Spark.Core.Internal.Caching
import Spark.Core.Internal.CachingUntyped
import Spark.Core.Internal.ContextStructures
import Spark.Core.Internal.Client
import Spark.Core.Internal.ComputeDag
import Spark.Core.Internal.PathsUntyped
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


-- -- The main function that takes a single output point and
-- -- tries to transform it as a valid computation.
-- prepareExecution1 :: LocalData a -> SparkStatePure (Try Computation)
-- prepareExecution1 ld = get >>= \session ->
--   let cg = buildComputationGraph ld
--       cg' = performGraphTransforms =<< cg
--       comp = _buildComputation session =<< cg'
--   in case comp of
--       Left _ -> return comp
--       Right _ -> do
--         _increaseCompCounter
--         return comp

{-| Given a context for the computation and a graph of computation, builds a
computation object.
-}
prepareComputation ::
  [(HdfsPath, DataInputStamp)] ->
  ComputeGraph ->
  SparkStatePure (Try Computation)
prepareComputation l cg = do
  -- Annotate the graph nodes with the stamp information
  let m = M.fromList l
    -- Only transform the sinks, since there is no writing.
  let inputs = cdInputs cg
  session <- get
  let compt = do
        inputs' <- sequence $ _updateVertex m <$> inputs
        let cg1 = cg { cdInputs = inputs' }
        -- TODO: prunning of all the computations already succesful
        -- Graph transforms
        cg2 <- performGraphTransforms cg1
        -- Build the computation
        _buildComputation session cg2
  when (isRight compt) _increaseCompCounter
  return compt

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
- deconstructions of the unions (in the future)

This could all be done on the server side at this point.
-}
performGraphTransforms :: ComputeGraph -> Try ComputeGraph
performGraphTransforms cg = do
  let g = traceHint "_performGraphTransforms g=" $ computeGraphToGraph cg
  let acg = traceHint "_performGraphTransforms: After autocaching:" $ fillAutoCache cachingType autocacheGen g
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
      terminalNodeNames = nodeName . vertexData <$> toList (cdOutputs tiedCg)
  -- TODO it is missing the first node here, hoping it is the first one.
  in case terminalNodeNames of
    [name] ->
      return $ Computation sid cid allNodes [name] name
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
getTargetNodes :: (HasCallStack) => Computation -> [UntypedLocalData]
getTargetNodes comp =
  let
    fun2 :: (HasCallStack) => UntypedNode -> UntypedLocalData
    fun2 n = case asLocalObservable <$> castLocality n of
      Right (Right x) -> x
      err -> failure $ sformat ("_getNodes:fun2: err="%shown%" n="%shown) err n
    finalNodeNames = traceHint "_getTargetNodes: finalNodeNames=" $cTerminalNodes comp
    dct = traceHint "_getTargetNodes: dct=" $ M.fromList $ (nodeName &&& id) <$> cNodes comp
    untyped' = finalNodeNames <&> \n ->
      let err = failure $ sformat ("Could not find "%sh%" in "%sh) n dct
      in M.findWithDefault err n dct
  in fun2 <$> untyped'

updateCache :: ComputationID -> [(NodeId, NodePath, PossibleNodeStatus)] -> SparkStatePure [(NodeId, FinalResult)]
updateCache c l = let s = sequence $ _updateCache1 c <$> l
  in catMaybes <$> s

_updateCache1 :: ComputationID -> (NodeId, NodePath, PossibleNodeStatus) -> SparkStatePure (Maybe (NodeId, FinalResult))
_updateCache1 cid (nid, p, status) = do
  session <- get
  let m = ssNodeCache session
  let up = case status of
        (NodeFinishedSuccess s) -> Just (NodeCacheSuccess, Right s)
        (NodeFinishedFailure e) -> Just (NodeCacheError, Left e)
        _ -> Nothing
  case up of
    Just (c, res) ->
      let v = NodeCacheInfo {
                nciStatus = c,
                nciComputation = cid,
                nciPath = p }
          m' = HM.insert nid v m
          session' = session { ssNodeCache = m' }
      in do
        put session'
        return (Just (nid, res))
    Nothing -> return Nothing


-- Stores the results of the computation in the state (so that we can accelerate the
-- next sessions) and returns the expected final results (as a Cell to be converted)
storeResults :: Computation -> [(UntypedLocalData, FinalResult)] -> SparkStatePure (Try Cell)
storeResults comp [] = return e where
  e = tryError $ sformat ("No result returned for computation "%shown) comp
storeResults _ res =
  let
    fun4 :: (LocalData Cell, FinalResult) -> Try Cell
    fun4 (node, fresult) =
      trace ("_storeResults node=" ++ show node ++ "final = " ++ show fresult) $
        _extract1 fresult (unSQLType (nodeType node))
    allResults = sequence $ forM res fun4
    expResult = head allResults -- Just accessing the final result for now
  in
    -- TODO store the results:
    return expResult
