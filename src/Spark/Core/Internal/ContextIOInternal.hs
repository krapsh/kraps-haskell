{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}

module Spark.Core.Internal.ContextIOInternal(
  returnPure,
  createSparkSession,
  createSparkSession',
  executeCommand1,
  executeCommand1',
  checkDataStamps,
  updateSourceInfo,
  createComputation,
  computationStats
) where

import Control.Concurrent(threadDelay)
import Control.Lens((^.))
import Control.Monad.State(mapStateT, get)
import Control.Monad(forM, forM_)
import Data.Aeson(toJSON, FromJSON)
import Data.Functor.Identity(runIdentity)
import Data.Text(Text, pack)
import qualified Data.Text as T
import qualified Network.Wreq as W
import Network.Wreq(responseBody)
import Control.Monad.Trans(lift)
import Control.Monad.Logger(runStdoutLoggingT, LoggingT, logDebugN, logInfoN, MonadLoggerIO)
import System.Random(randomIO)
import Data.Word(Word8)
import Data.Maybe(mapMaybe)
import Control.Monad.IO.Class
import GHC.Generics
-- import Formatting
import Network.Wreq.Types(Postable)
import Data.ByteString.Lazy(ByteString)
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS

import Spark.Core.Dataset
import Spark.Core.Internal.Client
import Spark.Core.Internal.ContextInternal
import Spark.Core.Internal.ContextStructures
import Spark.Core.Internal.DatasetFunctions(untypedLocalData, nodePath)
import Spark.Core.Internal.DatasetStructures(UntypedLocalData)
import Spark.Core.Internal.OpStructures(DataInputStamp(..))
import Spark.Core.Row
import Spark.Core.StructuresInternal
import Spark.Core.Try
import Spark.Core.Types
import Spark.Core.Internal.Utilities

returnPure :: forall a. SparkStatePure a -> SparkState a
returnPure p = lift $ mapStateT (return . runIdentity) p

{- | Creates a new Spark session.

This session is unique, and it will not try to reconnect to an existing
session.
-}
createSparkSession :: (MonadLoggerIO m) => SparkSessionConf -> m SparkSession
createSparkSession conf = do
  sessionName <- case confRequestedSessionName conf of
    "" -> liftIO _randomSessionName
    x -> pure x
  let session = _createSparkSession conf sessionName 0
  let url = _sessionEndPoint session
  logDebugN $ "Creating spark session at url: " <> url
  -- TODO get the current counter from remote
  _ <- _ensureSession session
  return session

{-| Convenience function for simple cases that do not require monad stacks.
-}
createSparkSession' :: SparkSessionConf -> IO SparkSession
createSparkSession' = _runLogger . createSparkSession

{- |
Executes a command:
- performs the transforms and the optimizations in the pure state
- sends the computation to the backend
- waits for the terminal nodes to reach a final state
- commits the final results to the state

If any failure is detected that is internal to Karps, it returns an error.
If the error comes from an underlying library (http stack, programming failure),
an exception may be thrown instead.
-}
executeCommand1 :: forall a. (FromSQL a) =>
  LocalData a -> SparkState (Try a)
executeCommand1 ld = do
    tcell <- executeCommand1' (untypedLocalData ld)
    return $ tcell >>= (tryEither . cellToValue)

-- The main function to launch computations.
executeCommand1' :: UntypedLocalData -> SparkState (Try Cell)
executeCommand1' ld = do
  logDebugN $ "executeCommand1': computing observable " <> show' ld
  -- Retrieve the computation graph
  let cgt = buildComputationGraph ld
  _ret cgt $ \cg -> do
    cgWithSourceT <- updateSourceInfo cg
    _ret cgWithSourceT $ \cgWithSource -> do
      -- Update the computations with the stamps, and build the computation.
      compt <- createComputation cgWithSource
      _ret compt $ \comp -> do
        -- Run the computation.
        session <- get
        _ <- _sendComputation session comp
        waitForCompletion comp

waitForCompletion :: Computation -> SparkState (Try Cell)
waitForCompletion comp = do
  -- We track all the observables, instead of simply the targets.
  let obss = getObservables comp
  let trackedNodes = obss <&> \n ->
        (nodeId n, nodePath n,
         unSQLType (nodeType n), nodePath n)
  nrs' <- _computationMultiStatus (cId comp) HS.empty trackedNodes
  -- Find the main result again in the list of everything.
  -- TODO: we actually do not need all the results, just target nodes.
  let targetNid = case cTerminalNodeIds comp of
        [nid] -> nid
        -- TODO: handle the case of multiple terminal targets
        l -> missing $ "waitForCompletion: missing multilist case with " <> show' l
  case filter (\z -> fst z == targetNid) nrs' of
    [(_, tc)] -> return tc
    l -> return $ tryError $ "Expected single result, got " <> show' l

{-| Exposed for debugging -}
computationStats ::
  ComputationID -> SparkState BatchComputationResult
computationStats cid = do
  logDebugN $ "computationStats: stats for " <> show' cid
  session <- get
  _computationStats session cid

{-| Exposed for debugging -}
createComputation :: ComputeGraph -> SparkState (Try Computation)
createComputation cg = returnPure $ prepareComputation cg

{-| Exposed for debugging -}
updateSourceInfo :: ComputeGraph -> SparkState (Try ComputeGraph)
updateSourceInfo cg = do
  let sources = inputSourcesRead cg
  if null sources
  then return (pure cg)
  else do
    logDebugN $ "updateSourceInfo: found sources " <> show' sources
    -- Get the source stamps. Any error at this point is considered fatal.
    stampsRet <- checkDataStamps sources
    logDebugN $ "updateSourceInfo: retrieved stamps " <> show' stampsRet
    let stampst = sequence $ _f <$> stampsRet
    let cgt = insertSourceInfo cg =<< stampst
    return cgt


_ret :: Try a -> (a -> SparkState (Try b)) -> SparkState (Try b)
_ret (Left x) _ = return (Left x)
_ret (Right x) f = f x

_f :: (a, Try b) -> Try (a, b)
_f (x, t) = case t of
                Right u -> Right (x, u)
                Left e -> Left e

data StampReturn = StampReturn {
  stampReturnPath :: !Text,
  stampReturnError :: !(Maybe Text),
  stampReturn :: !(Maybe Text)
} deriving (Eq, Show, Generic)

instance FromJSON StampReturn

{-| Given a list of paths, checks each of these paths on the file system of the
given Spark cluster to infer the status of these resources.

The primary role of this function is to check how recent these resources are
compared to some previous usage.
-}
checkDataStamps :: [HdfsPath] -> SparkState [(HdfsPath, Try DataInputStamp)]
checkDataStamps l = do
  session <- get
  let url = _sessionResourceCheck session
  status <- liftIO (W.asJSON =<< W.post (T.unpack url) (toJSON l) :: IO (W.Response [StampReturn]))
  let s = status ^. responseBody
  return $ mapMaybe _parseStamp s


_parseStamp :: StampReturn -> Maybe (HdfsPath, Try DataInputStamp)
_parseStamp sr = case (stampReturn sr, stampReturnError sr) of
  (Just s, _) -> pure (HdfsPath (stampReturnPath sr), pure (DataInputStamp s))
  (Nothing, Just err) -> pure (HdfsPath (stampReturnPath sr), tryError err)
  _ -> Nothing -- No error being returned for now, we just discard it.

_randomSessionName :: IO Text
_randomSessionName = do
  ws <- forM [1..10] (\(_::Int) -> randomIO :: IO Word8)
  let ints = (`mod` 10) <$> ws
  return . T.pack $ "session" ++ concat (show <$> ints)

type DefLogger a = LoggingT IO a

_runLogger :: DefLogger a -> IO a
_runLogger = runStdoutLoggingT

_post :: (MonadIO m, Postable a) =>
  Text -> a -> m (W.Response ByteString)
_post url = liftIO . W.post (T.unpack url)

_get :: (MonadIO m) =>
  Text -> m (W.Response ByteString)
_get url = liftIO $ W.get (T.unpack url)

-- TODO move to more general utilities
-- Performs repeated polling until the result can be converted
-- to a certain other type.
-- Int controls the delay in milliseconds between each poll.
_pollMonad :: (MonadIO m) => m a -> Int -> (a -> Maybe b) -> m b
_pollMonad rec delayMillis check = do
  curr <- rec
  case check curr of
    Just res -> return res
    Nothing -> do
      _ <- liftIO $ threadDelay (delayMillis * 1000)
      _pollMonad rec delayMillis check


-- Creates a new session from a string containing a session ID.
_createSparkSession :: SparkSessionConf -> Text -> Integer -> SparkSession
_createSparkSession conf sessionId idx =
  SparkSession conf sid idx HM.empty where
    sid = LocalSessionId sessionId

_port :: SparkSession -> Text
_port = pack . show . confPort . ssConf

-- The URL of the end point
_sessionEndPoint :: SparkSession -> Text
_sessionEndPoint sess =
  let port = _port sess
      sid = (unLocalSession . ssId) sess
  in
    T.concat [
      (confEndPoint . ssConf) sess, ":", port,
      "/sessions/", sid]

_sessionResourceCheck :: SparkSession -> Text
_sessionResourceCheck sess =
  let port = _port sess
      sid = (unLocalSession . ssId) sess
  in
    T.concat [
      (confEndPoint . ssConf) sess, ":", port,
      "/resources_status/", sid]

_sessionPortText :: SparkSession -> Text
_sessionPortText = pack . show . confPort . ssConf

-- The URL of the computation end point
_compEndPoint :: SparkSession -> ComputationID -> Text
_compEndPoint sess compId =
  let port = _sessionPortText sess
      sid = (unLocalSession . ssId) sess
      cid = unComputationID compId
  in
    T.concat [
      (confEndPoint . ssConf) sess, ":", port,
      "/computations/", sid, "/", cid]

-- The URL of the status of a computation
_compEndPointStatus :: SparkSession -> ComputationID -> Text
_compEndPointStatus sess compId =
  let port = _sessionPortText sess
      sid = (unLocalSession . ssId) sess
      cid = unComputationID compId
  in
    T.concat [
      (confEndPoint . ssConf) sess, ":", port,
      "/computations_status/", sid, "/", cid]

-- Ensures that the server has instantiated a session with the given ID.
_ensureSession :: (MonadLoggerIO m) => SparkSession -> m ()
_ensureSession session = do
  let url = _sessionEndPoint session <> "/create"
  _ <- _post url (toJSON 'a')
  return ()


_sendComputation :: (MonadLoggerIO m) => SparkSession -> Computation -> m ()
_sendComputation session comp = do
  let base' = _compEndPoint session (cId comp)
  let url = base' <> "/create"
  logInfoN $ "Sending computations at url: " <> url <> "with nodes: " <> show' (cNodes comp)
  _ <- _post url (toJSON (cNodes comp))
  return ()

_computationStatus :: (MonadLoggerIO m) =>
  SparkSession -> ComputationID -> NodePath -> m PossibleNodeStatus
_computationStatus session compId npath = do
  let base' = _compEndPointStatus session compId
  let rest = prettyNodePath npath
  let url = base' <> rest
  _ <- _get url
  status <- liftIO (W.asJSON =<< W.get (T.unpack url) :: IO (W.Response PossibleNodeStatus))
  let s = status ^. responseBody
  return s

-- TODO: not sure how this works when trying to make a fix point: is it going to
-- blow up the 'stack'?
_computationMultiStatus ::
   -- The computation being run
  ComputationID ->
  -- The set of nodes that have been processed in this computation, and ended
  -- with a success.
  -- TODO: should we do all the nodes processed in this computation?
  HS.HashSet NodeId ->
  -- The list of nodes for which we have not had completion information so far.
  [(NodeId, NodePath, DataType, NodePath)] ->
  SparkState [(NodeId, Try Cell)]
_computationMultiStatus _ _ [] = return []
_computationMultiStatus cid done l = do
  session <- get
  -- Find the nodes that still need processing (i.e. that have not previously
  -- finished with a success)
  let f (nid, _, _, _) = not $ HS.member nid done
  let needsProcessing = filter f l
  -- Poll a bunch of nodes to try to get a status update.
  let statusl = _try (_computationStatus session cid) <$> needsProcessing :: [SparkState (NodeId, NodePath, DataType, PossibleNodeStatus)]
  status <- sequence statusl
  -- Update the state with the new data
  (updated, statusUpdate) <- returnPure $ updateCache cid status
  forM_ statusUpdate $ \(p, s) -> case s of
      NodeCacheSuccess ->
        logInfoN $ "_computationMultiStatus: " <> prettyNodePath p <> " finished"
      NodeCacheError ->
        logInfoN $ "_computationMultiStatus: " <> prettyNodePath p <> " finished (ERROR)"
      NodeCacheRunning ->
        logInfoN $ "_computationMultiStatus: " <> prettyNodePath p <> " running"
  -- Filter out the updated nodes, so that we do not ask for them again.
  let updatedNids = HS.union done (HS.fromList (fst <$> updated))
  let g (nid, _, _, _) = not $ HS.member nid updatedNids
  let stillNeedsProcessing = filter g needsProcessing
  -- Do not block uselessly if we have nothing else to do
  if null stillNeedsProcessing
  then return updated
  else do
    let delayMillis = confPollingIntervalMillis $ ssConf session
    _ <- liftIO $ threadDelay (delayMillis * 1000)
    -- TODO: this chaining is certainly not tail-recursive
    -- How much of a memory leak is it?
    reminder <- _computationMultiStatus cid updatedNids stillNeedsProcessing
    return $ updated ++ reminder

_try :: (Monad m) => (y -> m z) -> (x, x', x'', y) -> m (x, x', x'', z)
_try f (x, x', x'', y) = f y <&> \z -> (x, x', x'', z)

_computationStats :: (MonadLoggerIO m) =>
  SparkSession -> ComputationID -> m BatchComputationResult
_computationStats session compId = do
  let url = _compEndPointStatus session compId <> "/" -- The final / is mandatory
  logDebugN $ "Sending computations stats request at url: " <> url
  stats <- liftIO (W.asJSON =<< W.get (T.unpack url) :: IO (W.Response BatchComputationResult))
  let s = stats ^. responseBody
  return s


_waitSingleComputation :: (MonadLoggerIO m) =>
  SparkSession -> Computation -> NodePath -> m FinalResult
_waitSingleComputation session comp npath =
  let
    extract :: PossibleNodeStatus -> Maybe FinalResult
    extract (NodeFinishedSuccess (Just s) _) = Just $ Right s
    extract (NodeFinishedFailure f) = Just $ Left f
    extract _ = Nothing
    getStatus = _computationStatus session (cId comp) npath
    i = confPollingIntervalMillis $ ssConf session
  in
    _pollMonad getStatus i extract
