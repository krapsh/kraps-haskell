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
  checkDataStamps
) where

import Control.Concurrent(threadDelay)
import Control.Lens((^.))
import Control.Monad.State(mapStateT, get)
import Control.Monad(forM)
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

import Spark.Core.Dataset
import Spark.Core.Internal.Client
import Spark.Core.Internal.ContextInternal
import Spark.Core.Internal.ContextStructures
import Spark.Core.Internal.DatasetFunctions(untypedLocalData)
import Spark.Core.Internal.DatasetStructures(UntypedLocalData)
import Spark.Core.Internal.OpStructures(DataInputStamp(..))
import Spark.Core.Row
import Spark.Core.StructuresInternal
import Spark.Core.Try
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

If any failure is detected that is internal to Krapsh, it returns an error.
If the error comes from an underlying library (http stack, programming failure),
an exception may be thrown instead.
-}
executeCommand1 :: forall a. (FromSQL a, HasCallStack) =>
  LocalData a -> SparkState (Try a)
executeCommand1 ld = do
    tcell <- executeCommand1' (untypedLocalData ld)
    return $ tcell >>= (tryEither . cellToValue)

-- The main function to launch computations.
executeCommand1' :: (HasCallStack) => UntypedLocalData -> SparkState (Try Cell)
executeCommand1' ld = do
  -- Retrieve the computation graph
  let cgt = buildComputationGraph ld
  _ret cgt $ \cg -> do
    -- Source inspection:
    -- Gather the sources
    let sources = inputSourcesRead cg
    logDebugN $ "executeCommand1: found sources " <> show' sources
    -- Get the source stamps. Any error at this point is considered fatal.
    stampsRet <- checkDataStamps sources
    logDebugN $ "executeCommand1: retrieved stamps " <> show' stampsRet
    let stampst = sequence $ _f <$> stampsRet
    _ret stampst $ \stamps -> do
      -- Update the computations with the stamps, and build the computation.
      compt <- returnPure $ prepareComputation stamps cg
      _ret compt $ \comp -> do
        -- Run the computation.
        let obss = getTargetNodes comp
        session <- get
        -- Main loop to wait on the results.
        let fun3 ld2 = do
              result <- _waitSingleComputation session comp (nodeName ld2)
              return (ld2, result)
        let nodeResults = sequence (fun3 <$> obss) :: SparkState [(LocalData Cell, FinalResult)]
        _ <- _sendComputation session comp
        nrs <- nodeResults
        returnPure $ storeResults comp nrs

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
  -- logDebugN $ "url:" <> url
  _ <- _post url (toJSON 'a')
  return ()


_sendComputation :: (MonadLoggerIO m) => SparkSession -> Computation -> m ()
_sendComputation session comp = do
  let base' = _compEndPoint session (cId comp)
  let url = base' <> "/create"
  logInfoN $ "Sending computations at url: " <> url
  _ <- _post url (toJSON (cNodes comp))
  return ()

_computationStatus :: (MonadLoggerIO m) =>
  SparkSession -> ComputationID -> NodeName -> m PossibleNodeStatus
_computationStatus session compId nname = do
  let base' = _compEndPointStatus session compId
  let rest = unNodeName nname
  let url = base' <> "/" <> rest
  logDebugN $ "Sending computations status request at url: " <> url
  _ <- _get url
  -- raw <- _get url
  --logDebugN $ sformat ("Got raw status: "%sh) raw
  status <- liftIO (W.asJSON =<< W.get (T.unpack url) :: IO (W.Response PossibleNodeStatus))
  --logDebugN $ sformat ("Got status: "%sh) status
  let s = status ^. responseBody
  case s of
    NodeFinishedSuccess _ -> logInfoN $ rest <> " finished: success"
    NodeFinishedFailure _ -> logInfoN $ rest <> " finished: failure"
    _ -> return ()
  return s

_computationMultiStatus :: ComputationID -> [(NodeId, NodeName)] -> SparkState [FinalResult]
_computationMultiStatus _ [] = return []
_computationMultiStatus cid l = do
  session <- get
  -- Find the nodes that still need processing
  let f (nid, _) = case HM.lookup nid (ssNodeCache session) of
            Just nci -> nciStatus nci == NodeCacheSuccess
            _ -> False
  let needsProcessing = filter f l
  -- Poll a bunch of nodes to try to get a status update.
  let statusl = (_try (_computationStatus session cid)) <$> needsProcessing :: [SparkState (NodeId, PossibleNodeStatus)]
  status <- sequence statusl
  -- Update the state with the new data
  updated <- returnPure $ updateCache cid status
  return undefined

_try :: (Monad m) => (y -> m z) -> (x, y) -> m (x, z)
_try f (x, y) = f y <&> \z -> (x, z)

_waitSingleComputation :: (MonadLoggerIO m) =>
  SparkSession -> Computation -> NodeName -> m FinalResult
_waitSingleComputation session comp nname =
  let
    extract :: PossibleNodeStatus -> Maybe FinalResult
    extract (NodeFinishedSuccess s) = Just $ Right s
    extract (NodeFinishedFailure f) = Just $ Left f
    extract _ = Nothing
    -- getStatus :: m PossibleNodeStatus
    getStatus = _computationStatus session (cId comp) nname
    i = confPollingIntervalMillis $ ssConf session
  in
    _pollMonad getStatus i extract
