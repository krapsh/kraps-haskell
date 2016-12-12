{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

-- | Functions to create and manipulate one default context.
--
-- This is most appropriate when working in an interactive session,
-- during which it is usually clear that there is a single
-- Spark context in use.
--
-- This module uses unsafe Haskell code that should not be used
-- outside prototyping in an interactive REPL. In any good case,
-- you should use the SparkState monad.
module Spark.Core.Internal.ContextInteractive(
  SparkInteractiveException,
  createSparkSessionDef,
  exec1Def,
  closeSparkSessionDef
) where

import qualified Data.Vector as V
import Control.Exception
import Control.Monad.Catch(throwM)
import Data.IORef
import Data.Typeable
import Control.Monad.State(runStateT)
import Data.Text
import System.IO.Unsafe(unsafePerformIO)
import Control.Monad.Logger(runStdoutLoggingT)


import Spark.Core.Internal.ContextStructures
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.ContextIOInternal
import Spark.Core.Internal.RowGenericsFrom(FromSQL)
import Spark.Core.Internal.Utilities
import Spark.Core.StructuresInternal
import Spark.Core.Try

-- The global session reference. Should not be accessed outside
-- this file.
_globalSessionRef :: IORef (Maybe SparkSession)
{-# NOINLINE _globalSessionRef #-}
_globalSessionRef = unsafePerformIO (newIORef Nothing)

-- | The exception thrown when a request cannot be completed
-- in an interactive session.
data SparkInteractiveException = SparkInteractiveException {
  _sieInner :: NodeError
} deriving Typeable

instance Show SparkInteractiveException where
  show (SparkInteractiveException inner) =
    show inner

instance Exception SparkInteractiveException

{- | Creates a spark session that will be used as the default session.

If a session already exists, an exception will be thrown.
 -}
createSparkSessionDef :: (HasCallStack) => SparkSessionConf -> IO ()
createSparkSessionDef conf = do
  current <- _currentSession
  case current of
    Nothing ->
      return ()
    Just _ ->
      -- TODO let users change the state
      _throw "A default context already exist. If you wish to modify the exsting context, you must use modifySparkConfDef"
  new <- createSparkSession' conf
  _setSession new
  return ()

{- | Executes a command using the default spark session.

This is the most unsafe way of running a command:
it executes a command using the default spark session, and
throws an exception if any error happens.
 -}
exec1Def :: (FromSQL a, HasCallStack) => LocalData a -> IO a
exec1Def ld = do
  mCtx <- _currentSession
  case mCtx of
    Nothing ->
      _throw "No default context found. You must first create a default spark context with createSparkSessionDef"
    Just ctx -> do
      (res, newSt) <- (runStateT . runStdoutLoggingT . executeCommand1) ld ctx
      _setSession newSt
      case res of
        Right x ->
          return x
        Left err ->
          throwM (SparkInteractiveException err)

{-| Closes the default session. The default session is empty after this call
completes.

NOTE: This does not currently clear up the resources! It is a stub implementation
used in testing.
-}
closeSparkSessionDef :: (HasCallStack) => IO ()
closeSparkSessionDef = do
  _ <- _removeSession
  return ()

_currentSession :: (HasCallStack) => IO (Maybe SparkSession)
_currentSession = readIORef _globalSessionRef

_setSession :: (HasCallStack) => SparkSession -> IO ()
_setSession st = writeIORef _globalSessionRef (Just st)

_removeSession :: (HasCallStack) => IO (Maybe SparkSession)
_removeSession = do
  current <- _currentSession
  _ <- writeIORef _globalSessionRef Nothing
  return current

_throw :: (HasCallStack) => Text -> IO a
_throw txt = throwM $
  SparkInteractiveException Error {
    ePath = NodePath V.empty,
    eMessage = txt
  }
