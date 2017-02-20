{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.PruningSpec where

import Test.Hspec
import qualified Data.Text as T
import Data.List(sort)

import Spark.Core.Context
import Spark.Core.Types
import Spark.Core.Row
import Spark.Core.Functions
import Spark.Core.Column
import Spark.Core.IntegrationUtilities
import Spark.Core.CollectSpec(run)

run2 :: T.Text -> IO () -> SpecWith (Arg (IO ()))
run2 s f = it (T.unpack s) $ do
  createSparkSessionDef $ defaultConf {
      confRequestedSessionName = s,
      confUseNodePrunning = True }
  f
  -- This is horribly not robust to any sort of failure, but it will do for now
  -- TODO(kps) make more robust
  closeSparkSessionDef
  return ()


spec :: Spec
spec = do
  describe "Integration test - pruning" $ do
    run2 "running_twice" $ do
      let ds = dataset [1::Int,2]
      l2 <- exec1Def $ collect (asCol ds)
      l2' <- exec1Def $ collect (asCol ds)
      l2 `shouldBe` l2'
