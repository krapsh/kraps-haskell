{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.CachingSpec where

import Test.Hspec
import qualified Data.Text

import Spark.Core.Context
import Spark.Core.Functions
import Spark.Core.Column
import Spark.Core.ColumnFunctions
import Spark.Core.StructuresInternal(ComputationID(..))


-- Collecting a dataset made from a list should yield the same list (modulo
-- some reordering)
collectIdempotent :: [Int] -> IO ()
collectIdempotent l = do
  -- stats <- computationStatsDef (ComputationID "0")
  -- print "STATS"
  -- print (show stats)
  let ds = dataset l
  let ds' = autocache ds
  let c1 = asCol ds'
  let s1 = sumCol c1
  let s2 = count ds'
  let x = s1 + s2
  l2 <- exec1Def x
  l2 `shouldBe` (sum l + length l)

run :: String -> IO () -> SpecWith (Arg (IO ()))
run s f = it s $ do
  createSparkSessionDef $ defaultConf { confRequestedSessionName = Data.Text.pack s }
  f
  -- This is horribly not robust to any sort of failure, but it will do for now
  -- TODO(kps) make more robust
  closeSparkSessionDef
  return ()

spec :: Spec
spec = do
  describe "Integration test - caching on ints" $ do
    run "cache_sum1" $
      collectIdempotent ([1,2,3] :: [Int])
