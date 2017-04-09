{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.SimpleAddSpec where

import Test.Hspec
import qualified Data.Text

import Spark.Core.Context
import Spark.Core.Types
import Spark.Core.Row
import Spark.Core.Functions


smallSum :: (Eq a, Show a, SQLTypeable a, ToSQL a, FromSQL a, Num a) => a -> a -> IO ()
smallSum x y = do
  let x' = constant x
  let y' = constant y
  let z1' = x' + y'
  z1 <- exec1Def z1'
  z1 `shouldBe` (x + y)
  let z2' = y' + x'
  z2 <- exec1Def z2'
  z2 `shouldBe` (x + y)

negation :: (Eq a, Show a, SQLTypeable a, ToSQL a, FromSQL a, Num a) => a -> a -> IO ()
negation x y = do
  let x' = constant x
  let y' = constant y
  let z1' = x' - y'
  z1 <- exec1Def z1'
  z1 `shouldBe` (x - y)
  let z2' = y' - x'
  z2 <- exec1Def z2'
  z2 `shouldBe` (y - x)

checkNegate :: (Eq a, Show a, SQLTypeable a, ToSQL a, FromSQL a, Num a) => a -> IO ()
checkNegate x = do
  let x' = constant x
  let z1' = negate x'
  z1 <- exec1Def z1'
  z1 `shouldBe` negate x

run :: String -> IO () -> SpecWith (Arg (IO ()))
run s f = it s $ do
  createSparkSessionDef $ defaultConf {
    confRequestedSessionName = Data.Text.pack s,
    confPollingIntervalMillis = 100,
    confUseNodePrunning = False } -- Disabling caching for now, it causes issues.
  f
  -- This is horribly not robust to any sort of failure, but it will do for now
  -- TODO(kps) make more robust
  closeSparkSessionDef
  return ()

spec :: Spec
spec = do
  describe "Integration test - sum on ints" $ do
    run "empty_ints1" $
      smallSum (1 :: Int) (2 :: Int)
    run "zero_ints1" $
      smallSum (0 :: Int) (2 :: Int)
    run "negation_ints1" $
      negation (1 :: Int) (2 :: Int)
    run "negate_ints1" $
      checkNegate (1 :: Int)
