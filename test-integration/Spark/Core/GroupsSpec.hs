{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.GroupsSpec where

import Test.Hspec
import Data.Text(Text)

import Spark.Core.Context
import Spark.Core.Functions
import Spark.Core.ColumnFunctions
import Spark.Core.Column
import Spark.Core.IntegrationUtilities
import Spark.Core.CollectSpec(run)
import Spark.Core.Internal.Groups

sumGroup :: [MyPair] -> [(Text, Int)] -> IO ()
sumGroup l lexp = do
  let ds = dataset l
  let keys = ds // myKey'
  let values = ds // myVal'
  let g = groupByKey keys values
  let ds2 = g `aggKey` sumCol
  l2 <- exec1Def $ collect (asCol ds2)
  l2 `shouldBe` lexp

spec :: Spec
spec = do
  describe "Integration test - groups on (text, int)" $ do
    run "empty" $
      sumGroup [] []
    run "one" $
      sumGroup [MyPair "x" 1] [("x", 1)]
    run "two" $
      sumGroup [MyPair "x" 1, MyPair "x" 2, MyPair "y" 1] [("x", 3), ("y", 1)]
