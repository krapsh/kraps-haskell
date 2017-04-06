{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.ColumnSpec where

import Test.Hspec

import Spark.Core.Column
import Spark.Core.Dataset
import Spark.Core.Functions
import Spark.Core.Types
import Spark.Core.ColumnFunctions
import Spark.Core.Internal.Utilities

data Z
data Y

myScaler :: Column ref Double -> Column ref Double
myScaler col =
  let cnt = asDouble (countCol col)
      m = sumCol col / cnt
      centered = col .- m
      stdDev = sumCol (centered * centered) / cnt
  in centered ./ stdDev


spec :: Spec
spec = do
  describe "ColumnSpec: ensure rules compile correctly" $ do
    let ds = dataset [(1,2)] :: Dataset (Int, Int)
    let c1 = ds // _1
    let c2 = ds // _2
    let c1' = untypedCol c1
    let c2' = untypedCol c2
    let i1 = 3 :: Int
    let o1 = constant 4 :: LocalData Int
    let o2 = 5 :: LocalData Int
    let o1' = asLocalObservable o1
    let o2' = asLocalObservable o2
    it "+ should not blow up" $ do
      let z1 = c1 + c2
      let z2 = c1' + c2'
      let z3 = c1 + 1
      let z4 = 1 + c1
      'a' `shouldBe` 'a'
    it ".+ should not blow up with colums" $ do
      let z1 = c1 .+ c2
      let z2 = c1' .+ c2'
      let z3 = c1 .+ c2'
      let z4 = c1' .+ c2
      let z5 = c1 .+ o1
      let z6 = c1 .+ o1'
      'a' `shouldBe` 'a'
    it "simple aggregations" $ do
      let c3 = c1 + (c2 .+ sumCol c2)
      let ds2 = pack1 c3
      nodeType ds2 `shouldBe` (buildType :: SQLType Int)
    it "mean" $ do
      let ds' = dataset [1, 2] :: Dataset Double
      let c = asCol ds'
      let cnt = asDouble (countCol c)
      let m = traceHint "m=" $ sumCol c / cnt
      let centered = c .- m
      let stdDev = sumCol (centered * centered) / cnt
      let scaled = traceHint "scaled=" $ centered ./ stdDev
      let ds2 = pack1 scaled
      nodeType ds2 `shouldBe` (buildType :: SQLType Double)
