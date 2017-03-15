
module Spark.Core.ColumnSpec where

import Test.Hspec

import Spark.Core.Column
import Spark.Core.Dataset
import Spark.Core.Functions
import Spark.Core.ColumnFunctions

data Z
data Y

spec :: Spec
spec = do
  describe "ColumnSpec: ensure rules compile correctly" $ do
    let c1 = undefined :: Column Z Int
    let c2 = undefined :: Column Z Int
    let c1' = undefined :: DynColumn
    let c2' = undefined :: DynColumn
    let i1 = 3 :: Int
    let o1 = constant 4 :: LocalData Int
    let o2 = 5 :: LocalData Int
    let o1' = undefined :: LocalFrame
    let o2' = undefined :: LocalFrame
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
