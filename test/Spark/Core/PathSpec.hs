{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}

module Spark.Core.PathSpec where

import Data.Maybe(fromJust)
import Test.Hspec

import Spark.Core.Functions
import Spark.Core.Dataset

fun1 :: LocalData Int
fun1 =
  let m1 = constant (1::Int)
      m2 = constant (2::Int) in
    constant (3::Int)
      `logicalParents` [untyped m1, untyped m2]
      `parents` []


fun2 :: LocalData Int -> LocalData Int
fun2 ld1 = let
  m1 = constant (1 :: Int) `parents` [untyped ld1] @@ "m1"
    in
      constant (3 :: Int)
        `logicalParents` [untyped m1]
        `parents` [untyped ld1]
        @@ "c2"

-- fun3 :: LocalData Int -> LocalData Int
-- fun3 ld = ld + 3

spec :: Spec
spec = do
  describe "Tests with nodes" $ do
    it "should get a node" $ do
      let n1 = fun1
      let l = fromJust $ nodeLogicalParents n1
      (length l) `shouldBe` 2
    -- it "should work with ints" $ do
    --   let n2 = (fun3 4) @@ "" in
    --     (length $ nodeDependencies n2) `shouldBe` 2
