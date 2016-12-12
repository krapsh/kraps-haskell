

module Spark.Core.ContextSpec where

import Test.Hspec

import Spark.Core.Functions

spec :: Spec
spec = do
  describe "Basic routines to get something out" $ do
    it "should print a node" $ do
      let x = dataset ([1 ,2, 3, 4]::[Int])
      x `shouldBe` x
        --   b = nodeToBundle (untyped x) in
        -- trace (pretty b) $
        --   1 `shouldBe` 1
