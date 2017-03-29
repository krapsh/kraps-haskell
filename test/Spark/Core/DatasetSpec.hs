{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.DatasetSpec where

import qualified Data.Text as T
import Test.Hspec
import qualified Data.Vector as V

import Spark.Core.Dataset
import Spark.Core.Functions
import Spark.Core.Column
import Spark.Core.StructuresInternal
import Spark.Core.Internal.ContextInternal
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.ComputeDag

nName :: String -> NodeName
nName = NodeName . T.pack

spec :: Spec
spec = do
  describe "create a dataframe" $ do
    it "should not explode" $
      let x = dataset ([1 ,2, 3, 4]::[Int]) in
        nodeName x `shouldBe` nName "distributedliteral_c87697"

    it "renaming should work" $
      let x = dataset ([1 ,2, 3]::[Int]) @@ "ds1" in
        nodeName x `shouldBe` nName "ds1"

  describe "check localset" $ do
    it "should not explode" $
      let x = dataset ([1 ,2, 3]::[Int]) in
        nodeName x `shouldBe` nName "distributedliteral_1ba31e"

    it "renaming should work" $
      let x = dataset ([1 ,2, 3]::[Int]) @@ "ds1" in
        nodeName x `shouldBe` nName "ds1"

  describe "column syntax" $
    it "should not explode" $ do
      let ds = dataset ([1 ,2, 3]::[Int])
      let c1 = ds/-"c1"
      c1 `shouldBe` c1

  describe "Logical dependencies" $ do
    it "should work" $ do
      let ds = dataset ([1 ,2, 3, 4]::[Int])
      let ds1 = dataset ([1]::[Int]) `depends` [untyped ds]
      let g = traceHint (T.pack "g=") $ computeGraphToGraph $ forceRight $ buildComputationGraph ds1
      V.length (gVertices g) `shouldBe` 2


--   describe "simple test" $ do
--     it "the type should match" $ do
--       let
--        n1 = constant "xxx"
--        n = NodeType $ T.pack "org.spark.Constant" in
--         (nodeOp n1) `shouldBe` n

--     it "no name" $ do
--       let n1 = constant "xxx"
--           t = NodeName $ T.pack "org.spark.Constant" in
--         (nodeName n1) `shouldBe` t

--     it "some name" $ do
--       let n1 = constant "xxx" @@ "name"
--           t = NodeName $ T.pack "name" in
--         (nodeName n1) `shouldBe` t
