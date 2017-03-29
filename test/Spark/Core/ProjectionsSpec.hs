{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.ProjectionsSpec where

import Test.Hspec
import GHC.Generics
import Data.List(isPrefixOf)
import Data.Either(isRight, isLeft)
import qualified Data.Vector as V
import qualified Data.Text as T

import Spark.Core.Functions
import Spark.Core.Dataset
import Spark.Core.Column
import Spark.Core.Row
import Spark.Core.Types
import Spark.Core.Try
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.TypesFunctions


data Tree = Tree {
  treeId :: Int,
  treeWidth :: Int,
  treeHeight :: Int } deriving (Generic, Show)

treeId' :: StaticColProjection Tree Int
treeId' = unsafeStaticProjection buildType "treeId"
treeWidth' :: StaticColProjection Tree Int
treeWidth' = unsafeStaticProjection buildType "treeWidth"
instance SQLTypeable Tree
instance ToSQL Tree

newtype MyId = MyId Int deriving (Generic, Show, Num)
instance SQLTypeable MyId
instance ToSQL MyId

newtype Height = Height Int deriving (Generic, Num, Show)
instance SQLTypeable Height
instance ToSQL Height

data STree = STree {
  sTreeId :: MyId,
  sTreeWidth :: Height,
  sTreeHeight :: Int } deriving (Generic, Show)

instance SQLTypeable STree
instance ToSQL STree
sTreeId' :: StaticColProjection STree MyId
sTreeId' = unsafeStaticProjection buildType "sTreeId"
sTreeWidth' :: StaticColProjection STree Height
sTreeWidth' = unsafeStaticProjection buildType "sTreeWidth"
instance TupleEquivalence STree (MyId, Height, Int) where
  tupleFieldNames = NameTuple ["sTreeId", "sTreeWidth", "sTreeHeight"]

rawData :: [(Int, Int, Int)]
rawData = [(1, 3, 2)]

spec :: Spec
spec = do
  let ds = dataset [Tree 1 3 2]
  -- The untyped elements
  let dt = structType [structField (T.pack "treeId") intType, structField (T.pack "treeWidth") intType, structField (T.pack "treeHeight") intType]
  let fun (id', height, width) = RowArray $ V.fromList [IntElement id', IntElement height, IntElement width]
  let df1 = traceHint (T.pack "df1=") $ dataframe dt (fun <$> rawData)
  let ds1 = traceHint (T.pack "ds1=") $ forceRight (asDS df1) :: Dataset Tree
  describe "Simple projection demos" $ do
    it "should get a node" $ do
      ds `shouldBe` ds1
    it "Failing dynamic projection on dataframe" $ do
      df1/-"xx" `shouldSatisfy` isLeft
    it "Failing dynamic projection on dataset" $ do
      ds1/-"xx" `shouldSatisfy` isLeft
    it "Basic arithmetic on DS cols" $ do
      let c1 = ds1//treeWidth'
      let c2 = (c1 + c1)
      (show c2) `shouldSatisfy` ("treeWidth + treeWidth{int}" `isPrefixOf`)
    it "Basic arithmetic on DF cols" $ do
      let c1 = df1 // treeWidth'
      let c2 = c1 + c1
      (show c2) `shouldSatisfy` ("Right treeWidth + treeWidth{int}" `isPrefixOf`)
    it "Construction of ds2" $ do
      let str = struct' [ (df1/-"treeId") @@ "sTreeId",
                          (df1/-"treeWidth") @@ "sTreeWidth",
                          (df1/-"treeHeight") @@ "sTreeHeight"]
      let df2 = pack' str
      let ds2 = traceHint (T.pack "ds2=") $ asDS df2 :: Try (Dataset STree)
      ds2 `shouldSatisfy` isRight
    it "Static construction of ds2" $ do
      let ds2 = do
              idCol <- castCol' (buildType::SQLType MyId) (df1/-"treeId")
              widthCol <- castCol' (buildType::SQLType Height) (df1/-"treeWidth")
              heightCol <- castCol' (buildType::SQLType Int) (df1/-"treeWidth")
              let s = pack (idCol, widthCol, heightCol) :: Dataset STree
              return $ traceHint (T.pack "ds2=") s
      ds2 `shouldSatisfy` isRight
    it "Basic arithmetic on DS cols 1" $ do
      let ds2' = do
              idCol <- castCol' (buildType::SQLType MyId) (df1/-"treeId")
              widthCol <- castCol' (buildType::SQLType Height) (df1/-"treeWidth")
              heightCol <- castCol' (buildType::SQLType Int) (df1/-"treeWidth")
              let s = pack (idCol, widthCol, heightCol) :: Dataset STree
              return $ traceHint (T.pack "ds2=") s
      let ds2 = forceRight ds2'
      let c1 = ds2//sTreeWidth'
      let c2 = c1 + c1
      (show c2) `shouldSatisfy` ("sTreeWidth + sTreeWidth{int}" `isPrefixOf`)
