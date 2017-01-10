{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Spark.Core.Internal.GroupsSpec where

import Data.Text(Text)
import Test.Hspec
import GHC.Generics

import Spark.Core.Functions
import Spark.Core.ColumnFunctions
import Spark.Core.Dataset
import Spark.Core.Column
import Spark.Core.Row
import Spark.Core.Types
import Spark.Core.Internal.Groups


data MyPair = MyPair {
  myKey :: Text,
  myVal :: Int } deriving (Generic, Show)

myKey' :: StaticColProjection MyPair Text
myKey' = unsafeStaticProjection buildType "myKey"
myVal' :: StaticColProjection MyPair Int
myVal' = unsafeStaticProjection buildType "myVal"
instance SQLTypeable MyPair
instance ToSQL MyPair

spec :: Spec
spec = do
  describe "typed grouping tests" $ do
    let ds = dataset [MyPair "1" 1, MyPair "2" 2]
    let keys = ds // myKey'
    let values = ds // myVal'
    let g = groupByKey keys values
    it "group" $ do
      let ds2 = groupAsDS g
      asDF ds2 `shouldBe` asDF ds
    it "map group" $ do
      let g2 = g `mapGroup` \c -> c + c
      let ds2 = groupAsDS g2
      asDF ds2 `shouldBe` asDF ds
    it "simple reduce" $ do
      let ds2 = g `aggKey` sumCol
      asDF ds2 `shouldBe` asDF ds
    it "complex reduce" $ do
      let ds2 = g `aggKey` \c -> sumCol (c + c)
      asDF ds2 `shouldBe` asDF ds
