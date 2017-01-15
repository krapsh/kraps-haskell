{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Spark.Core.Internal.GroupsSpec where

import Data.Text(Text)
import Test.Hspec
import GHC.Generics
import Data.Either(isRight)

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

-- The tests are really light for now, and just check that the code passes the
-- dynamic type checker.
spec :: Spec
spec = do
  describe "typed grouping tests" $ do
    let ds = dataset [MyPair "1" 1, MyPair "2" 2]
    let keys = ds // myKey'
    let values = ds // myVal'
    let g = groupByKey keys values
    let sqlt1 = buildType :: SQLType MyPair
    it "group" $ do
      let tds2 = castType sqlt1 (groupAsDS g)
      tds2 `shouldSatisfy` isRight
    it "map group" $ do
      let g2 = g `mapGroup` \c -> c + c
      let tds2 = castType sqlt1 (groupAsDS g2)
      tds2 `shouldSatisfy` isRight
    it "simple reduce" $ do
      let ds2 = g `aggKey` sumCol
      let tds3 = castType sqlt1 ds2
      tds3 `shouldSatisfy` isRight
    it "complex reduce" $ do
      let ds2 = g `aggKey` \c -> sumCol (c + c)
      let tds3 = castType sqlt1 ds2
      tds3 `shouldSatisfy` isRight
