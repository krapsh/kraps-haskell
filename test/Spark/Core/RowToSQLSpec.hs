{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

module Spark.Core.RowToSQLSpec where

import qualified Data.Vector as V
import GHC.Generics (Generic)
import Test.Hspec

import Spark.Core.Types
import Spark.Core.Row

data TestStruct1 = TestStruct1 {
  ts1f1 :: Int,
  ts1f2 :: Maybe Int } deriving (Show, Eq, Generic, ToSQL, FromSQL)

data TestStruct2 = TestStruct2 { ts2f1 :: [Int] } deriving (Show, Generic, SQLTypeable)

data TestStruct3 = TestStruct3 { ts3f1 :: Int } deriving (Show, Eq, Generic, ToSQL, FromSQL)
data TestStruct4 = TestStruct4 { ts4f1 :: TestStruct3 } deriving (Show, Eq, Generic, ToSQL, FromSQL)

data TestStruct5 = TestStruct5 {
  ts5f1 :: Int,
  ts5f2 :: Int,
  ts5f3 :: TestStruct3
} deriving (Show, Eq, Generic, ToSQL, FromSQL)

newtype TestT1 = TestT1 { unTestT1 :: Int } deriving (Eq, Show, Generic, ToSQL, FromSQL)


v2c :: (Show a, ToSQL a, FromSQL a, Eq a) => a -> Cell -> IO ()
v2c !x !y = do
  _ <- shouldBe (valueToCell x) y
  _ <- shouldBe (cellToValue y) (Right x)
  return ()

spec :: Spec
spec = do
  describe "Simple type tests" $ do
    it "int" $
      v2c (3 :: Int) (IntElement 3)
    it "int?" $
      v2c (Just 3 :: Maybe Int) (IntElement 3)
    it "int? 2" $
      v2c (Nothing :: Maybe Int) Empty
    it "TestStruct3" $
      v2c (TestStruct3 2) (RowArray $ V.fromList [IntElement 2])
    it "TestStruct4" $
      v2c (TestStruct4 (TestStruct3 3)) $
        (RowArray $ V.fromList [
            RowArray $ V.fromList [IntElement 3]
          ])
    it "TestStruct1 - empty" $
      v2c (TestStruct1 2 Nothing) (RowArray $ V.fromList [IntElement 2, Empty])
    it "TestStruct1 - full" $
      v2c (TestStruct1 2 (Just 4)) (RowArray $ V.fromList [IntElement 2, IntElement 4])
    it "TestStruct5" $
      v2c (TestStruct5 1 2 (TestStruct3 3)) $
        (RowArray $ V.fromList [
            IntElement 1,
            IntElement 2,
            RowArray $ V.fromList [IntElement 3]
          ])
  -- describe "Simple type tests" $ do
  --   it "newtype" $
  --     v2c (TestT1 3) (IntElement 3)
