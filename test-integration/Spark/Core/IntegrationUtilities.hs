{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Spark.Core.IntegrationUtilities where

import GHC.Generics (Generic)
import Data.Text(Text)
import Data.Aeson(ToJSON)

import Spark.Core.Context
import Spark.Core.Types
import Spark.Core.Row
import Spark.Core.Column

data TestStruct1 = TestStruct1 {
  ts1f1 :: Int,
  ts1f2 :: Maybe Int } deriving (Show, Eq, Generic)
instance ToSQL TestStruct1
instance FromSQL TestStruct1
instance SQLTypeable TestStruct1

data TestStruct2 = TestStruct2 {
  ts2f1 :: [Int]
} deriving (Show, Generic)
instance SQLTypeable TestStruct2

data TestStruct3 = TestStruct3 {
  ts3f1 :: Int
} deriving (Show, Eq, Generic)
instance ToSQL TestStruct3
instance SQLTypeable TestStruct3

data TestStruct4 = TestStruct4 {
  ts4f1 :: TestStruct3
} deriving (Show, Eq, Generic)

data TestStruct5 = TestStruct5 {
  ts5f1 :: Int,
  ts5f2 :: Int
} deriving (Show, Eq, Generic, Ord)
-- instance ToJSON TestStruct5
instance SQLTypeable TestStruct5
instance FromSQL TestStruct5
instance ToSQL TestStruct5

data TestStruct6 = TestStruct6 {
  ts6f1 :: Int,
  ts6f2 :: Int,
  ts6f3 :: TestStruct3
} deriving (Show, Eq, Generic)

data TestStruct7 = TestStruct7 {
  ts7f1 :: Text
} deriving (Show, Eq, Generic)
instance ToSQL TestStruct7
instance SQLTypeable TestStruct7
instance ToJSON TestStruct7

newtype TestT1 = TestT1 {
  unTestT1 :: Int
} deriving (Eq, Show, Generic, Num)

data MyPair = MyPair {
  myKey :: Text,
  myVal :: Int } deriving (Generic, Show)

myKey' :: StaticColProjection MyPair Text
myKey' = unsafeStaticProjection buildType "myKey"
myVal' :: StaticColProjection MyPair Int
myVal' = unsafeStaticProjection buildType "myVal"
instance SQLTypeable MyPair
instance ToSQL MyPair
