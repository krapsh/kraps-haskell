{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.ColumnSpec where

import Test.Hspec
import Data.List.NonEmpty(NonEmpty( (:|) ))

import Spark.Core.Context
import Spark.Core.Dataset
import Spark.Core.Column
import Spark.Core.Row
import Spark.Core.Functions
import Spark.Core.ColumnFunctions
import Spark.Core.SimpleAddSpec(run)
import Spark.Core.Internal.LocalDataFunctions(iPackTupleObs)
import Spark.Core.Internal.DatasetFunctions(untypedLocalData)

myScaler :: Column ref Double -> Column ref Double
myScaler col =
  let cnt = asDouble (countCol col)
      m = sumCol col / cnt
      centered = col .- m
      stdDev = sumCol (centered * centered) / cnt
  in centered ./ stdDev


spec :: Spec
spec = do
  describe "local data operations" $ do
    run "LocalPack" $ do
      let x = untypedLocalData (1 :: LocalData Int)
      let x2 = iPackTupleObs (x :| [x])
      res <- exec1Def x2
      res `shouldBe` rowArray [IntElement 1, IntElement 1]
    run "BroadcastPair" $ do
      undefined
  describe "columns - integration" $ do
    run "mean" $ do
      let ds = dataset [-1, 1] :: Dataset Double
      let c = myScaler (asCol ds)
      res <- exec1Def (collect c)
      res `shouldBe` [-1, 1]
