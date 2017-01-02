{-# LANGUAGE MultiParamTypeClasses #-}

module Spark.Core.JoinsSpec where

import Test.Hspec

import Spark.Core.Context
import Spark.Core.Dataset
import Spark.Core.Column
import Spark.Core.Row
import Spark.Core.Functions
import Spark.Core.SimpleAddSpec(run)

spec :: Spec
spec = do
  describe "Join test - join on ints" $ do
    run "empty_ints1" $ do
      let ds1 = dataset [(1,2)] :: Dataset (Int, Int)
      let ds2 = dataset [(1,3)] :: Dataset (Int, Int)
      let df1 = asDF ds1
      let df2 = asDF ds2
      let df = joinInner' (df1//"_1") (df1//"_2") (df2//"_1") (df2//"_2" @@ "_3")
      res <- exec1Def' (collect' (asCol' df))
      res `shouldBe` rowArray [rowArray [IntElement 1, IntElement 2, IntElement 3]]
