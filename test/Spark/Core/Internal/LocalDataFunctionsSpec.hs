
module Spark.Core.Internal.LocalDataFunctionsSpec where

import Test.Hspec

import Spark.Core.Dataset
import Spark.Core.Functions()

spec :: Spec
spec = do
  describe "Arithmetic operations on local data (integers)" $ do
    it "ints" $ do
      let x1 = 1 :: LocalData Int
      let x2 = 2 :: LocalData Int
      let y1 = x1 + x2
      let y2 = x1 `div` x2
      (y2 `shouldBe` y2)
      (y1 `shouldBe` y1)
