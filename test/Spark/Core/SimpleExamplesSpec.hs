{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE QuasiQuotes #-}

-- Some small examples that get fully verified.
module Spark.Core.SimpleExamplesSpec where

import Data.Either(isRight)
import Data.Maybe(isJust)
import Test.Hspec
import qualified Data.Text as T
import Text.RawString.QQ

import Spark.Core.Dataset
import Spark.Core.Functions
import Spark.Core.Column
import Spark.Core.ColumnFunctions
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.Utilities(pretty)
import Spark.Core.Internal.OpFunctions(extraNodeOpData)

ds1 :: Dataset Int
ds1 = dataset [1,2,3]

ds2 :: Dataset Double
ds2 = error "ds2"

spec :: Spec
spec = do
  describe "Simple examples" $ do
    it "Precdence of renaming" $ do
      let numbers = asCol ds1
      let s = sumCol numbers
      let numCount = count ds1
      let avg = s `div` numCount @@ "myaverage"
      _cnName avg `shouldSatisfy` isJust
    it "name for simple integers" $ do
      let numbers = asCol ds1
      let s = sumCol numbers
      let numCount = count ds1
      let avg = s `div` numCount @@ "myaverage"
      -- TODO: should it show "value: int" instead?
      -- I think it should show it for distributed nodes only.
      -- SQL is not allowed on observables
      (show avg) `shouldBe` "/myaverage@org.spark.LocalDiv!int"
  describe "pack1" $ do
    it "Extracting and packing one column" $ do
      let numbers = asCol ds1
      let ds1' = pack1 numbers
      (nodeType ds1) `shouldBe` (nodeType ds1')
  describe "pack" $ do
    it "Extracting and packing one column" $ do
      let ds1' = pack' . asCol $ ds1
      (nodeType <$> (asDF ds1)) `shouldBe` (nodeType <$> ds1')
  describe "simple json example" $ do
    it "packing and unpacking one column" $ do
      let ds1' = pack' . asCol $ ds1
      let d' = pretty . extraNodeOpData . nodeOp <$> ds1'
      d' `shouldBe` Right (T.pack "{\"cellType\":{\"dt\":\"integer\",\"nullable\":false},\"content\":[1,2,3]}")
    it "packing and unpacking 2 columns, one with a bad name" $ do
      let col1 = asCol ds1
      let col2 = col1 @@ "other"
      let ds1' = pack' (col1, col2)
      ds1' `shouldSatisfy` isRight -- NOT SURE WHY IT WOULD FAIL
    it "packing and unpacking 2 columns, one with a good name" $ do
      let col1 = asCol ds1 @@ "first"
      let col2 = col1 @@ "second"
      let ds1' = pack' (col1, col2)
      ds1' `shouldSatisfy` isRight


    -- it "example2" $ do
    --   let numbers = asCol ds2
    --   let avg = (colSum numbers) / (count ds2)
    --   1 `shouldBe` 1
