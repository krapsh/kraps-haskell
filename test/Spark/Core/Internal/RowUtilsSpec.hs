{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Spark.Core.Internal.RowUtilsSpec where

import Data.Aeson
import Data.Maybe(fromJust)
import Test.Hspec
import Data.ByteString.Lazy(ByteString)
import qualified Data.Vector as V

import Spark.Core.Types
import Spark.Core.Row
import Spark.Core.Internal.TypesFunctions

fun :: ByteString -> DataType -> Cell -> IO ()
fun js dt cell2 =
  let
    mval = decode js :: Maybe Value
    val = fromJust mval
    cellt = jsonToCell dt val
  in cellt `shouldBe` (Right cell2)


spec :: Spec
spec = do
  describe "JSON -> Row" $ do
    it "ints" $ do
      fun "2" intType (IntElement 2)
    it "[ints]" $ do
      fun "[2]" (arrayType' intType) (RowArray (V.singleton (IntElement 2)))
