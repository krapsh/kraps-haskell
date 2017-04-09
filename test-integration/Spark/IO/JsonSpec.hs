{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Spark.IO.JsonSpec where

import Test.Hspec
import Data.Aeson(encode)
import qualified Data.ByteString.Lazy
-- import System.IO

import Spark.Core.Context
import Spark.Core.Types
import Spark.Core.Row
import Spark.Core.Functions
import Spark.Core.Column
import Spark.IO.Inputs
import Spark.Core.IntegrationUtilities
import Spark.Core.SimpleAddSpec(run)

spec :: Spec
spec = do
  describe "Read a json file" $ do
    run "simple read" $ do
      let xs = [TestStruct7 "x"]
      let js = encode xs
      _ <- Data.ByteString.Lazy.writeFile "/tmp/x.json" js
      let dt = unSQLType (buildType :: SQLType TestStruct7)
      let df = json' dt "/tmp/x.json"
      let c = collect' (asCol' df)
      c1 <- exec1Def' c
      c1 `shouldBe` rowArray [rowArray [StringElement "x"]]
      c2 <- exec1Def' c
      c2 `shouldBe` rowArray [rowArray [StringElement "x"]]
    run "simple inference" $ do
      let xs = [TestStruct7 "x"]
      let js = encode xs
      _ <- Data.ByteString.Lazy.writeFile "/tmp/x.json" js
      df <- execStateDef $ jsonInfer "/tmp/x.json"
      let c = collect' (asCol' df)
      c1 <- exec1Def' c
      c1 `shouldBe` rowArray [rowArray [StringElement "x"]]
