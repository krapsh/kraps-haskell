{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Spark.IO.JsonSpec where

import Test.Hspec

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
    -- run "simple read" $ do
    --   let dt = unSQLType (buildType :: SQLType TestStruct3)
    --   let df = json' dt "/tmp/x.json"
    --   let c = collect' (asCol' df)
    --   c1 <- exec1Def' c
    --   c1 `shouldBe` rowArray [rowArray [IntElement 3]]
    run "simple inference" $ do
      df <- execStateDef $ jsonInfer "/tmp/x.json"
      let c = collect' (asCol' df)
      c1 <- exec1Def' c
      c1 `shouldBe` rowArray [rowArray [IntElement 3]]
