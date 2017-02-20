{-
let s = "s"

createSparkSessionDef $ defaultConf { confRequestedSessionName = Data.Text.pack s }

execStateDef (checkDataStamps [HdfsPath (Data.Text.pack "/tmp/")])

-}

module Spark.IO.StampSpec where

import Test.Hspec

-- import Spark.Core.Context
-- import Spark.Core.Types
-- import Spark.Core.Row
-- import Spark.Core.Functions
-- import Spark.Core.Column
-- import Spark.IO.Inputs
-- import Spark.Core.IntegrationUtilities
import Spark.Core.SimpleAddSpec(run)

spec :: Spec
spec = do
  describe "Read a json file" $ do
    run "simple read" $ do
      let x = 1 :: Int
      x `shouldBe` x
