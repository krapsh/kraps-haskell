{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MultiParamTypeClasses #-}

-- :script test/Spark/Core/Internal/PathsSpec.hs
module Spark.Core.Internal.DAGFunctionsSpec where

import Test.Hspec
import qualified Data.Map.Strict as M
import qualified Data.Vector as V
import qualified Data.ByteString.Char8 as C8
import Control.Arrow((&&&))
import Data.Foldable(toList)

import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.DAGFunctions
import Spark.Core.Internal.Utilities

data MyV = MyV {
  mvId :: VertexId,
  mvParents :: [MyV]
} deriving (Eq)

id2Str :: VertexId -> String
id2Str = C8.unpack . unVertexId

instance Show MyV where
  show v = "MyV(" ++ (id2Str . mvId $ v) ++ ")"


instance GraphVertexOperations MyV where
  vertexToId = mvId
  expandVertexAsVertices = mvParents

instance GraphOperations MyV () where
  expandVertex = ((const () &&& id) <$>) . mvParents

myv :: String -> [MyV] -> MyV
myv s = MyV (VertexId (C8.pack s))

expandNodes :: MyV -> DagTry [String]
expandNodes vx =
  let tg = buildGraph vx :: DagTry (Graph MyV ())
  in (id2Str . mvId . vertexData <$>) . toList . gVertices <$> tg

-- edges: from -> to
expandEdges :: MyV -> DagTry [(String, String)]
expandEdges vx =
  let tg = buildGraph vx :: DagTry (Graph MyV ())
  in tg <&> \g ->
    concat $ M.assocs (gEdges g) <&> \(vid, v) ->
      (C8.unpack . unVertexId . vertexId . veEndVertex &&&
       C8.unpack . unVertexId . const vid) <$> V.toList v

spec :: Spec
spec = do
  describe "Tests on paths" $ do
    it "no parent" $ do
      let v0 = myv "v0" []
      expandNodes v0 `shouldBe` Right ["v0"]
    it "common parent" $ do
      let v0 = myv "v0" []
      let v0' = myv "v0" []
      let v1 = myv "v1" [v0, v0']
      expandEdges v1 `shouldBe` Right [("v0", "v1"), ("v0", "v1")]
    it "diamond" $ do
      let va = myv "va" []
      let va' = myv "va" []
      let v0 = myv "v0" [va]
      let v0' = myv "v0" [va']
      let v1 = myv "v1" [v0, v0']
      expandEdges v1 `shouldBe` Right [("va", "v0"), ("v0", "v1"), ("v0", "v1")]
    it "simple sources" $ do
      let v0 = myv "v0" []
      let v1 = myv "v1" [v0]
      let tg = buildGraph v1 :: DagTry (Graph MyV ())
      let g = forceRight tg
      mvId . vertexData <$> graphSources g `shouldBe` [mvId v1]
    it "simple sinks" $ do
      let v0 = myv "v0" []
      let v1 = myv "v1" [v0]
      let tg = buildGraph v1 :: DagTry (Graph MyV ())
      let g = forceRight tg
      mvId . vertexData <$> graphSinks g `shouldBe` [mvId v0]
    it "longer sources" $ do
      let v0 = myv "v0" []
      let v1 = myv "v1" [v0]
      let v2 = myv "v2" [v1]
      let tg = buildGraph v2 :: DagTry (Graph MyV ())
      let g = forceRight tg
      mvId . vertexData <$> graphSources g `shouldBe` [mvId v2]
    it "longer sinks" $ do
      let v0 = myv "v0" []
      let v1 = myv "v1" [v0]
      let v2 = myv "v2" [v1]
      let tg = buildGraph v2 :: DagTry (Graph MyV ())
      let g = forceRight tg
      mvId . vertexData <$> graphSinks g `shouldBe` [mvId v0]
  describe "building DAGs" $ do
    it "2 nodes" $ do
      let v0 = myv "v0" []
      let v1 = myv "v1" [v0]
      let v2 = myv "v2" [v1]
      let l = forceRight $ buildVertexList v2
      id2Str . mvId <$> l `shouldBe` ["v0", "v1", "v2"]
    it "triangle" $ do
      let v0 = myv "v0" []
      let v1 = myv "v1" [v0]
      let v2 = myv "v2" [v0, v1]
      let l = forceRight $ buildVertexList v2
      -- The return order should be in lexicographic order
      -- (which is unique in this case).
      id2Str . mvId <$> l `shouldBe` ["v0", "v1", "v2"]
