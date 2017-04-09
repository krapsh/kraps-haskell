{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- :script test/Spark/Core/Internal/PathsSpec.hs
module Spark.Core.Internal.PathsSpec where

import Test.Hspec
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import qualified Data.ByteString.Char8 as C8
import qualified Data.Text as T

import Spark.Core.StructuresInternal
import Spark.Core.Functions
import Spark.Core.Dataset
import Spark.Core.Internal.Paths
import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.DAGFunctions
import Spark.Core.Internal.ComputeDag
import Spark.Core.Internal.PathsUntyped
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.DatasetStructures

data MyV = MyV {
  mvId :: VertexId,
  mvLogical :: [MyV],
  mvParents :: [MyV]
} deriving (Eq)

instance Show MyV where
  show v = "MyV(" ++ (C8.unpack . unVertexId . mvId $ v) ++ ")"


assignPaths :: UntypedNode -> [UntypedNode]
assignPaths n =
  let cgt = buildCGraph n :: DagTry (ComputeDag UntypedNode NodeEdge)
      cg = forceRight cgt
      acgt = assignPathsUntyped cg
      ncg = forceRight acgt
  in graphDataLexico . tieNodes $ ncg


instance GraphVertexOperations MyV where
  vertexToId = mvId
  expandVertexAsVertices = mvParents

myv :: String -> [MyV] -> [MyV] -> MyV
myv s logical inner = MyV (VertexId (C8.pack s)) logical inner

myvToVertex :: MyV -> Vertex MyV
myvToVertex x = Vertex (mvId x) x

buildScopes :: [MyV] -> Scopes
buildScopes l = iGetScopes0 l' fun where
  l' = myvToVertex <$> l
  fun vx = ParentSplit {
    psLogical = myvToVertex <$> (mvLogical . vertexData $ vx),
    psInner = myvToVertex <$> (mvParents . vertexData $ vx) }

simple :: [(Maybe String, [String])] -> Scopes
simple [] = M.empty
simple ((ms, ss) : t) =
  let
    key = VertexId . C8.pack <$> ms
    vals = VertexId . C8.pack <$> ss
    new = M.singleton key (S.fromList vals)
  in mergeScopes new (simple t)

gatherings :: [(String, [[String]])] -> M.Map VertexId [[VertexId]]
gatherings [] = M.empty
gatherings ((key, paths) : t) =
  let
    k = VertexId . C8.pack $ key
    ps = (VertexId . C8.pack <$>) <$> paths
    new = M.singleton k ps
  in M.unionWith (++) new (gatherings t)

gatherPaths' :: [MyV] -> M.Map VertexId [[VertexId]]
gatherPaths' = gatherPaths . buildScopes

spec :: Spec
spec = do
  describe "Tests on paths" $ do
    it "nothing" $ do
      buildScopes [] `shouldBe` simple []
    it "no parent" $ do
      let v0 = myv "v0" [] []
      let res = [ (Nothing, ["v0"]), (Just "v0", []) ]
      buildScopes [v0] `shouldBe` simple res
    it "one logical parent" $ do
      let v0 = myv "v0" [] []
      let v1 = myv "v1" [v0] []
      let res = [ (Nothing, ["v0", "v1"])
                , (Just "v1", [])
                , (Just "v0", []) ]
      buildScopes [v1, v0] `shouldBe` simple res
    it "one inner parent" $ do
      let v0 = myv "v0" [] []
      let v1 = myv "v1" [] [v0]
      let res = [ (Nothing, ["v1"])
                , (Just "v1", ["v0"])
                , (Just "v0", []) ]
      buildScopes [v1, v0] `shouldBe` simple res
    it "logical scoping over a parent" $ do
      let v0 = myv "v0" [] []
      let v1 = myv "v1" [v0] []
      let v2 = myv "v2" [v0] [v1]
      let res = [ (Nothing, ["v0", "v2"])
                , (Just "v0", [])
                , (Just "v1", [])
                , (Just "v2", ["v1"]) ]
      buildScopes [v2] `shouldBe` simple res
    it "common ancestor" $ do
      let top = myv "top" [] []
      let inner = myv "inner" [top] []
      let v1 = myv "v1" [top] [inner]
      let v2 = myv "v2" [top] [inner]
      let res = [ (Nothing, ["top", "v1", "v2"])
                , (Just "inner", [])
                , (Just "top", [])
                , (Just "v1", ["inner"])
                , (Just "v2", ["inner"]) ]
      buildScopes [v1, v2] `shouldBe` simple res
    it "common ancestor, unbalanced" $ do
      let top = myv "top" [] []
      let inner = myv "inner" [top] []
      let v1 = myv "v1" [top] [inner]
      let v2 = myv "v2" [] [inner]
      let res = [ (Nothing, ["top", "v1", "v2"])
                , (Just "inner", [])
                , (Just "top", [])
                , (Just "v1", ["inner"])
                , (Just "v2", ["inner", "top"]) ]
      buildScopes [v1, v2] `shouldBe` simple res
  describe "Path gatherings" $ do
    it "nothing" $ do
      gatherPaths' [] `shouldBe` gatherings []
    it "no parent" $ do
      let v0 = myv "v0" [] []
      let res = [("v0", [[]])]
      gatherPaths' [v0] `shouldBe` gatherings res
    it "one logical parent" $ do
      let v0 = myv "v0" [] []
      let v1 = myv "v1" [v0] []
      let res = [ ("v1", [[]])
                , ("v0", [[]])]
      gatherPaths' [v1] `shouldBe` gatherings res
    it "one inner parent" $ do
      let v0 = myv "v0" [] []
      let v1 = myv "v1" [] [v0]
      let res = [ ("v1", [[]])
                , ("v0", [["v1"]])]
      gatherPaths' [v1] `shouldBe` gatherings res
    it "logical scoping over a parent" $ do
      let v0 = myv "v0" [] []
      let v1 = myv "v1" [v0] []
      let v2 = myv "v2" [v0] [v1]
      let res = [ ("v0", [[]])
                , ("v1", [["v2"]])
                , ("v2", [[]]) ]
      gatherPaths' [v2] `shouldBe` gatherings res
    it "common ancestor" $ do
      let top = myv "top" [] []
      let inner = myv "inner" [top] []
      let v1 = myv "v1" [top] [inner]
      let v2 = myv "v2" [top] [inner]
      let res = [ ("inner", [["v1"], ["v2"]])
                , ("top", [[]])
                , ("v1", [[]])
                , ("v2", [[]]) ]
      gatherPaths' [v1, v2] `shouldBe` gatherings res
  describe "Real paths" $ do
    it "simple test" $ do
      let c0 = constant (1 :: Int) @@ "c0"
      let c1 = identity c0 @@ "c1"
      let c2 = identity c1 `logicalParents` [untyped c0] @@ "c2"
      nodeId <$> nodeParents c1 `shouldBe` [nodeId c0]
      nodeId <$> nodeParents c2 `shouldBe` [nodeId c1]
      let withParents = T.unpack . catNodePath . nodePath <$> assignPaths (untyped c2)
      withParents `shouldBe` ["c0", "c2/c1", "c2"]
    it "simple test 2" $ do
      let ds = dataset ([1 ,2, 3, 4]::[Int]) @@ "ds"
      let c = count ds @@ "c"
      let c2 = (c + (identity c @@ "id")) `logicalParents` [untyped ds] @@ "c2"
      let withParents = T.unpack . catNodePath . nodePath <$> assignPaths (untyped c2)
      withParents `shouldBe`  ["ds", "c2/c","c2/id","c2"]
