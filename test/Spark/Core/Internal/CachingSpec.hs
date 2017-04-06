{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Spark.Core.Internal.CachingSpec where

import Test.Hspec

import qualified Data.ByteString.Char8 as C8
import Data.Either(isLeft, isRight)
import Control.Arrow((&&&))
import Data.Text(Text)
import Data.Foldable(toList)
import Formatting

import Spark.Core.Try
import Spark.Core.Functions
import Spark.Core.Column
import Spark.Core.ColumnFunctions
import Spark.Core.Internal.Caching
-- Required for instance resolution
import Spark.Core.StructuresInternal()
import Spark.Core.Internal.Client(LocalSessionId(..))
import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.DAGFunctions
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.ContextStructures
import Spark.Core.Internal.ContextInternal
import Spark.Core.Internal.Pruning(emptyNodeCache)

data TestType = AutocacheNode | CacheNode | UncacheNode | Dataset | Row deriving (Eq, Show)

data TestNode = TestNode { tnId :: VertexId, tnType :: TestType, tnParents :: [(StructureEdge, TestNode)] } deriving (Eq)

instance Show TestNode where
  show v = "TestNode(" ++ (C8.unpack . unVertexId. tnId $ v) ++ ")"

nid :: String -> VertexId
nid = VertexId . C8.pack

node :: String -> TestType -> [TestNode] -> TestNode
node s tp p = TestNode (VertexId (C8.pack s)) tp ((const ParentEdge &&& id) <$> p)

instance GraphVertexOperations TestNode where
  vertexToId = tnId
  expandVertexAsVertices x = snd <$> tnParents x

instance GraphOperations TestNode StructureEdge where
  expandVertex tn = tnParents tn

acGen :: AutocacheGen TestNode
acGen =
  let deriveUncache' (Vertex (VertexId x) (TestNode _ AutocacheNode _)) =
        let vid' = VertexId $ C8.pack . (++ "_uncache") . C8.unpack $ x
        in Vertex vid' (TestNode vid' UncacheNode [])
      deriveUncache' x = error (show x)
      deriveIdentity' (Vertex (VertexId x) (TestNode _ r _)) =
        let vid' = VertexId $ C8.pack . (++ "_identity") . C8.unpack $ x
        in Vertex vid' (TestNode vid' r [])
  in AutocacheGen {
    deriveUncache = deriveUncache',
    deriveIdentity = deriveIdentity'
  }

expandFun :: TestNode -> CacheTry NodeCachingType
expandFun n = case (tnType n, tnParents n) of
  (AutocacheNode, [_]) -> pure $ AutocacheOp (tnId n)
  (AutocacheNode, x) -> Left $ sformat ("Node: "%shown%": expected one parent for autocaching, got "%shown) n x
  (CacheNode, [_]) -> pure $ CacheOp (tnId n)
  (CacheNode, x) -> Left $ sformat ("Node: "%shown%": expected one parent for caching, got "%shown) n x
  (UncacheNode, [(ParentEdge, x)]) -> pure $ UncacheOp (tnId n) (tnId x)
  (UncacheNode, x) -> Left $ sformat ("Node: "%shown%": Expected one parent for uncaching, got "%shown) n x
  (Dataset, _) -> Right Through
  (Row, _) -> Right Stop

errors :: TestNode -> CacheTry [CachingFailure]
errors tn = do
  g <- buildGraph tn :: Either Text (Graph TestNode StructureEdge)
  checkCaching (graphMapEdges g (const ParentEdge)) expandFun

errors' :: TestNode -> CacheTry (Graph TestNode StructureEdge)
errors' tn = do
  g <- buildGraph tn :: Either Text (Graph TestNode StructureEdge)
  fillAutoCache expandFun acGen g

intErrors :: LocalData a -> Try ComputeGraph
intErrors ld =
  let cg = buildComputationGraph ld
  in performGraphTransforms emptySession =<< cg

emptySession :: SparkSession
emptySession = SparkSession c (LocalSessionId "id") 3 emptyNodeCache
  where c = SparkSessionConf "end_point" (negate 1) 10 "session_name" True

spec :: Spec
spec = do
  describe "Caching operations" $ do
    it "missing parent node" $ do
      let n1 = node "1" CacheNode []
      errors n1 `shouldSatisfy` isLeft
    it "caching: parent node" $ do
      let n0 = node "0" Dataset []
      let n1 = node "1" CacheNode [n0]
      errors n1 `shouldBe` Right []
    it "uncaching: missing parent node" $ do
      let n1 = node "1" UncacheNode []
      errors n1 `shouldSatisfy` isLeft
    it "uncaching: parent node" $ do
      let n0 = node "0" Dataset []
      let n1 = node "1" CacheNode [n0]
      let n2 = node "2" CacheNode [n1]
      errors n2 `shouldBe` Right []
    it "too many nodes for uncaching" $ do
      let n0 = node "0" Dataset []
      let n1 = node "1" CacheNode [n0]
      let n2 = node "2" UncacheNode [n1, n2]
      errors n2 `shouldSatisfy` isLeft
    it "access after uncaching" $ do
      let n0 = node "0" Dataset []
      let n1 = node "1" CacheNode [n0]
      let n2 = node "2" UncacheNode [n1]
      let n3 = node "3" Dataset [n1, n2]
      errors n3 `shouldBe` Right [CachingFailure (nid "1") (nid "2") (nid "3")]
    it "ambigous access after uncaching" $ do
      let n0 = node "0" Dataset []
      let n1 = node "1" CacheNode [n0]
      let n2 = node "2" UncacheNode [n1]
      let n3 = node "3" Dataset [n1]
      let n4 = node "4" Dataset [n3, n2]
      errors n4 `shouldBe` Right [CachingFailure (nid "1") (nid "2") (nid "3")
                                  ,CachingFailure (nid "1") (nid "2") (nid "4")]
  describe "Autocaching operations" $ do
    it "missing parent node" $ do
      let n1 = node "1" AutocacheNode []
      let g = traceHint "g=" (errors' n1)
      g `shouldSatisfy` isLeft
    it "auto-uncaching with no child should not create uncaching" $ do
      let n0 = node "0" Dataset []
      let n1 = node "1" AutocacheNode [n0]
      let g = traceHint "g=" (errors' n1)
      g `shouldSatisfy` isRight
      ((length . toList . gVertices) <$> g) `shouldBe` Right 2
    it "access after uncaching" $ do
      let n0 = node "0" Dataset []
      let n1 = node "1" AutocacheNode [n0]
      let n2 = node "2" Row [n1]
      let g = traceHint "g=" (errors' n2)
      g `shouldSatisfy` isRight
      ((length . toList . gVertices) <$> g) `shouldBe` Right 5
    it "access after and scoping" $ do
      let n0 = node "0" Dataset []
      let n1 = node "1" AutocacheNode [n0]
      let n2a = node "2a" Row [n1]
      let n2b = node "2b" Row [n1]
      let n3 = node "3" Row [n2a, n2b]
      let g = traceHint "g=" (errors' n3)
      g `shouldSatisfy` isRight
      ((length . toList . gVertices) <$> g) `shouldBe` Right 8
  describe "Autocaching integration tests" $ do
    it "test 1" $ do
      let l = [1,2,3] :: [Int]
      let ds = dataset l
      let ds' = autocache ds
      let c1 = asCol ds'
      let s1 = sumCol c1
      let s2 = count ds'
      let x = s1 + s2
      let g = traceHint "g=" (intErrors x)
      g `shouldSatisfy` isRight
