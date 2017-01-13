{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}


module Spark.Core.Internal.OpFunctions(
  simpleShowOp,
  extraNodeOpData,
  hashUpdateNodeOp,
  prettyShowColOp,
) where

import qualified Data.Text as T
import qualified Data.Aeson as A
import qualified Data.Vector as V
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Text(Text)
import Data.Aeson((.=), toJSON)
import Data.Char(isSymbol)
import qualified Crypto.Hash.SHA256 as SHA

import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.Utilities

-- (internal)
-- The serialized type of a node operation, as written in
-- the JSON description.
simpleShowOp :: NodeOp -> T.Text
simpleShowOp (NodeLocalOp op) = soName op
simpleShowOp (NodeDistributedOp op) = soName op
simpleShowOp (NodeLocalLit _ _) = "org.spark.LocalConstant"
simpleShowOp (NodeOpaqueAggregator op) = soName op
simpleShowOp (NodeAggregatorReduction ua) = _prettyShowAggTrans . uaoInitialOuter $ ua
simpleShowOp (NodeAggregatorLocalReduction ua) = _prettyShowSGO . uaoMergeBuffer $ ua
simpleShowOp (NodeStructuredTransform _) = "org.spark.Select"
simpleShowOp (NodeDistributedLit _ _) = "org.spark.Constant"
simpleShowOp (NodeGroupedReduction _) = missing "simpleShowOp: NodeGroupedReduction"
simpleShowOp (NodeReduction _) = missing "simpleShowOp: NodeReduction"

-- A human-readable string that represents column operations.
prettyShowColOp :: ColOp -> T.Text
prettyShowColOp (ColExtraction fpath) = T.pack (show fpath)
prettyShowColOp (ColFunction txt cols) =
  _prettyShowColFun txt (V.toList (prettyShowColOp <$> cols))
prettyShowColOp (ColLit _ cell) = T.pack (show cell)
prettyShowColOp (ColStruct s) =
  "struct(" <> T.intercalate "," (prettyShowColOp . tfValue <$> V.toList s) <> ")"

_prettyShowAggOp :: AggOp -> T.Text
_prettyShowAggOp (AggUdaf _ ucn fp) = ucn <> "(" <> show' fp <> ")"
_prettyShowAggOp (AggFunction sfn v) = _prettyShowColFun sfn r where
  r = V.toList (show' <$> v)
_prettyShowAggOp (AggStruct v) =
  "struct(" <> T.intercalate "," (_prettyShowAggOp . afValue <$> V.toList v) <> ")"

_prettyShowAggTrans :: AggTransform -> Text
_prettyShowAggTrans (OpaqueAggTransform op) = soName op
_prettyShowAggTrans (InnerAggOp ao) = _prettyShowAggOp ao
_prettyShowAggTrans (InnerAggStruct v) = _prettyShowAggOp (AggStruct v)

_prettyShowSGO :: SemiGroupOperator -> Text
_prettyShowSGO (OpaqueSemiGroupLaw so) = soName so
_prettyShowSGO (UdafSemiGroupOperator ucn) = ucn
_prettyShowSGO (ColumnSemiGroupLaw sfn) = sfn

-- (internal)
-- The extra data associated with the operation, and that is required
-- by the backend to successfully perform the operation.
-- We pass the type as seen by krapsh (along with some extra information about
-- nullability). This information is required by spark to analyze the exact
-- type of some operations.
extraNodeOpData :: NodeOp -> A.Value
extraNodeOpData (NodeLocalLit dt cell) =
  A.object [ "type" .= toJSON dt,
             "content" .= toJSON cell]
extraNodeOpData (NodeStructuredTransform st) = toJSON st
extraNodeOpData (NodeDistributedLit dt lst) =
  -- The backend deals with all the details translating the augmented type
  -- as a SQL datatype.
  A.object [ "cellType" .= toJSON dt,
             "content" .= toJSON lst]
extraNodeOpData (NodeDistributedOp so) = soExtra so
extraNodeOpData _ = A.Null

-- Adds the content of a node op to a hash.
-- Right now, this builds the json representation and passes it
-- to the hash function, which simplifies the verification on
-- on the server side.
-- TODO: this depends on some implementation details such as the hashing
-- function used by Aeson.
hashUpdateNodeOp :: SHA.Ctx -> NodeOp -> SHA.Ctx
hashUpdateNodeOp ctx op = _hashUpdateJson ctx $ A.object [
  "op" .= simpleShowOp op,
  "extra" .= extraNodeOpData op]


_prettyShowColFun :: T.Text -> [Text] -> T.Text
_prettyShowColFun txt [col] | _isSym txt =
  T.concat [txt, col]
_prettyShowColFun txt [col1, col2] | _isSym txt =
  -- This is not perfect for complex operations, but it should get the job done
  -- for now.
  -- TODO eventually use operator priority here
  T.concat [col1, txt, col2]
_prettyShowColFun txt cols =
  let vals = T.intercalate ", " cols in
  T.concat [txt, "(", vals, ")"]

_isSym :: T.Text -> Bool
_isSym txt = all isSymbol (T.unpack txt)

-- This schema is not great because there is some ambiguity about the final
-- nodes.
-- Someone could craft a JSON that would confuse the object detection.
-- Not sure if this is much of a security risk anyway.
instance A.ToJSON ColOp where
  toJSON (ColExtraction fp) = A.object [
    "colOp" .= T.pack "extraction",
    "field" .= toJSON fp]
  toJSON (ColFunction txt cols) = A.object [
    "colOp" .= T.pack "fun",
    "function" .= txt,
    "args" .= (toJSON <$> cols)]
  toJSON (ColLit _ cell) = A.object [
    "colOp" .= T.pack "literal",
    "lit" .= toJSON cell]
  toJSON (ColStruct v) =
    let fun (TransformField fn colOp) =
          A.object ["name" .= T.pack (show fn), "op" .= toJSON colOp]
    in A.Array $ fun <$> v


_hashUpdateJson :: SHA.Ctx -> A.Value -> SHA.Ctx
_hashUpdateJson ctx val = SHA.update ctx bs where
  bs = BS.concat . LBS.toChunks . encodeDeterministicPretty $ val
