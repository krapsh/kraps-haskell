{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Spark.Core.Internal.DatasetFunctions(
  parents,
  untyped,
  untyped',
  logicalParents,
  logicalParents',
  depends,
  dataframe,
  asDF,
  asDS,
  asLocalObservable,
  asObservable,
  -- Standard functions
  identity,
  autocache,
  cache,
  uncache,
  union,
  -- Developer
  castLocality,
  emptyDataset,
  emptyLocalData,
  emptyNodeStandard,
  nodeId,
  nodeLogicalDependencies,
  nodeLogicalParents,
  nodeLocality,
  nodeName,
  nodePath,
  nodeOp,
  nodeParents,
  nodeType,
  untypedDataset,
  untypedLocalData,
  updateNode,
  updateNodeOp,
  broadcastPair,
  -- Developer conversions
  -- TODO: remove all that
  fun1ToOpTyped,
  fun2ToOpTyped,
  nodeOpToFun1,
  nodeOpToFun1Typed,
  nodeOpToFun1Untyped,
  nodeOpToFun2,
  nodeOpToFun2Typed,
  nodeOpToFun2Untyped,
  unsafeCastDataset,
  placeholder,
  castType,
  castType',
  -- Internal
  opnameCache,
  opnameUnpersist,
  opnameAutocache,

) where

import qualified Crypto.Hash.SHA256 as SHA
import qualified Data.Aeson as A
import qualified Data.Text as T
import qualified Data.Text.Format as TF
import qualified Data.Vector as V
import Data.Aeson((.=), toJSON)
import Data.Text.Encoding(decodeUtf8)
import Data.ByteString.Base16(encode)
import Data.Maybe(fromMaybe, listToMaybe)
import Data.Text.Lazy(toStrict)
import Data.String(IsString(fromString))
import Formatting

import Spark.Core.StructuresInternal
import Spark.Core.Try
import Spark.Core.Row
import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.OpFunctions
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.RowUtils
import Spark.Core.Internal.TypesGenerics
import Spark.Core.Internal.TypesFunctions

-- | (developer) The operation performed by this node.
nodeOp :: ComputeNode loc a -> NodeOp
nodeOp = _cnOp

-- | The nodes this node depends on directly.
nodeParents :: ComputeNode loc a -> [UntypedNode]
nodeParents = V.toList . _cnParents

-- | (developer) Returns the logical parenst of a node.
nodeLogicalParents :: ComputeNode loc a -> Maybe [UntypedNode]
nodeLogicalParents = (V.toList <$>) . _cnLogicalParents

-- | Returns the logical dependencies of a node.
nodeLogicalDependencies :: ComputeNode loc a -> [UntypedNode]
nodeLogicalDependencies = V.toList . _cnLogicalDeps

-- | The name of a node.
-- TODO: should be a NodePath
nodeName :: ComputeNode loc a -> NodeName
nodeName node = fromMaybe (_defaultNodeName node) (_cnName node)

{-| The path of a node, as resolved.

This path includes information about the logical parents (after resolution).
-}
nodePath :: ComputeNode loc a -> NodePath
nodePath node =
  if V.null . unNodePath . _cnPath $ node
    then NodePath . V.singleton . nodeName $ node
    else _cnPath node

-- | The type of the node
-- TODO have nodeType' for dynamic types as well
nodeType :: ComputeNode loc a -> SQLType a
nodeType = SQLType . _cnType

{-| The identity function.

Returns a compute node with the same datatype and the same content as the
previous node. If the operation of the input has a side effect, this side
side effect is *not* reevaluated.

This operation is typically used when establishing an ordering between some
operations such as caching or side effects, along with `logicalDependencies`.
-}
identity :: ComputeNode loc a -> ComputeNode loc a
identity n = n2 `parents` [untyped n]
  where n2 = emptyNodeStandard (nodeLocality n) (nodeType n) name
        name = if _cnLocality n == Local
                then "org.spark.LocalIdentity"
                else "org.spark.Identity"

{-| Caches the dataset.

This function instructs Spark to cache a dataset with the default persistence
level in Spark (MEMORY_AND_DISK).

Note that the dataset will have to be evaluated first for the caching to take
effect, so it is usual to call `count` or other aggregrators to force
the caching to occur.
-}
cache :: Dataset a -> Dataset a
cache  n = n2 `parents` [untyped n]
  where n2 = emptyNodeStandard (nodeLocality n) (nodeType n) opnameCache

-- (internal)
opnameCache :: T.Text
opnameCache = "org.spark.Cache"

{-| Uncaches the dataset.

This function instructs Spark to unmark the dataset as cached. The disk and the
memory used by Spark in the future.

Unlike Spark, Karps is stricter with the uncaching operation:
 - the argument of cache must be a cached dataset
 - once a dataset is uncached, its cached version cannot be used again (i.e. it
   must be recomputed).

Karps performs escape analysis and will refuse to run programs with caching
issues.
-}
uncache :: ComputeNode loc a -> ComputeNode loc a
uncache  n = n2 `parents` [untyped n]
  where n2 = emptyNodeStandard (nodeLocality n) (nodeType n) opnameUnpersist

-- (internal)
opnameUnpersist :: T.Text
opnameUnpersist = "org.spark.Unpersist"

{-| Automatically caches the dataset on a need basis, and performs deallocation
when the dataset is not required.

This function marks a dataset as eligible for the default caching level in
Spark. The current implementation performs caching only if it can be established
that the dataset is going to be involved in more than one shuffling or
aggregation operation.

If the dataset has no observable child, no uncaching operation is added: the
autocache operation is equivalent to unconditional caching.
-}
autocache :: Dataset a -> Dataset a
autocache n = n2 `parents` [untyped n]
  where n2 = emptyNodeStandard (nodeLocality n) (nodeType n) opnameAutocache

opnameAutocache :: T.Text
opnameAutocache = "org.spark.Autocache"

{-| Returns the union of two datasets.

In the context of streaming and differentiation, this union is biased towards
the left: the left argument expresses the stream and the right element expresses
the increment.
-}
union :: Dataset a -> Dataset a -> Dataset a
union n1 n2 = n `parents` [untyped n1, untyped n2]
  where n = emptyNodeStandard (nodeLocality n1) (nodeType n1) _opnameUnion

_opnameUnion :: T.Text
_opnameUnion = "org.spark.Union"

-- | Converts to a dataframe and drops the type info.
-- This always works.
asDF :: ComputeNode LocDistributed a -> DataFrame
asDF = pure . _unsafeCastNode

-- | Attempts to convert a dataframe into a (typed) dataset.
--
-- This will fail if the dataframe itself is a failure, of if the casting
-- operation is not correct.
-- This operation assumes that both field names and types are correct.
asDS :: forall a. (SQLTypeable a) => DataFrame -> Try (Dataset a)
asDS = _asTyped


-- | Converts a local node to a local frame.
-- This always works.
asLocalObservable :: ComputeNode LocLocal a -> LocalFrame
asLocalObservable = pure . _unsafeCastNode

asObservable :: forall a. (SQLTypeable a) => LocalFrame -> Try (LocalData a)
asObservable = _asTyped

-- | Converts any node to an untyped node
untyped :: ComputeNode loc a -> UntypedNode
untyped = _unsafeCastNode

untyped' :: Try (ComputeNode loc a) -> UntypedNode'
untyped' = fmap untyped


untypedDataset :: ComputeNode LocDistributed a -> UntypedDataset
untypedDataset = _unsafeCastNode

{-| Removes type informatino from an observable. -}
untypedLocalData :: ComputeNode LocLocal a -> UntypedLocalData
untypedLocalData = _unsafeCastNode

{-| Adds parents to the node.
It is assumed the parents are the unique set of nodes required
by the operation defined in this node.
If you want to set parents for the sake of organizing computation
use logicalParents.
If you want to add some timing dependencies between nodes,
use depends.
-}
parents :: ComputeNode loc a -> [UntypedNode] -> ComputeNode loc a
parents node l = updateNode node $ \n ->
  n { _cnParents = V.fromList l V.++ _cnParents n }

{-| Establishes a naming convention on this node: the path of this node will be
determined as if the parents of this node were the list provided (and without
any effect from the direct parents of this node).

For this to work, the logical parents should split the nodes between internal
nodes, logical parents, and the rest. In other words, for any ancestor of this node,
and for any valid path to reach this ancestor, this path should include at least one
node from the logical dependencies.

This set can be a super set of the actual logical parents.

The check is lazy (done during the analysis phase). An error (if any) will
only be reported during analysis.
-}
logicalParents :: ComputeNode loc a -> [UntypedNode] -> ComputeNode loc a
logicalParents node l = updateNode node $ \n ->
  n { _cnLogicalParents = pure . V.fromList $ l }

logicalParents' :: Try (ComputeNode loc a) -> [UntypedNode'] -> Try (ComputeNode loc a)
logicalParents' n' l' = do
  n <- n'
  l <- sequence l'
  return (logicalParents n l)

{-| Sets the logical dependencies on this node.

All the nodes given will be guaranteed to be executed before the current node.

If there are any failures, this node will also be treated as a failure (even
if the parents are all successes).
-}
depends :: ComputeNode loc a -> [UntypedNode] -> ComputeNode loc a
depends node l = updateNode node $ \n ->
  n { _cnLogicalDeps = V.fromList l }


-- (internal)
-- Tries to update the locality of a node. This is a checked cast.
-- TODO: remove, it is only used to cast to local frame
castLocality :: forall a loc loc'. (CheckedLocalityCast loc') =>
    ComputeNode loc a -> Try (ComputeNode loc' a)
castLocality node =
  let
    loc2 = _cnLocality node
    locs = unTypedLocality <$> (_validLocalityValues :: [TypedLocality loc'])
  in if locs == [loc2] then
    pure $ node { _cnLocality = loc2 }
  else
    tryError $ sformat ("Wrong locality :"%shown%", expected: "%shown) loc2 locs

-- (internal)
-- The id of a node. If it is not set in the node, it will be
-- computed from scratch.
-- This is a potentially long operation.
nodeId :: ComputeNode loc a -> NodeId
nodeId = _cnNodeId

-- (internal)
-- This operation should always be used to make sure that the
-- various caches inside the compute node are maintained.
updateNode :: ComputeNode loc a -> (ComputeNode loc a -> ComputeNode loc' b) -> ComputeNode loc' b
updateNode ds f = ds2 { _cnNodeId = id2 } where
  ds2 = f ds
  id2 = _nodeId ds2


updateNodeOp :: ComputeNode loc a -> NodeOp -> ComputeNode loc a
updateNodeOp n no = updateNode n (\n' -> n' { _cnOp = no })

-- (internal)
-- The locality of the node
nodeLocality :: ComputeNode loc a -> TypedLocality loc
nodeLocality = TypedLocality . _cnLocality

-- (internal)
emptyDataset :: NodeOp -> SQLType a -> Dataset a
emptyDataset = _emptyNode

-- (internal)
emptyLocalData :: NodeOp -> SQLType a -> LocalData a
emptyLocalData = _emptyNode

{-| Creates a dataframe from a list of cells and a datatype.

Wil fail if the content of the cells is not compatible with the
data type.
-}
dataframe :: DataType -> [Cell] -> DataFrame
dataframe dt cells' = do
  validCells <- tryEither $ sequence (checkCell dt <$> cells')
  let jData = V.fromList (toJSON <$> validCells)
  let op = NodeDistributedLit dt jData
  return $ _emptyNode op (SQLType dt)


-- *********** function / object conversions *******

-- | (internal)
placeholderTyped :: forall a loc. (IsLocality loc) =>
  SQLType a -> ComputeNode loc a
placeholderTyped tp = _unsafeCastNode n where
  n = placeholder (unSQLType tp) :: ComputeNode loc Cell

placeholder :: forall loc. (IsLocality loc) => DataType -> ComputeNode loc Cell
placeholder tp =
  let
    t = SQLType tp
    so = makeOperator "org.spark.Placeholder" t
    (TypedLocality l) = _getTypedLocality :: TypedLocality loc
    op = case l of
      Local -> NodeLocalOp so
      Distributed -> NodeDistributedOp so
  in  _emptyNode op t

-- | (internal) conversion
fun1ToOpTyped :: forall a loc a' loc'. (IsLocality loc) =>
  SQLType a -> (ComputeNode loc a -> ComputeNode loc' a') -> NodeOp
fun1ToOpTyped sqlt f = nodeOp $ f (placeholderTyped sqlt)

-- | (internal) conversion
fun2ToOpTyped :: forall a1 a2 a loc1 loc2 loc. (IsLocality loc1, IsLocality loc2) =>
  SQLType a1 -> SQLType a2 -> (ComputeNode loc1 a1 -> ComputeNode loc2 a2 -> ComputeNode loc a) -> NodeOp
fun2ToOpTyped sqlt1 sqlt2 f = nodeOp $ f (placeholderTyped sqlt1) (placeholderTyped sqlt2)

-- | (internal) conversion
nodeOpToFun1 :: forall a1 a2 loc1 loc2. (SQLTypeable a2, IsLocality loc2) =>
  NodeOp -> ComputeNode loc1 a1 -> ComputeNode loc2 a2
nodeOpToFun1 = nodeOpToFun1Typed (buildType :: SQLType a2)

-- | (internal) conversion
nodeOpToFun1Typed :: forall a1 a2 loc1 loc2. (IsLocality loc2) =>
  SQLType a2 -> NodeOp -> ComputeNode loc1 a1 -> ComputeNode loc2 a2
nodeOpToFun1Typed sqlt no node =
  let n2 = _emptyNode no sqlt :: ComputeNode loc2 a2
  in n2 `parents` [untyped node]

-- | (internal) conversion
nodeOpToFun1Untyped :: forall loc1 loc2. (IsLocality loc2) =>
  DataType -> NodeOp -> ComputeNode loc1 Cell -> ComputeNode loc2 Cell
nodeOpToFun1Untyped dt no node =
  let n2 = _emptyNode no (SQLType dt) :: ComputeNode loc2 Cell
  in n2 `parents` [untyped node]

-- | (internal) conversion
nodeOpToFun2 :: forall a a1 a2 loc loc1 loc2. (SQLTypeable a, IsLocality loc) =>
  NodeOp -> ComputeNode loc1 a1 -> ComputeNode loc2 a2 -> ComputeNode loc a
nodeOpToFun2 = nodeOpToFun2Typed (buildType :: SQLType a)

-- | (internal) conversion
nodeOpToFun2Typed :: forall a a1 a2 loc loc1 loc2. (IsLocality loc) =>
  SQLType a -> NodeOp -> ComputeNode loc1 a1 -> ComputeNode loc2 a2 -> ComputeNode loc a
nodeOpToFun2Typed sqlt no node1 node2 =
  let n2 = _emptyNode no sqlt :: ComputeNode loc a
  in n2 `parents` [untyped node1, untyped node2]

-- | (internal) conversion
nodeOpToFun2Untyped :: forall loc1 loc2 loc3. (IsLocality loc3) =>
  DataType -> NodeOp -> ComputeNode loc1 Cell -> ComputeNode loc2 Cell -> ComputeNode loc3 Cell
nodeOpToFun2Untyped dt no node1 node2 =
  let n2 = _emptyNode no (SQLType dt) :: ComputeNode loc3 Cell
  in n2 `parents` [untyped node1, untyped node2]


{-| Low-level operator that takes an observable and propagates it along the
content of an existing dataset.

Users are advised to use the Column-based `broadcast` function instead.
-}
broadcastPair :: Dataset a -> LocalData b -> Dataset (a, b)
broadcastPair ds ld = n `parents` [untyped ds, untyped ld]
  where n = emptyNodeStandard (nodeLocality ds) sqlt name
        sqlt = tupleType (nodeType ds) (nodeType ld)
        name = "org.spark.BroadcastPair"

-- ******* INSTANCES *********

-- Put here because it depends on some private functions.
instance forall loc a. Show (ComputeNode loc a) where
  show ld = let
    txt = fromString "{}@{}{}{}" :: TF.Format
    loc :: T.Text
    loc = case nodeLocality ld of
      TypedLocality Local -> "!"
      TypedLocality Distributed -> ":"
    np = prettyNodePath . nodePath $ ld
    no = prettyShowOp . nodeOp $ ld
    fields = T.pack . show . nodeType $ ld in
      T.unpack $ toStrict $ TF.format txt (np, no, loc, fields)

instance forall loc a. A.ToJSON (ComputeNode loc a) where
  toJSON node = A.object [
    "locality" .= nodeLocality node,
    "path" .= nodePath node,
    "op" .= (simpleShowOp . nodeOp $ node),
    "extra" .= (extraNodeOpData . nodeOp $ node),
    "parents" .= (nodePath <$> nodeParents node),
    "logicalDependencies" .= (nodePath <$> nodeLogicalDependencies node),
    "_type" .= (unSQLType . nodeType) node]

instance forall loc. A.ToJSON (TypedLocality loc) where
  toJSON (TypedLocality Local) = A.String "local"
  toJSON (TypedLocality Distributed) = A.String "distributed"

unsafeCastDataset :: ComputeNode LocDistributed a -> ComputeNode LocDistributed b
unsafeCastDataset ds = ds { _cnType = _cnType ds }

-- TODO: figure out the story around haskell types vs datatypes
-- Should we have equivalence classes for haskell, so that a tuple has the
-- same type as a structure?
-- Probably not, it breaks the correspondence.
-- Probably, it makes the metadata story easier.
castType :: SQLType a -> ComputeNode loc b -> Try (ComputeNode loc a)
castType sqlt n = do
  let dt = unSQLType sqlt
  let dt' = unSQLType (nodeType n)
  if dt `compatibleTypes` dt'
    then let n' = updateNode n (\node -> node { _cnType = dt }) in
      pure (_unsafeCastNode n')
    else tryError $ sformat ("castType: Casting error: dataframe has type "%sh%" incompatible with type "%sh) dt' dt

castType' :: SQLType a -> Try (ComputeNode loc Cell) -> Try (ComputeNode loc a)
castType' sqlt df = df >>= castType sqlt

_asTyped :: forall loc a. (SQLTypeable a) => Try (ComputeNode loc Cell) -> Try (ComputeNode loc a)
_asTyped = castType' (buildType :: SQLType a)

-- Performs an unsafe type recast.
-- This is useful for internal code that knows whether
-- this operation is legal or not through some other means.
-- This may still throw an error if the cast is illegal.
_unsafeCastNode :: ComputeNode loc1 a -> ComputeNode loc2 b
_unsafeCastNode x = x {
    _cnType = _cnType x,
    _cnLocality = _cnLocality x
  }

_unsafeCastNodeTyped :: TypedLocality loc2 -> ComputeNode loc1 a -> ComputeNode loc2 b
_unsafeCastNodeTyped l x = x {
    _cnType = _cnType x,
    _cnLocality = unTypedLocality l
  }

--
_unsafeCastLoc :: CheckedLocalityCast loc' =>
  TypedLocality loc -> TypedLocality loc'
_unsafeCastLoc (TypedLocality Local) =
  checkLocalityValidity (TypedLocality Local)
_unsafeCastLoc (TypedLocality Distributed) =
  checkLocalityValidity (TypedLocality Distributed)


-- This should be a programming error
checkLocalityValidity :: forall loc. (HasCallStack, CheckedLocalityCast loc) =>
  TypedLocality loc -> TypedLocality loc
checkLocalityValidity x =
  if x `notElem` _validLocalityValues
    then
      let msg = sformat ("CheckedLocalityCast: element "%shown%" not in the list of accepted values: "%shown)
                  x (_validLocalityValues :: [TypedLocality loc])
      in failure msg x
    else x


-- Computes the ID of a node.
-- Since this is a complex operation, it should be cached by each node.
_nodeId :: ComputeNode loc a -> NodeId
_nodeId node =
  let c1 = SHA.init
      f2 = unNodeId . nodeId
      c2 = hashUpdateNodeOp c1 (nodeOp node)
      c3 = SHA.updates c2 $ f2 <$> nodeParents node
      c4 = SHA.updates c3 $ f2 <$> nodeLogicalDependencies node
      -- c6 = SHA.update c4 $ (BS.concat . LBS.toChunks) b
  in
    -- Using base16 encoding to make sure it is readable.
    -- Not sure if it is a good idea in general.
    (NodeId . encode . SHA.finalize) c4

_defaultNodeName :: ComputeNode loc a -> NodeName
_defaultNodeName node =
  let opName = (prettyShowOp . nodeOp) node
      namePieces = T.splitOn (T.pack ".") opName
      lastOpt = (listToMaybe . reverse) namePieces
      l = fromMaybe (T.pack "???") lastOpt
      idbs = nodeId node
      idt = (T.take 6 . decodeUtf8 . unNodeId) idbs
      n = T.concat [T.toLower l, T.pack "_", idt]
  in NodeName n

-- Create a new empty node. Also performs a locality check to
-- make sure the info being provided is correct.
_emptyNode :: forall loc a. (IsLocality loc) =>
  NodeOp -> SQLType a -> ComputeNode loc a
_emptyNode op sqlt = _emptyNodeTyped (_getTypedLocality :: TypedLocality loc) sqlt op

_emptyNodeTyped :: forall loc a.
  TypedLocality loc -> SQLType a -> NodeOp -> ComputeNode loc a
_emptyNodeTyped tloc (SQLType dt) op = updateNode (_unsafeCastNodeTyped tloc ds) id where
  ds :: ComputeNode loc a
  ds = ComputeNode {
    _cnName = Nothing,
    _cnOp = op,
    _cnType = dt,
    _cnParents = V.empty,
    _cnLogicalParents = Nothing,
    _cnLogicalDeps = V.empty,
    _cnLocality = unTypedLocality tloc,
    _cnNodeId = error "_emptyNode: _cnNodeId",
    _cnPath = NodePath V.empty
  }

emptyNodeStandard :: forall loc a.
  TypedLocality loc -> SQLType a -> T.Text -> ComputeNode loc a
emptyNodeStandard tloc sqlt name = _emptyNodeTyped tloc sqlt op where
  so = StandardOperator {
         soName = name,
         soOutputType = unSQLType sqlt,
         soExtra = A.Null
       }
  op = if unTypedLocality tloc == Local
          then NodeLocalOp so
          else NodeDistributedOp so
