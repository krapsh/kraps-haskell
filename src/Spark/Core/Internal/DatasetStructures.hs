{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}

module Spark.Core.Internal.DatasetStructures where

import Data.Vector(Vector)

import Spark.Core.StructuresInternal
import Spark.Core.Try
import Spark.Core.Row
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.TypesStructures

{-| (internal) The main data structure that represents a data node in the
computation graph.

This data structure forms the backbone of computation graphs expressed
with spark operations.

loc is a typed locality tag.
a is the type of the data, as seen by the Haskell compiler. If erased, it
will be a Cell type.
-}
-- TODO: separate the topology info from the node info. It will help when
-- building the graphs.
data ComputeNode loc a = ComputeNode {
  -- | The id of the node.
  --
  -- Non strict because it may be expensive.
  _cnNodeId :: NodeId,
  -- The following fields are used to build a unique ID to
  -- a compute node:

  -- | The operation associated to this node.
  _cnOp :: !NodeOp,
  -- | The type of the node
  _cnType :: !DataType,
  -- | The direct parents of the node. The order of the parents is important
  -- for the semantics of the operation.
  _cnParents :: !(Vector UntypedNode),
  -- | A set of extra dependencies that can be added to force an order between
  -- the nodes.
  --
  -- The order is not important, they are sorted by ID.
  --
  -- TODO(kps) add this one to the id
  _cnLogicalDeps :: !(Vector UntypedNode),
  -- | The locality of this node.
  --
  -- TODO(kps) add this one to the id
  _cnLocality :: !Locality,
  -- Attributes that are not included in the id
  -- These attributes are mostly for the user to relate to the nodes.
  -- They are not necessary for the computation.
  --
  -- | The name
  _cnName :: !(Maybe NodeName),
  -- | A set of nodes considered as the logical input for this node.
  -- This has no influence on the calculation of the id and is used
  -- for organization purposes only.
  _cnLogicalParents :: !(Maybe (Vector UntypedNode)),
  -- | The path of this oned in a computation flow.
  --
  -- This path includes the node name.
  -- Not strict because it may be expensive to compute.
  -- By default it only contains the name of the node (i.e. the node is
  -- attached to the root)
  _cnPath :: NodePath
} deriving (Eq)

-- (internal) Phantom type tags for the locality
data TypedLocality loc = TypedLocality { unTypedLocality :: !Locality } deriving (Eq, Show)
data LocLocal
data LocDistributed
data LocUnknown

-- (developer) The type for which we drop all the information expressed in
-- types.
--
-- This is useful to express parent dependencies (pending a more type-safe
-- interface)
type UntypedNode = ComputeNode LocUnknown Cell

-- (internal) A dataset for which we have dropped type information.
-- Used internally by columns.
type UntypedDataset = Dataset Cell

type UntypedLocalData = LocalData Cell

{-| A typed collection of distributed data.

Most operations on datasets are type-checked by the Haskell
compiler: the type tag associated to this dataset is guaranteed
to be convertible to a proper Haskell type. In particular, building
a Dataset of dynamic cells is guaranteed to never happen.

If you want to do untyped operations and gain
some flexibility, consider using UDataFrames instead.

Computations with Datasets and observables are generally checked for
correctness using the type system of Haskell.
-}
type Dataset a = ComputeNode LocDistributed a

{-|
A unit of data that can be accessed by the user.

This is a typed unit of data. The type is guaranteed to be a proper
type accessible by the Haskell compiler (instead of simply a Cell
type, which represents types only accessible at runtime).

TODO(kps) rename to Observable
-}
type LocalData a = ComputeNode LocLocal a


{-|
The dataframe type. Any dataset can be converted to a dataframe.

For the Spark users: this is different than the definition of the
dataframe in Spark, which is a dataset of rows. Because the support
for single columns is more akward in the case of rows, it is more
natural to generalize datasets to contain cells.
When communicating with Spark, though, single cells are wrapped
into rows with single field, as Spark does.
-}
type DataFrame = Try UntypedDataset

{-| Observable, whose type can only be infered at runtime and
that can fail to be computed at runtime.

Any observable can be converted to an untyped
observable.

Untyped observables are more flexible and can be combined in
arbitrary manner, but they will fail during the validation of
the Spark computation graph.

TODO(kps) rename to DynObservable
-}
type LocalFrame = Try UntypedLocalData

type UntypedNode' = Try UntypedNode

{-| The different paths of edges in the compute DAG of nodes, at the
start of computations.

 - scope edges specify the scope of a node for naming. They are not included in
   the id.

-}
data NodeEdge = ScopeEdge | DataStructureEdge StructureEdge deriving (Show, Eq)

{-| The edges in a compute DAG, after name resolution (which is where most of
the checks and computations are being done)

- parent edges are the direct parents of a node, the only ones required for
  defining computations. They are included in the id.
- logical edges define logical dependencies between nodes to force a specific
  ordering of the nodes. They are included in the id.
-}
data StructureEdge = ParentEdge | LogicalEdge deriving (Show, Eq)


class CheckedLocalityCast loc where
  _validLocalityValues :: [TypedLocality loc]

-- Class to retrieve the locality associated to a type.
-- Is it better to use type classes?
class (CheckedLocalityCast loc) => IsLocality loc where
  _getTypedLocality :: TypedLocality loc

instance CheckedLocalityCast LocLocal where
  _validLocalityValues = [TypedLocality Local]

instance CheckedLocalityCast LocDistributed where
  _validLocalityValues = [TypedLocality Distributed]

-- LocLocal is a locality associated to Local
instance IsLocality LocLocal where
  _getTypedLocality = TypedLocality Local

-- LocDistributed is a locality associated to Distributed
instance IsLocality LocDistributed where
  _getTypedLocality = TypedLocality Distributed

instance CheckedLocalityCast LocUnknown where
  _validLocalityValues = [TypedLocality Distributed, TypedLocality Local]
