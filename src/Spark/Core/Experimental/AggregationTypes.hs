
module Spark.Core.Experimental.AggregationTypes where

import Spark.Core.Dataset
import Spark.Core.Internal.Utilities
import Spark.Core.Types

type Observable a = LocalData a

-- Some thoughts about the aggregation interface.

-- Any aggregator: nothing special is expected from this aggregator.
-- This aggregator will not be supported in groupBy, because it may
-- depend on the partitioning of the data.
-- In the case of the typed API, this verification is only done during
-- the analysis.
type OpaqueAggregatorDesc a out = Dataset a -> Observable out
type UntypedOpaqueAggregator = OpaqueAggregatorDesc UnknownType UnknownType

-- This aggregator is a bit better: it is invariant to partitioning.
-- This is the basic requirement that makes it usable in groupBy.
-- TODO see if this is of any use, because most invariant aggregators
-- are actually universal aggregators.
type PInvariantAggregatorDesc a out = Dataset a -> Observable out

type UntypedPInvariantAggregator = PInvariantAggregatorDesc UnknownType UnknownType

-- This is the universal aggregator: the invariant aggregator and
-- some extra laws to combine multiple outputs.
-- It is useful for combining the results over multiple passes.
-- A real implementation in Spark has also an inner pass.
-- TODO: should it be Column or Dataset?
-- TODO: Column of course, it is typed
data UniversalAggregatorDesc a buff out = UniversalAggregatorDesc {
  -- The result is partioning invariant
  uaInitialOuter :: Dataset a -> Observable buff,
  -- This operation is associative and commutative
  -- The logical parents of the final observable have to be the 2 inputs
  uaMergeBuffer :: Observable buff -> Observable buff -> Observable buff,
  -- Nothing special on this one
  -- The logical parent of the final observable has to be the initial input.
  uaOutput :: Observable buff -> Observable out
}

type UntypedUniversalAggregator =
  UniversalAggregatorDesc UnknownType UnknownType UnknownType

-- The following laws have to be observed:
-- Distribution over the concatenation
-- forall ds1, ds2: Dataset in, saInitialOuter (concat ds1 ds2) = saMergeBuffer (saInitialOuter ds1) (saInitialOuter ds2)


-- The stateful aggregator.
-- This aggregator considers the case of streams of datasets, where there is an ordering of
-- the observables being generated.
data StatefulAggregatorDesc a ibuff obuff out = StatefulAggregatorDesc {
  saOutput :: Observable obuff -> Observable out,
  -- TODO should it start from a first observation? Not necessary I think.
  saInitialOuter :: Observable obuff,
  -- TODO Should it be Dataset?
  -- Takes the current outer state and passes it to the current dataset.
  saAggregateInner :: Observable obuff -> Dataset a -> Observable ibuff,
  saMergeOuter :: Observable obuff -> Observable ibuff -> Observable obuff
}

data AggregatorType =
    OpaqueAggregator UntypedOpaqueAggregator
  | PInvariantAggregator UntypedPInvariantAggregator
  | UniversalAggregator UntypedUniversalAggregator


-- invariants:
-- mergeOuter x (mergeOuter initialOuter y) == mergeOuter x y
-- mergeOuter (mergeOuter o (aggregateInner c1)) (aggregateInner c2)  == mergeOuter o (aggregateInner (union c1 c2))



-- NormalAggregator in out = GeneralizedAggregator in out out out
-- and also flipping on mergeOuter

-- mergeCol :: Col c1 -> Col c2 -> Col (c1, c2)

-- mergeAgg :: Agg in1 out1 -> Agg in2 out2 -> Agg (in1, in2) (out1, out2)
