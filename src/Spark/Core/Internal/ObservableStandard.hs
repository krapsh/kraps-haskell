
module Spark.Core.Internal.ObservableStandard(
  asDouble) where

import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.ColumnFunctions
import Spark.Core.Internal.ColumnStandard
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.TypesFunctions
import Spark.Core.Internal.LocalDataFunctions
import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.FunctionsInternals
import Spark.Core.Internal.Projections
import Spark.Core.Internal.OpStructures
import Spark.Core.Internal.TypesGenerics(SQLTypeable, buildType)
import Spark.Core.StructuresInternal
import Spark.Core.Try


asDouble :: (Num a, SQLTypeable a) => LocalData a -> LocalData Double
asDouble = projectColFunction asDoubleCol
