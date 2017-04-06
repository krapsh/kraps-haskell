
module Spark.Core.Internal.ObservableStandard(
  asDouble) where

import Spark.Core.Internal.ColumnStandard
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.FunctionsInternals
import Spark.Core.Internal.TypesGenerics(SQLTypeable)

{-| Casts a local data as a double.
-}
asDouble :: (Num a, SQLTypeable a) => LocalData a -> LocalData Double
asDouble = projectColFunction asDoubleCol
