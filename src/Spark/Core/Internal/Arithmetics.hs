{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

module Spark.Core.Internal.Arithmetics(
  GeneralizedHomoReturn,
  GeneralizedHomo2,
  HomoColOp2,
  -- | Developer API
  performOp
  ) where

import qualified Data.Text as T
import qualified Data.Vector as V
import Data.Maybe(fromMaybe)
import Formatting
import Data.Text(Text)
import Control.Monad(guard)

import Spark.Core.Try
import Spark.Core.StructuresInternal
import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.ColumnFunctions
import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.TypesGenerics(SQLTypeable, buildType)

{-| All the automatic conversions supported when lifting a -}
type family GeneralizedHomoReturn x1 x2 where
  GeneralizedHomoReturn (Column ref x1) (Column ref x1) = Column ref x1
  GeneralizedHomoReturn (Column ref x1) DynColumn = DynColumn
  GeneralizedHomoReturn (Column ref x1) (LocalData x1) = Column ref x1
  GeneralizedHomoReturn (Column ref x1) LocalFrame = DynColumn
  GeneralizedHomoReturn DynColumn (Column ref x1) = DynColumn
  GeneralizedHomoReturn DynColumn DynColumn = DynColumn
  GeneralizedHomoReturn DynColumn (LocalData x1) = DynColumn
  GeneralizedHomoReturn DynColumn LocalFrame = DynColumn
  GeneralizedHomoReturn (LocalData x1) (Column ref x1) = Column ref x1
  GeneralizedHomoReturn (LocalData x1) DynColumn = DynColumn
  GeneralizedHomoReturn (LocalData x1) (LocalData x1) = LocalData x1
  GeneralizedHomoReturn (LocalData x1) LocalFrame = LocalFrame

-- The type of an homogeneous operation.
-- TODO it would be nice to enforce this contstraint at the type level,
-- but it is a bit more complex to do.
type HomoColOp2 = UntypedColumnData -> UntypedColumnData -> UntypedColumnData

{-| The class of types that can be lifted to operations onto Kraps types.

This is the class for operations on homogeneous types (the inputs and the
output have the same underlying type).

At its core, it takes a broadcasted operation that works on columns, and
makes that operation available on other shapes.
-}
class GeneralizedHomo2 x1 x2 where
  _projectHomo :: x1 -> x2 -> HomoColOp2 -> GeneralizedHomoReturn x1 x2

{-| Performs an operation, using a reference operation defined on columns.
-}
performOp :: (GeneralizedHomo2 x1 x2) =>
  HomoColOp2 ->
  x1 ->
  x2 ->
  GeneralizedHomoReturn x1 x2
performOp f x1 x2 = _projectHomo x1 x2 f

-- ******* INSTANCES *********

instance GeneralizedHomo2 DynColumn DynColumn where
  _projectHomo = _performDynDynTp

_performDynDynTp ::
  DynColumn -> DynColumn -> HomoColOp2 -> DynColumn
_performDynDynTp dc1 dc2 f = do
  c1 <- dc1
  c2 <- dc2
  -- TODO: add type guard
  let c = f c1 c2
  -- TODO: add dynamic check on the type of the return
  return (dropColType c)


-- TODO: move this to a separate file
-- ******** IMPLEMENTATION CODE TO MOVE ********

{-| A generalization of the addition for the Kraps types.
-}
(.+) :: forall a1 a2. (Num a1, Num a2, GeneralizedHomo2 a1 a2) =>
  a1 -> a2 -> GeneralizedHomoReturn a1 a2
(.+) = performOp (homoColOp2 "sum")
