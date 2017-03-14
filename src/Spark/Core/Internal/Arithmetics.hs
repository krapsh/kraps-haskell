{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RankNTypes #-}

module Spark.Core.Internal.Arithmetics where

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

type HomoColOp2 x = (forall ref. Column ref x -> Column ref x -> Column ref x)

{-| The class of types that can be lifted to operations onto Kraps types.

This is the class for operations on homogeneous types (the inputs and the
output have the same underlying type).

At its core, it takes a broadcasted operation that works on columns, and
makes that operation available on other shapes.
-}
class GeneralizedHomo2 x1 x2 x where
  _projectHomo :: x1 -> x2 -> HomoColOp2 x -> GeneralizedHomoReturn x1 x2

{-| Performs an operation, using a reference operation defined on columns.
-}
performOp :: (GeneralizedHomo2 x1 x2 x) =>
  (forall ref. Column ref x -> Column ref x -> Column ref x) ->
  x1 ->
  x2 ->
  GeneralizedHomoReturn x1 x2
performOp f x1 x2 = _projectHomo x1 x2 f

-- ******* INSTANCES *********

instance (SQLTypeable x) => GeneralizedHomo2 DynColumn DynColumn x where
  _projectHomo = _performDynDynTp

_performDynDynTp :: forall x. (SQLTypeable x) =>
  DynColumn -> DynColumn -> HomoColOp2 x -> DynColumn
_performDynDynTp dc1 dc2 f = do
  c1 <- dc1
  c2 <- dc2
  let sqlt = buildType :: SQLType x
  let dt = unSQLType sqlt
  guard (unSQLType (colType c1) /= dt) $
    tryError $ "_performDynDynTp c1" -- TODO
  guard (unSQLType (colType c2) /= dt) $
    tryError $ "_performDynDynTp c2" -- TODO
  c1' <- castTypeCol sqlt c1
  c2' <- castTypeCol sqlt c2
  let c = f c1' c2'
  return c
