{-# OPTIONS_GHC -fno-warn-orphans #-}
-- Disabled for old versions
-- {-# OPTIONS_GHC -fno-warn-redundant-constraints #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
-- Required by old versions
{-# LANGUAGE FlexibleContexts #-}

{-| This module contains all the class instances and operators related
to arithmetics with Datasets, Dataframes, Columns and Observables.
-}
module Spark.Core.Internal.ArithmeticsImpl(
  (.+),
  (.-),
  (./),
  div'
  ) where

import Spark.Core.Internal.ColumnFunctions
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.LocalDataFunctions(constant)
import Spark.Core.Internal.FunctionsInternals(projectColFunction2', projectColFunction')
import Spark.Core.Internal.Arithmetics


{-| A generalization of the addition for the Karps types.
-}
(.+) :: forall a1 a2. (Num a1, Num a2, GeneralizedHomo2 a1 a2) =>
  a1 -> a2 -> GeneralizedHomoReturn a1 a2
(.+) = performOp (homoColOp2 "+")

{-| A generalization of the negation for the Karps types.
-}
(.-) :: forall a1 a2. (Num a1, Num a2, GeneralizedHomo2 a1 a2) =>
  a1 -> a2 -> GeneralizedHomoReturn a1 a2
(.-) = performOp (homoColOp2 "-")

(./) :: (Fractional a1, Fractional a2, GeneralizedHomo2 a1 a2) =>
  a1 -> a2 -> GeneralizedHomoReturn a1 a2
(./) = performOp (homoColOp2 "/")

div' :: forall a1 a2. (Num a1, Num a2, GeneralizedHomo2 a1 a2) =>
  a1 -> a2 -> GeneralizedHomoReturn a1 a2
div' = performOp (homoColOp2 "/")

-- All the operations are defined from column operations
-- This adds a little overhead, but it can be optimized by the backend.
instance Num LocalFrame where
  (+) = projectColFunction2' (+)
  (-) = projectColFunction2' (-)
  (*) = projectColFunction2' (*)
  abs = projectColFunction' abs
  signum = projectColFunction' signum
  -- It will choose by default to use the Int type, which may not be
  -- what the user wants.
  -- In case there is some doubt, user should use typed operations.
  fromInteger x = asLocalObservable $ constant (fromInteger x :: Int)
  negate = projectColFunction' negate
