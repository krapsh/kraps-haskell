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

-- TODO remove this file
module Spark.Core.Internal.AlgebraStructures where

-- | Algebraic structures that are common to columns and observables.


data BinaryOpFun in1 in2 to = BinaryOpFun {
  bodLift1 :: in1 -> to,
  bodLift2 :: in2 -> to,
  bodOp :: to -> to -> to
}


class HomoBinaryOp2 in1 in2 to | in1 in2 -> to where
  _liftFun :: (to -> to -> to) -> BinaryOpFun in1 in2 to

_applyBinOp0 :: forall in1 in2 to. in1 -> in2 -> BinaryOpFun in1 in2 to -> to
_applyBinOp0 i1 i2 (BinaryOpFun l1 l2 bo) = bo (l1 i1) (l2 i2)

applyBinOp :: forall in1 in2 to. (HomoBinaryOp2 in1 in2 to) => (to -> to -> to) -> in1 -> in2 -> to
applyBinOp f i1 i2 =
  _applyBinOp0 i1 i2 (_liftFun f)

-- -- | Overloaded operator for operationts that are guaranteed to succeed.
-- (.+) :: (Num out, HomoBinaryOp2 a1 a2 out) => a1 -> a2 -> out
-- (.+) = applyBinOp (+)

(.-) :: (Num out, HomoBinaryOp2 a1 a2 out) => a1 -> a2 -> out
(.-) = applyBinOp (-)

(.*) :: (Num out, HomoBinaryOp2 a1 a2 out) => a1 -> a2 -> out
(.*) = applyBinOp (*)

-- TODO(kps) add here the rest of the Integral operations
div' :: (Integral out, HomoBinaryOp2 a1 a2 out) => a1 -> a2 -> out
div' = applyBinOp div

-- **** Fractional ****

(./) :: (Fractional out, HomoBinaryOp2 a1 a2 out) => a1 -> a2 -> out
(./) = applyBinOp (/)
