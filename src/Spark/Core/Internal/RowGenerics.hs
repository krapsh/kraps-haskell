{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MultiParamTypeClasses #-}


-- The generic implementation for the protocol that converts to
-- and from SQL cells.
-- Going through JSON is not recommended because of precision loss
-- for the numbers, and other issues related to numbers.
module Spark.Core.Internal.RowGenerics(
  ToSQL,
  valueToCell,
) where

import GHC.Generics
import qualified Data.Vector as V
import Data.Text(pack, Text)

import Spark.Core.Internal.RowStructures
import Spark.Core.Internal.Utilities

-- We need to differentiate between the list built for the
-- constructor and an inner object.
data CurrentBuffer =
  ConsData ![Cell]
  | BuiltCell !Cell deriving (Show)

_cellOrError :: CurrentBuffer -> Cell
_cellOrError (BuiltCell cell) = cell
_cellOrError x = let msg = "Expected built cell, received " ++ show x in
  failure (pack msg)

-- All the types that can be converted to a SQL value.
class ToSQL a where
  _valueToCell :: a -> Cell

  default _valueToCell :: (Generic a, GToSQL (Rep a)) => a -> Cell
  _valueToCell !x = _g2cell (from x)

valueToCell :: (ToSQL a) => a -> Cell
valueToCell = _valueToCell

-- class FromSQL a where
--   _cellToValue :: Cell -> Try a

instance ToSQL a => ToSQL (Maybe a) where
  _valueToCell (Just x) = _valueToCell x
  _valueToCell Nothing = Empty

instance (ToSQL a, ToSQL b) => ToSQL (a, b) where
  _valueToCell (x, y) = RowArray (V.fromList [valueToCell x, valueToCell y])

instance ToSQL Int where
  _valueToCell = IntElement

instance ToSQL Double where
  _valueToCell = DoubleElement

instance ToSQL Text where
  _valueToCell = StringElement


class GToSQL r where
  _g2buffer :: r a -> CurrentBuffer
  _g2cell :: r a -> Cell
  _g2cell = _cellOrError . _g2buffer

instance GToSQL U1 where
  _g2buffer U1 = failure $ pack "GToSQL UI called"

-- | Constants, additional parameters and recursion of kind *
instance (GToSQL a, GToSQL b) => GToSQL (a :*: b) where
  _g2buffer (a :*: b) = case (_g2buffer a, _g2buffer b) of
    (ConsData l1, ConsData l2) -> ConsData (l1 ++ l2)
    (y1, y2) -> failure $ pack $ "GToSQL (a :*: b): Expected buffers, received " ++ show y1 ++ " and " ++ show y2

instance (GToSQL a, GToSQL b) => GToSQL (a :+: b) where
  _g2buffer (L1 x) = _g2buffer x
  _g2buffer (R1 x) = let !y = _g2buffer x in y

-- -- | Sums: encode choice between constructors
-- instance (GToSQL a) => GToSQL (M1 i c a) where
--   _g2cell !(M1 x) = let !y = _g2cell x in
--     trace ("GToSQL M1: y = " ++ show y) y

instance (GToSQL a) => GToSQL (M1 C c a) where
  _g2buffer (M1 x) = let !y = _g2buffer x in y

instance (GToSQL a) => GToSQL (M1 S c a) where
  _g2buffer (M1 x) = let !y = ConsData [_g2cell x] in y

instance (GToSQL a) => GToSQL (M1 D c a) where
  _g2buffer (M1 x) =
    case _g2buffer x of
      ConsData cs -> BuiltCell $ RowArray (V.fromList cs)
      BuiltCell cell -> BuiltCell cell

-- | Products: encode multiple arguments to constructors
instance (ToSQL a) => GToSQL (K1 i a) where
  _g2buffer (K1 x) = let !y = _valueToCell x in BuiltCell y
