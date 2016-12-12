{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}


-- The generic implementation for the protocol that converts to
-- and from SQL cells.
-- Going through JSON is not recommended because of precision loss
-- for the numbers, and other issues related to numbers.
module Spark.Core.Internal.RowGenericsFrom(
  FromSQL,
  cellToValue,
) where

import GHC.Generics
import Data.Text(Text, pack)
import Control.Monad.Except
import Formatting
import qualified Data.Vector as V

import Spark.Core.Internal.RowStructures
import Spark.Core.Internal.Utilities

-- Convert a cell to a value (if possible)
cellToValue :: (FromSQL a) => Cell -> Either Text a
cellToValue = _cellToValue

type TryS = Either Text

-- Switch between the constructor list or parsing a regular cell.
data Decode2 =
    D2Cons ![Cell]
  | D2Normal !Cell
  deriving (Eq, Show)

-- All the types that can be converted to a SQL value.
class FromSQL a where
  _cellToValue :: Cell -> TryS a

  default _cellToValue :: (Generic a, GFromSQL (Rep a)) => Cell -> TryS a
  _cellToValue cell = let
      x1r = _gFcell (D2Normal cell) :: TryS (Rep a a)
      x1t = to <$> x1r
    in x1t


instance FromSQL a => FromSQL (Maybe a) where
  _cellToValue Empty = pure Nothing
  _cellToValue x = pure <$> _cellToValue x

instance FromSQL Int where
  _cellToValue (IntElement x) = pure x
  _cellToValue x = throwError $ sformat ("FromSQL: Decoding an int from "%shown) x

instance FromSQL Cell where
  _cellToValue = pure

instance FromSQL a => FromSQL [a] where
  _cellToValue (RowArray xs) = sequence (_cellToValue <$> V.toList xs)
  _cellToValue x = throwError $ sformat ("FromSQL: Decoding array from "%shown) x
-- ******* GENERIC ********

class GFromSQL r where
  _gFcell :: Decode2 -> TryS (r a)

instance GFromSQL U1 where
  _gFcell x = failure $ pack $ "GFromSQL UI called" ++ show x

instance (GFromSQL a, GFromSQL b) => GFromSQL (a :*: b) where
  _gFcell (D2Normal (RowArray arr)) | not (V.null arr) =
    let (cell : l) = V.toList arr
        x1t = _gFcell (D2Normal cell)
        x2t = _gFcell (D2Cons l)
        x = do
          x1 <- x1t
          x2 <- x2t
          return (x1 :*: x2)
    in x
  _gFcell (D2Cons (cell : l)) =
    let x1t = _gFcell (D2Cons [cell])
        x2t = _gFcell (D2Cons l)
        x = do
          x1 <- x1t
          x2 <- x2t
          return (x1 :*: x2)
    in x
  _gFcell x = failure $ pack ("GFromSQL (a :*: b) " ++ show x)


instance (GFromSQL a, GFromSQL b) => GFromSQL (a :+: b) where
  _gFcell x = failure $ pack $ "GFromSQL (a :+: b)" ++ show x

_m1 :: GFromSQL f1 => String -> Decode2 -> TryS (M1 i1 c1 f1 p1)
_m1 msg (D2Cons x) = failure $ pack (msg ++ " FAILED CONS: " ++ show x)
_m1 _ (D2Normal cell) =
    let xt = _gFcell (D2Normal cell) in
      M1 <$> xt

instance (GFromSQL a, Constructor c) => GFromSQL (M1 C c a) where
  _gFcell = _m1 "GFromSQL (M1 C c a)"

instance (GFromSQL a, Selector c) => GFromSQL (M1 S c a) where
  _gFcell (D2Normal (RowArray arr)) | V.length arr == 1 =
    M1 <$> _gFcell (D2Cons [cell]) where
      cell = V.head arr
  _gFcell z @ (D2Cons [_]) =
    M1 <$> _gFcell z
  _gFcell x = _m1 "GFromSQL (M1 S c a)" x

instance (GFromSQL a, Datatype c) => GFromSQL (M1 D c a) where
  _gFcell z @ (D2Normal (RowArray _)) =
    let xt = _gFcell z in
      M1 <$> xt
  _gFcell x = failure $ pack $ "FAIL GFromSQL (M1 D c a)" ++ show x

-- | Products: encode multiple arguments to constructors
instance (FromSQL a) => GFromSQL (K1 i a) where
  _gFcell (D2Normal cell) =
    let xt = _cellToValue cell :: TryS a in
      K1 <$> xt
  _gFcell (D2Cons [cell]) =
    let xt = _cellToValue cell in
      K1 <$> xt
  _gFcell x = failure $ pack ("GFromSQLK FAIL " ++ show x)
