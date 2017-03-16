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
  FromSQL(_cellToValue),
  TryS,
  cellToValue,
) where

import GHC.Generics
import Data.Text(Text, pack)
import Control.Applicative(liftA2)
import Control.Monad.Except
import Formatting
import qualified Data.Vector as V

import Spark.Core.Internal.RowStructures
import Spark.Core.Internal.Utilities
import Spark.Core.Internal.TypesStructuresRepr(DataTypeRepr, DataTypeElementRepr)

-- Convert a cell to a value (if possible)
cellToValue :: (FromSQL a) => Cell -> Either Text a
cellToValue = _cellToValue

type TryS = Either Text

-- Because of the way the generic decoders work,
-- an array of cell needs special treatment when it is
-- decoded as the constructor of an object. Then it should
-- be interpreted as a stateful tape, for which we read a
-- few cells (number unknown) and return some value from the
-- cells that have been consumed.
data Decode2 =
    -- A tape with some potentially remaining cells
    D2Cons ![Cell]
    -- Just a normal cell.
  | D2Normal !Cell
  deriving (Eq, Show)


-- All the types that can be converted to a SQL value.
class FromSQL a where
  _cellToValue :: Cell -> TryS a

  default _cellToValue :: (Generic a, GFromSQL (Rep a)) => Cell -> TryS a
  _cellToValue cell = let
      x = undefined :: a
      x1r = _gFcell (from x) (D2Normal cell) :: InterResult (Decode2, Rep a a)
      x2r = snd <$> x1r
      x1t = to <$> x2r
    in _toTry x1t

-- ******** Basic instance ********

instance FromSQL a => FromSQL (Maybe a) where
  _cellToValue Empty = pure Nothing
  _cellToValue x = pure <$> _cellToValue x

instance FromSQL Int where
  _cellToValue (IntElement x) = pure x
  _cellToValue x = throwError $ sformat ("FromSQL: Decoding an int from "%shown) x

instance FromSQL Double where
  _cellToValue (DoubleElement x) = pure x
  _cellToValue x = throwError $ sformat ("FromSQL: Decoding a double from "%shown) x

instance FromSQL Text where
  _cellToValue (StringElement txt) = pure txt
  _cellToValue x = throwError $ sformat ("FromSQL: Decoding a unicode text from "%shown) x

instance FromSQL Cell where
  _cellToValue = pure

instance FromSQL Bool where
  _cellToValue (BoolElement b) = pure b
  _cellToValue x = throwError $ sformat ("FromSQL: Decoding a boolean from "%shown) x

instance FromSQL DataTypeRepr
instance FromSQL DataTypeElementRepr

instance FromSQL a => FromSQL [a] where
  _cellToValue (RowArray xs) =
    sequence (_cellToValue <$> V.toList xs)
  _cellToValue x = throwError $ sformat ("FromSQL[]: Decoding array from "%shown) x

instance (FromSQL a1, FromSQL a2) => FromSQL (a1, a2) where
  _cellToValue (RowArray xs) = case V.toList xs of
    [x1, x2] ->
      liftA2 (,) (_cellToValue x1) (_cellToValue x2)
    l -> throwError $ sformat ("FromSQL: Expected 2 elements but got "%sh) l
  _cellToValue x = throwError $ sformat ("FromSQL(,): Decoding array from "%shown) x

-- ******* GENERIC ********

-- A final message at the bottom
-- A path in the elements to get there
data FailureInfo = FailureInfo !Text ![Text] deriving (Eq, Show)

type InterResult a = Either FailureInfo a


class GFromSQL r where
  -- An evidence about the type (in order to have info about the field names)
  -- The current stuff that has been decoded
  _gFcell :: r a -> Decode2 -> InterResult (Decode2, r a)

_toTry :: InterResult a -> TryS a
_toTry (Right x) = pure x
_toTry (Left (FailureInfo msg p)) = Left $ show' (reverse p) <> " : " <> msg

_fromTry :: TryS a -> InterResult a
_fromTry (Right x) = Right x
_fromTry (Left x) = Left $ FailureInfo x []

instance GFromSQL U1 where
  _gFcell x = failure $ pack $ "GFromSQL UI called" ++ show x

instance (GFromSQL a, GFromSQL b) => GFromSQL (a :*: b) where
  -- Switching to tape-reading mode
  _gFcell ev (D2Normal (RowArray arr)) = _gFcell ev (D2Cons (V.toList arr))
  -- Advancing into the reader
  _gFcell ev (D2Cons l) = do
    let (ev1 :*: ev2) = ev
    (d1, x1) <- _gFcell ev1 (D2Cons l)
    (d2, x2) <- _gFcell ev2 d1
    return (d2, x1 :*: x2)
  _gFcell _ x = failure $ pack ("GFromSQL (a :*: b) " ++ show x)


instance (GFromSQL a, GFromSQL b) => GFromSQL (a :+: b) where
  _gFcell _ x = failure $ pack $ "GFromSQL (a :+: b)" ++ show x

instance (GFromSQL a, Constructor c) => GFromSQL (M1 C c a) where
  _gFcell _ (D2Cons x) = failure $ pack ("GFromSQL (M1 C c a)" ++ " FAILED CONS: " ++ show x)
  _gFcell ev (D2Normal cell) = do
    let ev' = unM1 ev
    (d, x) <- _withHint (pack (conName ev)) $ _gFcell ev' (D2Normal cell)
    return (d, M1 x)

instance (GFromSQL a, Selector c) => GFromSQL (M1 S c a) where
  _gFcell ev (D2Normal (RowArray arr)) = do
    let ev' = unM1 ev
    let l = V.toList arr
    (d, x) <- _withHint ("(1)" <> pack (selName ev)) $ _gFcell ev' (D2Cons l)
    return (d, M1 x)
  _gFcell ev d = do
    let ev' = unM1 ev
    (d', x) <- _withHint ("(2)" <> pack (selName ev)) $ _gFcell ev' d
    return (d', M1 x)

instance (GFromSQL a, Datatype c) => GFromSQL (M1 D c a) where
  _gFcell ev (z @ (D2Normal (RowArray _))) = do
    let ev' = unM1 ev
    (d, x) <- _gFcell ev' z
    return (d, M1 x)
  _gFcell _ x = failure $ pack $ "FAIL GFromSQL (M1 D c a)" ++ show x

-- | Products: encode multiple arguments to constructors
instance (FromSQL a) => GFromSQL (K1 i a) where
  -- It is just a normal cell.
  -- Read one element and move on.
  _gFcell _ (D2Cons (cell : r)) = do
    x <- _fromTry $ _cellToValue cell
    return (D2Cons r, K1 x)
  -- Just reading a normal cell, return no tape.
  _gFcell _ (D2Normal cell) = do
    x <- _fromTry $ _cellToValue cell
    return (D2Cons [], K1 x)
  _gFcell _ x = failure $ pack ("GFromSQLK FAIL " ++ show x)

_withHint :: Text -> InterResult a -> InterResult a
_withHint extra (Left (FailureInfo msg l)) = Left (FailureInfo msg (extra : l))
_withHint _ (Right x) = Right x
