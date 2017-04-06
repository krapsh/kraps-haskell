module Spark.Core.Internal.RowStructures where

import Data.Aeson
import Data.Vector(Vector)
import qualified Data.Text as T

-- | The basic representation of one row of data. This is a standard type that comes out of the
-- SQL engine in Spark.

-- | An element in a Row object.
-- All objects manipulated by the Spark framework are assumed to
-- be convertible to cells.
--
-- This is usually handled by generic transforms.
data Cell =
    Empty -- To represent maybe
    | IntElement !Int
    | DoubleElement !Double
    | StringElement !T.Text
    | BoolElement !Bool
    | RowArray !(Vector Cell) deriving (Show, Eq)

-- | A Row of data: the basic data structure to transport information
-- TODO rename to rowCells
data Row = Row {
    cells :: !(Vector Cell)
  } deriving (Show, Eq)


-- AESON INSTANCES

-- TODO(kps) add some workaround to account for the restriction of
-- JSON types:
-- int32 -> int32
-- double -> double
-- weird double -> string?
-- long/bigint -> string?

-- | Cell
instance ToJSON Cell where
  toJSON Empty = Null
  toJSON (DoubleElement d) = toJSON d
  toJSON (IntElement i) = toJSON i
  toJSON (BoolElement b) = toJSON b
  toJSON (StringElement s) = toJSON s
  toJSON (RowArray arr) = toJSON arr

-- | Row
instance ToJSON Row where
  toJSON (Row x) = toJSON x
