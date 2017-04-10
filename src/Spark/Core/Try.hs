
{-|
Useful classes and functions to deal with failures
within the Karps framework.

This is a developer API. Users should not have to invoke functions
from this module.
-}
module Spark.Core.Try(
  NodeError(..),
  Try,
  tryError,
  tryEither
  ) where

import qualified Data.Text as T
import qualified Data.Vector as V

import Spark.Core.StructuresInternal

-- | An error associated to a particular node (an observable or a dataset).
data NodeError = Error {
  ePath :: NodePath,
  eMessage :: T.Text
} deriving (Eq, Show)

-- | The common result of attempting to build something.
type Try = Either NodeError


-- TODO: rename to tryError
_error :: T.Text -> Try a
_error txt = Left Error {
    ePath = NodePath V.empty,
    eMessage = txt
  }

-- | Returns an error object given a text clue.
tryError :: T.Text -> Try a
tryError = _error

-- | Returns an error object given a string clue.
--Remove this method
--tryError' :: String -> Try a
--tryError' = _error . T.pack

-- | (internal)
-- Given a potentially errored object, converts it to a Try.
tryEither :: Either T.Text a -> Try a
tryEither (Left msg) = tryError msg
tryEither (Right x) = Right x
