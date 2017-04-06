{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}


{-| A collection of small utility functions.
-}
module Spark.Core.Internal.Utilities(
  LB.HasCallStack,
  UnknownType,
  pretty,
  myGroupBy,
  myGroupBy',
  missing,
  failure,
  failure',
  forceRight,
  show',
  encodeDeterministicPretty,
  withContext,
  strictList,
  traceHint,
  SF.sh,
  (<&>),
  (<>)
  ) where

import Data.Aeson
import Data.Aeson.Encode.Pretty
import qualified Data.ByteString.Lazy.Char8 as Char8
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T
import qualified Formatting.ShortFormatters as SF
import Control.Arrow ((&&&))
import Data.List
import Data.Function
import Data.Text(Text)
import Formatting
import Debug.Trace(trace)
import qualified Data.Map.Strict as M
import Data.Monoid((<>))

import qualified Spark.Core.Internal.LocatedBase as LB

(<&>) :: Functor f => f a -> (a -> b) -> f b
(<&>) = flip fmap

-- | A type that is is not known and that is not meant to be exposed to the
-- user.
data UnknownType

{-| Pretty printing for Aeson values (and deterministic output)
-}
pretty :: Value -> Text
pretty = T.pack . Char8.unpack . encodeDeterministicPretty

-- | Produces a bytestring output of a JSON value that is deterministic
-- and that is invariant to the insertion order of the keys.
-- (i.e the keys are stored in alphabetic order)
-- This is to ensure that all id computations are stable and reproducible
-- on the server part.
-- TODO(kps) use everywhere JSON is converted
encodeDeterministicPretty :: Value -> LBS.ByteString
encodeDeterministicPretty =
  encodePretty' (defConfig { confIndent = Spaces 0, confCompare = compare })

-- | group by
-- TODO: have a non-empty list instead
myGroupBy' :: (Ord b) => (a -> b) -> [a] -> [(b, [a])]
myGroupBy' f = map (f . head &&& id)
                   . groupBy ((==) `on` f)
                   . sortBy (compare `on` f)

-- | group by
-- TODO: have a non-empty list instead
myGroupBy :: (Ord a) => [(a, b)] -> M.Map a [b]
myGroupBy l = let
  l2 = myGroupBy' fst l in
  M.map (snd <$>) $ M.fromList l2


-- | Missing implementations in the code base.
missing :: (LB.HasCallStack) => Text -> a
missing msg = LB.error $ T.concat ["MISSING IMPLEMENTATION: ", msg]

{-| The function that is used to trigger exception due to internal programming
errors.

Currently, all programming errors simply trigger an exception. All these
impure functions are tagged with an implicit call stack argument.
-}
failure :: (LB.HasCallStack) => Text -> a
failure msg = LB.error (T.concat ["FAILURE in Spark. Hint: ", msg])

failure' :: (LB.HasCallStack) => Format Text (a -> Text) -> a -> c
failure' x = failure . sformat x


{-| Given a DataFrame or a LocalFrame, attempts to get the value,
or throws the error.

This function is not total.
-}
forceRight :: (LB.HasCallStack, Show a) => Either a b -> b
forceRight (Right b) = b
forceRight (Left a) = LB.error $
  sformat ("Failure from either, got instead a left: "%shown) a

-- | Force the complete evaluation of a list to WNF.
strictList :: (Show a) => [a] -> [a]
strictList [] = []
strictList (h : t) = let !t' = strictList t in (h : t')

-- | (internal) prints a hint with a value
traceHint :: (Show a) => Text -> a -> a
traceHint hint x = trace (T.unpack hint ++ show x) x

-- | show with Text
show' :: (Show a) => a -> Text
show' x = T.pack (show x)

withContext :: Text -> Either Text a -> Either Text a
withContext _ (Right x) = Right x
withContext msg (Left other) = Left (msg <> "\n>>" <> other)
