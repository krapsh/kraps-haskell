{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

-- TODO(kps): this module stretches my understanding of Haskell.
-- There is probably better than that.

{-| Defines the notion of renaming something.

This is closed over a few well-defined types.
-}
module Spark.Core.Internal.CanRename where

import qualified Data.Text as T
import Formatting

import Spark.Core.Try
import Spark.Core.StructuresInternal
import Spark.Core.Internal.ColumnFunctions()
import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.Utilities

-- | The class of types that can be renamed.
-- It is made generic because it covers 2 notions:
--   - the name of a compute node that will eventually determine its compute path
--   - the name of field (which may become an object path)
-- This syntax tries to be convenient and will fail immediately
-- for basic errors such as illegal characters.
--
-- This could be revisited in the future, but it is a compromise
-- on readability.
class CanRename a txt where
  (@@) :: a -> txt -> a

infixl 1 @@


instance forall ref a. CanRename (ColumnData ref a) FieldName where
  c @@ fn = c { _cReferingPath = Just fn }


instance forall ref a s. (s ~ String) => CanRename (Column ref a) s where
  c @@ str = case fieldName (T.pack str) of
    Right fp -> c @@ fp
    Left msg ->
      -- The syntax check here is pretty lenient, so it fails, it has
      -- some good reasons. We stop here.
      failure $ sformat ("Could not make a field path out of string "%shown%" for column "%shown%":"%shown) str c msg

instance CanRename DynColumn FieldName where
  (Right cd) @@ fn = Right (cd @@ fn)
  -- TODO better error handling
  x @@ _ = x

instance forall s. (s ~ String) => CanRename DynColumn s where
  -- An error could happen when making a path out of a string.
  (Right cd) @@ str = case fieldName (T.pack str) of
    Right fp -> Right $ cd @@ fp
    Left msg ->
      -- The syntax check here is pretty lenient, so it fails, it has
      -- some good reasons. We stop here.
      tryError $ sformat ("Could not make a field path out of string "%shown%" for column "%shown%":"%shown) str cd msg
  -- TODO better error handling
  x @@ _ = x


instance forall loc a s. (s ~ String) => CanRename (ComputeNode loc a) s where
  -- There is no need to update the id, as this field is not involved
  -- in the calculation of the id.
  -- TODO: make this fail immediately? If the name is wrong, it is
  -- harder to figure out what is happening.
  (@@) cn name = cn { _cnName = Just nn } where
    nn = NodeName . T.pack $ name

instance forall loc a s. (s ~ String) => CanRename (Try (ComputeNode loc a)) s where
  (Right n) @@ str = Right (n @@ str)
  (Left n) @@ _ = Left n
