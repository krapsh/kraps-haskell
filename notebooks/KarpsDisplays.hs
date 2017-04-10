{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

-- This module is meant to be loaded from the IHaskell REPL.
module KarpsDisplays where

import IHaskell.Display
import Formatting
import Data.Text(unpack)
import qualified Data.Text
import Data.Foldable(toList)

import KarpsDagDisplay(nodeToDisplayGraph, tfIFrame, DisplayGraph, exportNodes)

import Spark.Core.Internal.Utilities(forceRight)
import Spark.Core.Internal.PathsUntyped
import Spark.Core.Internal.ComputeDag
import Spark.Core.Internal.DAGStructures
import Spark.Core.Internal.DatasetFunctions(untyped)
import Spark.Core.Internal.DatasetStructures(ComputeNode)
import Spark.Core.Internal.Utilities(forceRight)
import Spark.Core.Internal.Client
import Spark.Core.StructuresInternal(ComputationID(..))
import Spark.Core.Try

instance IHaskellDisplay DisplayGraph where
  display g = return $ Display [html code]
    where
      code = unpack . tfIFrame . exportNodes $ g


showGraph = display . forceRight . nodeToDisplayGraph

showGraph' = showGraph . forceRight

showNameGraph node = showGraph . vertexData . last . forceRight $ x where
  x = do
    cg <- tryEither $ buildCGraph (untyped node)
    nameCg <- assignPathsUntyped cg
    let tiedCg = tieNodes nameCg
    return $ toList (cdVertices tiedCg)
