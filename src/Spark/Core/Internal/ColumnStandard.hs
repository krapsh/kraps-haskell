{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

{-| The standard library of functions operating on columns only.
-}
module Spark.Core.Internal.ColumnStandard(
  asDoubleCol
) where


import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.ColumnFunctions
import Spark.Core.Internal.TypesGenerics(buildType)

asDoubleCol :: (Num a) => Column ref a -> Column ref Double
asDoubleCol = makeColOp1 "double" buildType
