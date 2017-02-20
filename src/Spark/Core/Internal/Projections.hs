{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RankNTypes #-}
-- {-# LANGUAGE UndecidableInstances #-}

-- TODO(kps): this module stretches my understanding of Haskell.
-- There is probably better than that.

{-| Defines some projections operations over dataframes, observables, and
columns. This allows users for a fairly natural manipulation of
data.

-}
module Spark.Core.Internal.Projections where

import qualified Data.Text as T
import Data.Maybe(fromMaybe)
import Formatting
import Data.Text(Text)

import Spark.Core.Try
import Spark.Core.StructuresInternal
import Spark.Core.Internal.TypesStructures
import Spark.Core.Internal.ColumnFunctions
import Spark.Core.Internal.ColumnStructures
import Spark.Core.Internal.DatasetFunctions
import Spark.Core.Internal.DatasetStructures
import Spark.Core.Internal.Utilities


{-| The class of static projections that are guaranteed to succeed
by using the type system.

from is the type of the dataset (which is also a typed dataset)
to is the type of the final column.
-}
data StaticColProjection from to = StaticColProjection {
  _staticProj :: (SQLType from, FieldPath, SQLType to)
}

{-| The class of projections that require some runtime introspection
to confirm that the projection is valid.
-}
data DynamicColProjection = DynamicColProjection {
  -- The start type is irrelevant.
  _dynProjTry :: DataType -> Try (FieldPath, DataType)
}

{-| The operation of extraction from a Spark object to another
object.
-}
class Projection from proj to | from proj -> to where
  _performProjection :: from -> proj -> to

{-| The projector operation.

This is the general projection operation in Spark. It lets you extract columns
from datasets or dataframes, or sub-observables from observables.

TODO(kps) put an example here.
-}
(//) :: forall from proj. Project from proj => from -> proj -> ProjectReturn from proj
(//) = _performProject
-- (//) :: forall from proj to. Projection from proj to => from -> proj -> to
-- (//) = _performProjection

{-| The projector operation for string.

This is the general projection operation in Spark. It lets you extract columns
from datasets or dataframes, or sub-observables from observables.

Because of a Haskell limitation, this operator is different for strings.

TODO(kps) put an example here.
-}
(/-) :: forall from. Project from Text => from -> Text -> ProjectReturn from Text
(/-) = _performProject


type family ProjectReturn from proj where
  ProjectReturn DataFrame DynamicColProjection = DynColumn
  ProjectReturn DataFrame (StaticColProjection from to) = DynColumn
  ProjectReturn DataFrame Text = DynColumn
  ProjectReturn DynColumn DynamicColProjection = DynColumn
  ProjectReturn DynColumn Text = DynColumn
  ProjectReturn (Dataset x) DynamicColProjection = DynColumn
  ProjectReturn (Dataset x) (StaticColProjection x y) = Column x y
  ProjectReturn (Dataset x) Text = DynColumn


class MyString x where
  convertToText :: x -> Text

instance (a ~ Text) => MyString a where
  convertToText = id

class Project from proj where
  _performProject :: from -> proj -> ProjectReturn from proj

instance Project DynColumn DynamicColProjection where
  _performProject = projectDColDCol

instance Project DataFrame DynamicColProjection where
  _performProject = projectDFDyn

instance forall a b. Project DataFrame (StaticColProjection a b) where
  _performProject df proj = projectDFDyn df (colStaticProjToDynProj proj)

instance forall a b. Project (Dataset a) (StaticColProjection a b) where
  _performProject = projectDsCol

instance forall a. Project (Dataset a) DynamicColProjection where
  _performProject = projectDSDyn

instance Project DynColumn Text where
  _performProject dc s =
    let s' = T.unpack $ convertToText s
    in _performProjection dc (stringToDynColProj s')

instance Project DataFrame Text where
  _performProject df s =
    let s' = T.unpack $ convertToText s
    in projectDFDyn df (stringToDynColProj s')

instance Project (Dataset a) Text where
  _performProject ds s =
    let s' = T.unpack $ convertToText s
    in projectDSDyn ds (stringToDynColProj s')

-- data Foo
-- data Bar
--
-- test =
--   let dyn1 = undefined :: DynColumn
--       pdyn1 = undefined :: DynamicColProjection
--       p = undefined :: StaticColProjection Foo Bar
--       ds1 = undefined :: Dataset Foo
--       foo = undefined :: Foo
--       df1 = undefined :: DataFrame
--       dyn2 = dyn1 // pdyn1
--       dyn3 = dyn1/-"ab"/-"cd"
--       dyn4 = dyn1 // pdyn1 // pdyn1
--       cdyn1 = df1/-"ab"//pdyn1
--       ds2 = ds1 // p
--       -- dyn4 = dyn1 /// foo
--   in ds2

-- instance Project

-- dataset -> static projection -> column
instance forall a to. Projection (Dataset a) (StaticColProjection a to) (Column a to) where
  _performProjection = projectDsCol

-- dataset -> dynamic projection -> DynColumn
instance forall a. Projection (Dataset a) DynamicColProjection DynColumn where
  _performProjection = projectDSDyn

-- dataset -> string -> DynColumn
instance forall a . Projection (Dataset a) String DynColumn where
  _performProjection ds s = projectDSDyn ds (stringToDynColProj s)

-- dataframe -> dynamic projection -> dyncolumn
instance Projection DataFrame DynamicColProjection DynColumn where
  _performProjection = projectDFDyn

-- dataframe -> static projection -> dyncolumn
-- This is a relaxation as we could return Try (Column to) intead.
-- It makes more sense from an API perspective to just return a dynamic result.
instance forall a to. Projection DataFrame (StaticColProjection a to) DynColumn where
  _performProjection df proj = projectDFDyn df (colStaticProjToDynProj proj)

-- dataframe -> string -> dyncolumn
instance Projection DataFrame String DynColumn where
  _performProjection df s = projectDFDyn df (stringToDynColProj s)

-- column -> static projection -> column
instance forall ref a to. Projection (Column ref a) (StaticColProjection a to) (Column ref to) where
  _performProjection = projectColCol


-- dyncolumn -> dynamic projection -> dyncolumn
instance Projection DynColumn DynamicColProjection DynColumn where
  _performProjection = projectDColDCol

instance forall a to. Projection DynColumn (StaticColProjection a to) DynColumn where
  _performProjection dc proj = projectDColDCol dc (colStaticProjToDynProj proj)

-- dyncolumn -> string -> dyncolumn
instance Projection DynColumn String DynColumn where
  _performProjection dc s = _performProjection dc (stringToDynColProj s)





{-| Lets the users define their own static projections.

Throws an error if the type cannot be found, so should be used with caution.

String has to be used because of type inferrence issues
-}
unsafeStaticProjection :: forall from to. (HasCallStack) =>
  SQLType from     -- ^ The start type
  -> String        -- ^ The name of a field assumed to be found in the start type.
                   --   This only has to be valid for Spark purposes, not
                   --   internal Haskell representation.
  -> StaticColProjection from to
unsafeStaticProjection sqlt field =
  let
    f = forceRight . fieldPath . T.pack $ field
    sqlt' = fromMaybe
      (failure $ sformat ("unsafeStaticProjection: Cannot find the field "%sh%" in type "%sh) field sqlt)
      (extractPathUnsafe sqlt f)
  in StaticColProjection (sqlt, f, sqlt')


-- Returns a projection from a path (even if invalid data)
-- TODO: what is the difference with the function below??
dynamicProjection :: String -> DynamicColProjection
dynamicProjection txt = case fieldPath (T.pack txt) of
  Left msg -> DynamicColProjection $ \_ ->
    tryError $ sformat ("dynamicProjection: invalid syntax for path "%shown%": "%shown) txt msg
  Right fpath -> pathToDynColProj fpath

{-| Given a string that contains a name or a path, builds a dynamic column
projection.
-}
stringToDynColProj :: String -> DynamicColProjection
stringToDynColProj s =
  let
    fun dt =
      case fieldPath (T.pack s) of
        Right fp -> _dynProjTry (pathToDynColProj fp) dt
        Left msg -> tryError (T.pack msg)
  in DynamicColProjection fun

pathToDynColProj :: FieldPath -> DynamicColProjection
pathToDynColProj fpath =
  let
    fun dt = case extractPathUnsafe (SQLType dt) fpath of
        Just (SQLType dt') -> pure (fpath, dt') -- TODO(kps) I have a doubt
        Nothing ->
          tryError $ sformat ("unsafeStaticProjection: Cannot find the field "%shown%" in type "%shown) fpath dt
   in DynamicColProjection fun


-- | Converts a static project to a dynamic projector.
colStaticProjToDynProj :: forall from to. StaticColProjection from to -> DynamicColProjection
colStaticProjToDynProj (StaticColProjection (SQLType dtFrom, fp, SQLType dtTo)) =
  DynamicColProjection $ \dt ->
    -- TODO factorize this as a projection on types.
    if dt /= dtFrom then
      tryError $ sformat ("Cannot convert type "%shown%" into type "%shown) dt dtFrom
    else pure (fp, dtTo)

-- ****** Functions that perform projections *******

-- TODO: take a compute node instead
projectDSDyn :: Dataset from -> DynamicColProjection -> DynColumn
projectDSDyn ds proj = do
 (p, dt) <- _dynProjTry proj (unSQLType . nodeType $ ds)
 colExtraction ds dt p

projectDFDyn :: DataFrame -> DynamicColProjection -> DynColumn
projectDFDyn df proj = do
 node <- df
 projectDSDyn node proj

projectDsCol :: Dataset from -> StaticColProjection from to -> Column from to
projectDsCol ds proj = let (_, p, sqlt) = _staticProj proj in
 iEmptyCol ds sqlt p

projectColCol :: Column ref from -> StaticColProjection from to -> Column ref to
projectColCol c (StaticColProjection (_, fp, SQLType dt)) =
 unsafeProjectCol c fp dt


projectColDynCol :: ColumnData ref a -> DynamicColProjection -> DynColumn
projectColDynCol cd proj =
 _dynProjTry proj (_cType cd) <&> uncurry (unsafeProjectCol . dropColReference $ cd)

projectDColDCol :: DynColumn -> DynamicColProjection -> DynColumn
projectDColDCol c proj = do
 cd <- c
 projectColDynCol cd proj
