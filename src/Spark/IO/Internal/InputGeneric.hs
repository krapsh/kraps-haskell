{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module Spark.IO.Internal.InputGeneric(
  SparkPath(..),
  DataSchema(..),
  InputOptionValue(..),
  InputOptionKey(..),
  InputSource(..),
  SourceDescription(..),
  generic',
  genericWithSchema',
  genericWithSchema
) where

import Data.Text(Text)
import qualified Data.Map.Strict as M
import qualified Data.Aeson as A
import Data.Aeson(toJSON, (.=))

import Spark.Core.Types
import Spark.Core.Context
import Spark.Core.Try
import Spark.Core.Dataset

import Spark.Core.Internal.Utilities(forceRight)
import Spark.Core.Internal.DatasetFunctions(asDF, emptyDataset, emptyLocalData)
import Spark.Core.Internal.TypesStructures(SQLType(..))
import Spark.Core.Internal.OpStructures

{-| A path to some data that can be read by Spark.
-}
newtype SparkPath = SparkPath Text deriving (Show, Eq)

{-| The schema policty with respect to a data source. It should either
request Spark to infer the schema from the source, or it should try to
match the source against a schema provided by the user.
-}
data DataSchema = InferSchema | UseSchema DataType deriving (Show, Eq)

{-| The low-level option values accepted by the Spark reader API.
-}
data InputOptionValue =
    InputIntOption Int
  | InputDoubleOption Double
  | InputStringOption Text
  | InputBooleanOption Bool

instance A.ToJSON InputOptionValue where
  toJSON (InputIntOption i) = toJSON i
  toJSON (InputDoubleOption d) = toJSON d
  toJSON (InputStringOption s) = toJSON s
  toJSON (InputBooleanOption b) = toJSON b

newtype InputOptionKey = InputOptionKey { unInputOptionKey :: Text } deriving (Eq, Show, Ord)

{-| The type of the source.
-}
data InputSource = JsonSource | TextSource | CsvSource | InputSource SparkPath

{-| A description of a data source, following Spark's reader API version 2.

Eeach source constists in an input source (json, xml, etc.), an optional schema
for this source, and a number of options specific to this source.

Since this descriptions is rather low-level, a number of wrappers of provided
for each of the most popular sources that are already built into Spark.
-}
data SourceDescription = SourceDescription {
  inputPath :: !SparkPath,
  inputSource :: !InputSource,
  userSchema :: !DataSchema,
  sdOptions :: !(M.Map InputOptionKey InputOptionValue)
}

{-| Generates a dataframe from a source description.

This may trigger some calculations on the Spark side if schema inference is
required.
-}
generic' :: SourceDescription -> SparkState DataFrame
generic' sd = do
  dtt <- _inferSchema sd
  return $ dtt >>= \dt -> genericWithSchema' dt sd

{-| Generates a dataframe from a source description, and assumes a given schema.

This schema overrides whatever may have been given in the source description. If
the source description specified that the schema must be checked or inferred,
this instruction is overriden.

While this is convenient, it may lead to runtime errors that are hard to
understand if the data does not follow the given schema.
-}
genericWithSchema' :: DataType -> SourceDescription -> DataFrame
genericWithSchema' dt sd = asDF $ emptyDataset no (SQLType dt) where
  sd' = sd { userSchema = UseSchema dt }
  so = StandardOperator {
      soName = "org.spark.GenericDatasource",
      soOutputType = dt,
      soExtra = A.toJSON sd'
    }
  no = NodeDistributedOp so

{-| Generates a dataframe from a source description, and assumes a certain
schema on the source.
-}
genericWithSchema :: forall a. (SQLTypeable a) => SourceDescription -> Dataset a
genericWithSchema sd =
  let sqlt = buildType :: SQLType a
      dt = unSQLType sqlt in
  forceRight $ castType sqlt =<< genericWithSchema' dt sd

-- Wraps the action of inferring the schema.
-- This is not particularly efficient here: it does a first pass to get the
-- schema, and then will do a second pass in order to read the data.
_inferSchema :: SourceDescription -> SparkState (Try DataType)
_inferSchema = executeCommand1 . _inferSchemaCmd

-- TODO: this is a monoidal operation, it could be turned into a universal
-- aggregator.
_inferSchemaCmd :: SourceDescription -> LocalData DataType
_inferSchemaCmd sd = emptyLocalData no sqlt where
  sqlt = buildType :: SQLType DataType
  dt = unSQLType sqlt
  so = StandardOperator {
      soName = "org.spark.InferSchema",
      soOutputType = dt,
      soExtra = A.toJSON sd
    }
  no = NodeOpaqueAggregator so

instance A.ToJSON SparkPath where
  toJSON (SparkPath p) = toJSON p

instance A.ToJSON DataSchema where
  toJSON InferSchema = "infer_schema"
  toJSON (UseSchema dt) = toJSON dt

instance A.ToJSON InputSource where
  toJSON JsonSource = "json"
  toJSON TextSource = "text"
  toJSON CsvSource = "csv"
  toJSON (InputSource s) = toJSON s

instance A.ToJSON SourceDescription where
  toJSON sd = A.object [
                "inputPath" .= toJSON (inputPath sd),
                "inputSource" .= toJSON (inputSource sd),
                "userSchema" .= toJSON (userSchema sd),
                "options" .= A.object (f <$> M.toList (sdOptions sd))
              ] where
                f (k, v) = unInputOptionKey k .= toJSON v