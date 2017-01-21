
module Spark.IO.Inputs(
  SparkPath,
  JsonMode,
  DataSchema,
  JsonOptions,
  SourceDescription,
  json',
  json
) where


import Data.Text(Text)
import Data.Map(Map)
import Spark.Core.Types
import Spark.Core.Dataset(DataFrame, Dataset)
import Spark.Core.Context

{-| A path to some data that can be read by Spark.
-}
newtype SparkPath = SparkPath Text

{-|
-}
data JsonMode = Permissive | DropMalformed | FailFast

{-| The schema policty with respect to a data source. It should either
request Spark to infer the schema from the source, or it should try to
match the source against a schema provided by the user.
-}
data DataSchema = InferSchema | UseSchema DataType

{-| The options for the json input.
-}
data JsonOptions = JsonOptions {
  useSchema :: DataSchema,
  mode :: JsonMode
}

{-| The low-level option values accepted by the Spark reader API.
-}
data InputOptionValue =
    InputIntOption Int
  | InputDoubleOption Double
  | InputStringOption Text
  | InputStringBoolean Bool

newtype InputOptionKey = InputOptionKey { unInputOptionKey :: Text }

{-| The type of the source.
-}
data InputSource = JsonSource | TextSource | CsvSource | InputSource Text

{-| A description of a data source, following Spark's reader API version 2.

Eeach source constists in an input source (json, xml, etc.), an optional schema
for this source, and a number of options specific to this source.

Since this descriptions is rather low-level, a number of wrappers of provided
for each of the most popular sources that are already built into Spark.
-}
data SourceDescription = SourceDescription {
  inputPath :: SparkPath,
  inputSource :: InputSource,
  userSchema :: DataSchema,
  sdOptions :: Map InputOptionKey InputOptionValue
}

{-| Declares a source of data of the given data type.

The source is not read at this point, it is just declared. It may be found to be
invalid in subsequent computations.
-}
json' :: DataType -> String -> DataFrame
json' = undefined

{-| Declares a source of data of the given data type.

The source is not read at this point, it is just declared.
-}
json :: (SQLTypeable a) => String -> Dataset a
json = undefined

{-| Reads a source of data expected to be in the JSON format.

The schema is not required and Spark will infer the schema of the source.
However, all the data contained in the source may end up being read in the
process.
-}
jsonInfer :: SparkPath -> SparkState DataFrame
jsonInfer = undefined

{-| Reads a source of data expected to be in the JSON format.

The schema is not required and Spark will infer the schema of the source.
However, all the data contained in the source may end up being read in the
process.
-}
jsonOpt' :: JsonOptions -> SparkPath -> SparkState DataFrame
jsonOpt' = undefined

{-| Reads a source of data expected to be in the JSON format.

The schema is not required and Spark will infer the schema of the source.
However, all the data contained in the source may end up being read in the
process.
-}
jsonOpt :: (SQLTypeable a) => JsonOptions -> SparkPath -> SparkState (Dataset a)
jsonOpt = undefined

{-| Generates a dataframe from a source description.

This may trigger some calculations on the Spark side if schema inference is
required.
-}
generic' :: SourceDescription -> SparkState DataFrame
generic' = undefined

{-| Generates a dataframe from a source description, and assumes a given schema.

This schema overrides whatever may have been given in the source description. If
the source description specified that the schema must be checked or inferred,
this instruction is overriden.

While this is convenient, it may lead to runtime errors that are hard to
understand if the data does not follow the given schema.
-}
genericWithSchema' :: DataType -> SourceDescription -> DataFrame
genericWithSchema' = undefined

{-| Generates a dataframe from a source description, and assumes a certain
schema on the source.
-}
genericWithSchema :: (SQLTypeable a) => SourceDescription -> Dataset a
genericWithSchema = undefined
