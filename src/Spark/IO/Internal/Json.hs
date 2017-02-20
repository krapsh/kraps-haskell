{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Spark.IO.Internal.Json(
  JsonMode,
  JsonOptions(..),
  json',
  json,
  jsonInfer,
  jsonOpt',
  jsonOpt,
  defaultJsonOptions
) where

import qualified Data.Map.Strict as M
import Data.Text(pack)


import Spark.Core.Types
import Spark.Core.Dataset(DataFrame, Dataset, castType')
import Spark.Core.Context
import Spark.Core.Try

import Spark.IO.Internal.InputGeneric

{-|
-}
data JsonMode = Permissive | DropMalformed | FailFast

{-| The options for the json input.
-}
data JsonOptions = JsonOptions {
  mode :: !JsonMode,
  jsonSchema :: !DataSchema
}


{-| Declares a source of data of the given data type.

The source is not read at this point, it is just declared. It may be found to be
invalid in subsequent computations.
-}
json' :: DataType -> String -> DataFrame
json' dt p = genericWithSchema' dt (_jsonSourceDescription (SparkPath (pack p)) defaultJsonOptions)

{-| Declares a source of data of the given data type.

The source is not read at this point, it is just declared.
-}
json :: (SQLTypeable a) => String -> Dataset a
json p = genericWithSchema (_jsonSourceDescription (SparkPath (pack p)) defaultJsonOptions)

{-| Reads a source of data expected to be in the JSON format.

The schema is not required and Spark will infer the schema of the source.
However, all the data contained in the source may end up being read in the
process.
-}
jsonInfer :: SparkPath -> SparkState DataFrame
jsonInfer = jsonOpt' defaultJsonOptions

{-| Reads a source of data expected to be in the JSON format.

The schema is not required and Spark will infer the schema of the source.
However, all the data contained in the source may end up being read in the
process.
-}
jsonOpt' :: JsonOptions -> SparkPath -> SparkState DataFrame
jsonOpt' jo sp = generic' (_jsonSourceDescription sp jo)

{-| Reads a source of data expected to be in the JSON format.

The schema is not required and Spark will infer the schema of the source.
However, all the data contained in the source may end up being read in the
process.
-}
jsonOpt :: forall a. (SQLTypeable a) => JsonOptions -> SparkPath -> SparkState (Try (Dataset a))
jsonOpt jo sp =
  let sqlt = buildType :: SQLType a
      dt = unSQLType sqlt
      jo' = jo { jsonSchema = UseSchema dt }
  in castType' sqlt <$> jsonOpt' jo' sp

defaultJsonOptions :: JsonOptions
defaultJsonOptions = JsonOptions {
  -- Fail fast by default, to be conservative about errors,
  -- and respect the strictness arguments.
  mode = FailFast,
  jsonSchema = InferSchema
}

_jsonSourceDescription :: SparkPath -> JsonOptions -> SourceDescription
_jsonSourceDescription sp jo = SourceDescription {
  inputSource = JsonFormat,
  inputPath = sp,
  inputSchema = jsonSchema jo,
  sdOptions = _jsonOptions jo,
  inputStamp = Nothing
}

_jsonOptions :: JsonOptions -> M.Map InputOptionKey InputOptionValue
_jsonOptions jo = M.fromList [(InputOptionKey "mode", _mode (mode jo))]

_mode :: JsonMode -> InputOptionValue
_mode Permissive = InputStringOption "PERMISSIVE"
_mode DropMalformed = InputStringOption "DROPMALFORMED"
_mode FailFast = InputStringOption "FAILFAST"
