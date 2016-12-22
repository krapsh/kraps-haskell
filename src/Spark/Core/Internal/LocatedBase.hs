-- Taken from https://hackage.haskell.org/package/located-base-0.1.1.0/docs/src/GHC-Err-Located.html

{-# LANGUAGE CPP #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE ImplicitParams #-}
{-# LANGUAGE OverloadedStrings #-}
module Spark.Core.Internal.LocatedBase (error, undefined, HasCallStack, showCallStack) where

#if __GLASGOW_HASKELL__ < 800

import GHC.SrcLoc
import GHC.Stack (CallStack, getCallStack)
import Prelude hiding (error, undefined)
import qualified Prelude
import Text.Printf
import Data.Text(Text, unpack)

type HasCallStack = (?callStack :: CallStack)

error :: HasCallStack => Text -> a
error msg = Prelude.error (unpack msg ++ "\n" ++ showCallStack ?callStack)

undefined :: HasCallStack => a
undefined = error "Prelude.undefined"

showCallStack :: CallStack -> String
showCallStack stk = case getCallStack stk of
  _:locs -> unlines $ "Callstack:" : map format locs
  _ -> Prelude.error "showCallStack: empty call-stack"
  where
  format (fn, loc) = printf "  %s, called at %s" fn (showSrcLoc loc)

#else

import GHC.Stack(HasCallStack, CallStack, prettyCallStack)
import qualified GHC.Stack()
import Data.Text(Text, unpack)
import qualified Prelude
import Prelude((.))

{-# DEPRECATED showCallStack "use GHC.Stack.prettyCallStack instead" #-}
showCallStack :: CallStack -> Prelude.String
showCallStack = prettyCallStack

error :: HasCallStack => Text -> a
error = Prelude.error . unpack

undefined :: HasCallStack => a
undefined = error "Prelude.undefined"

#endif
