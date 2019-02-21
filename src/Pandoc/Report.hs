{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE ScopedTypeVariables  #-}
module Pandoc.Report where

import           Control.Monad.Morph        (generalize, hoist, lift)
import           Control.Monad.Trans        (lift)
import qualified Data.Aeson.Encode.Pretty   as A
import qualified Data.ByteString.Lazy.Char8 as BS
import           Data.Monoid                ((<>))
import qualified Data.Text                  as T
import qualified Data.Text.Encoding         as T
import qualified Data.Text.Lazy             as LT
import qualified Graphics.Vega.VegaLite     as GV
import qualified Lucid                      as H
import qualified Text.Pandoc                as P
import qualified Text.Pandoc.Extensions     as P

import qualified Control.Monad.Freer.Pandoc   as P

--import qualified Control.Monad.Freer  as FR

htmlReaderOptions = P.def { P.readerExtensions = P.extensionsFromList [P.Ext_raw_html] }

htmlReaderOptionsWithHeader = htmlReaderOptions { P.readerStandalone = True }

htmlWriterOptions = P.def { P.writerExtensions = P.extensionsFromList [P.Ext_raw_html], P.writerHTMLMathMethod = P.MathJax "" }
