{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StrictData #-}
module Frames.Streamly
    ( streamTableMaybe
    , streamTable
    , toTableMaybe
    , toTable
    , unquotedCSV
    )
where
import qualified Streamly.Prelude              as Streamly
import           Streamly                       ( IsStream )
import qualified Streamly.Data.Fold            as Streamly.Fold
import           Control.Monad.Catch            ( MonadCatch )
import           Control.Monad.IO.Class         ( MonadIO )
import qualified Streamly.Internal.FileSystem.File
                                               as File
import           Data.Maybe (isNothing)
import qualified Data.Text                     as T
import qualified Streamly.Internal.Data.Unicode.Stream as Streamly.Unicode

import qualified Data.Vinyl                    as Vinyl
import qualified Data.Vinyl.Functor            as Vinyl
import           Data.Word                      ( Word8 )

import qualified Frames.CSV                     as Frames 
import qualified Frames.Rec                     as Frames


-- Thanks to Tim Pierson for these functions!                 

-- | Stream a table from a file path.
--
streamTableMaybe
    :: (MonadIO m, IsStream t, Vinyl.RMap rs, Frames.ReadRec rs, MonadCatch m)
    => Frames.ParserOptions
    -> FilePath
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs)
streamTableMaybe opts src =
  Streamly.map (doParse . Frames.tokenizeRow opts)
  . handleHeader
  . Streamly.map T.pack
  . Streamly.splitOnSuffix (== '\n') Streamly.Fold.toList
  . Streamly.Unicode.decodeUtf8
  $ File.toBytes src
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = recEitherToMaybe . Frames.readRec
{-# INLINE streamTableMaybe #-}

-- | Stream Table from a file path, dropping rows where any field fails to parse
streamTable
    :: (MonadIO m, IsStream t, Vinyl.RMap rs, Frames.ReadRec rs, MonadCatch m)
    => Frames.ParserOptions
    -> FilePath
    -> t m (Frames.Record rs)
streamTable opts src =
  Streamly.mapMaybe (Frames.recMaybe . doParse . Frames.tokenizeRow opts)
  . handleHeader
  . Streamly.map T.pack
  . Streamly.splitOnSuffix (== '\n') Streamly.Fold.toList
  . Streamly.Unicode.decodeUtf8
  $ File.toBytes src
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = recEitherToMaybe . Frames.readRec
{-# INLINE streamTable #-}


-- | Convert a stream of `Word8` to a table by decoding to utf8 and splitting the stream
-- on newline ('\n') characters.
--
toTableMaybe
    :: (MonadIO m, IsStream t, Vinyl.RMap rs, Frames.ReadRec rs)
    => Frames.ParserOptions
    -> t m Word8
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs)
toTableMaybe opts =
    Streamly.map (doParse . Frames.tokenizeRow opts)
    . handleHeader
    . Streamly.map T.pack
    . Streamly.splitOnSuffix (== '\n') Streamly.Fold.toList
    . Streamly.Unicode.decodeUtf8
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = recEitherToMaybe . Frames.readRec
{-# INLINE toTableMaybe #-}
-- | Convert a stream of `Word8` to a table by decoding to utf8 and splitting the stream
-- on newline ('\n') characters, dropping rows where any field fails to parse.
toTable
    :: (MonadIO m, IsStream t, Vinyl.RMap rs, Frames.ReadRec rs)
    => Frames.ParserOptions
    -> t m Word8
    -> t m (Frames.Record rs)
toTable opts =
    Streamly.mapMaybe (Frames.recMaybe . doParse . Frames.tokenizeRow opts)
    . handleHeader
    . Streamly.map T.pack
    . Streamly.splitOnSuffix (== '\n') Streamly.Fold.toList
    . Streamly.Unicode.decodeUtf8
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = recEitherToMaybe . Frames.readRec
{-# INLINE toTable #-}


recEitherToMaybe :: Vinyl.RMap rs => Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs -> Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs
recEitherToMaybe = Vinyl.rmap (either (const (Vinyl.Compose Nothing)) (Vinyl.Compose . Just) . Vinyl.getCompose)
{-# INLINEABLE recEitherToMaybe #-}

-- | ParserOptions for a CSV file without quoting
--
unquotedCSV :: Frames.ParserOptions
unquotedCSV = Frames.ParserOptions Nothing "," Frames.NoQuoting
