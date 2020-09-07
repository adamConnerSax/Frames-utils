{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
-- all commented below are TBD
module Frames.Streamly.CSV
    (
      readTable
    , readTableOpt
    , readTableMaybe
    , readTableMaybeOpt
    , readTableEither
    , readTableEitherOpt
    , streamTable
    , streamTableOpt
    , streamTableMaybe  
    , streamTableMaybeOpt
    , streamTableEither
    , streamTableEitherOpt
      -- * Produce (streaming) CSV output
    , streamToCSV
    , streamCSV
    , streamToSV
    , streamSV
--    , listCSV -- so people need not learn streamly just to get csv they can work with
--    , streamPrintableCSV
--    , listPrintableCSV
    )
where

import qualified Streamly.Prelude                       as Streamly
--import qualified Streamly                               as Streamly
import           Streamly                                ( IsStream )
import qualified Streamly.Data.Fold                     as Streamly.Fold
import qualified Streamly.Internal.Data.Fold            as Streamly.Fold
import           Control.Monad.Catch                     ( MonadCatch )
import           Control.Monad.IO.Class                  ( MonadIO(liftIO) )
import qualified Streamly.Internal.FileSystem.File      as File
import           Data.Maybe                              (isNothing)
import qualified Data.Text                              as T
import qualified Data.Text.IO                           as T
import qualified Streamly.Internal.Data.Unicode.Stream  as Streamly.Unicode

import qualified Data.Vinyl                             as Vinyl
import qualified Data.Vinyl.Functor                     as Vinyl
--import qualified Data.Vinyl.TypeLevel                     as Vinyl
import           Data.Word                               ( Word8 )

import qualified System.Clock

import qualified Frames                                 as Frames
import qualified Frames.CSV                             as Frames
import qualified Frames.ShowCSV                         as Frames 

import Data.Proxy (Proxy (..))
  
-- Do we need to match type-application order to Frames API?
streamToSV
  :: forall rs m t.
     ( Frames.ColumnHeaders rs
     , Monad m
     , Vinyl.RecordToList rs
     , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
     , IsStream t
     )
     => T.Text -> t m (Frames.Record rs) -> t m T.Text
streamToSV sep s =
  (T.intercalate sep . fmap T.pack $ Frames.columnHeaders (Proxy :: Proxy (Frames.Record rs)))
  `Streamly.cons`
  (Streamly.map (T.intercalate sep . Frames.showFieldsCSV) s)  
{-# INLINEABLE streamToSV #-}

streamToCSV
  :: forall rs m t.
     ( Frames.ColumnHeaders rs
     , Monad m
     , Vinyl.RecordToList rs
     , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
     , IsStream t
     )
     => t m (Frames.Record rs) -> t m T.Text
streamToCSV = streamToSV ","
{-# INLINEABLE streamToCSV #-}

streamSV
  :: forall f rs m t.
     ( Frames.ColumnHeaders rs
     , Foldable f
     , Monad m
     , Vinyl.RecordToList rs
     , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
     , IsStream t
     )
     => T.Text -> f (Frames.Record rs) -> t m T.Text
streamSV sep = streamToSV sep . Streamly.fromFoldable  
{-# INLINEABLE streamSV #-}

streamCSV
  :: forall f rs m t.
     ( Frames.ColumnHeaders rs
     , Foldable f
     , Monad m
     , Vinyl.RecordToList rs
     , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
     , IsStream t
     )
     => f (Frames.Record rs) -> t m T.Text
streamCSV = streamSV ","

{-
writeSV
  ::  forall rs m t.
   ( Frames.ColumnHeaders rs
   , Monad m
   , MonadIO m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
   , IsStream t
   )
  => T.Text -> FilePath -> t m (F.Record rs) -> m ()
writeSV sep fp = . streamToSV sep 
-}

-- Thanks to Tim Pierson for the functions below!

-- | Stream a table from a file path, using the default options.
-- Results composed with the @Maybe@ functor. Unparsed fields are returned as @Nothing@.
-- NB:  If the inferred/given rs is different from the actual file row-type, things will go awry.
readTableMaybe
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs
    , MonadCatch m)
    => FilePath
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs)
readTableMaybe = readTableMaybeOpt Frames.defaultParser
{-# INLINEABLE readTableMaybe #-}

-- | Stream a table from a file path.
-- Results composed with the @Maybe@ functor. Unparsed fields are returned as @Nothing@.
-- NB:  If the inferred/given rs is different from the actual file row-type, things will go awry.
readTableMaybeOpt
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs
    , MonadCatch m)
    => Frames.ParserOptions
    -> FilePath
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs)
readTableMaybeOpt opts = Streamly.map recEitherToMaybe . readTableEitherOpt opts
{-# INLINEABLE readTableMaybeOpt #-}

-- | Stream a table from a file path.
-- Results composed with the @Either Text@ functor. Unparsed fields are returned as a @Left@
-- containing the string that failed to parse.
-- Uses default options.
-- NB:  If the inferred/given rs is different from the actual file row-type, things will go awry.
readTableEither
  :: forall rs t m.
     (MonadIO m
     , IsStream t
     , Vinyl.RMap rs
     , Frames.ReadRec rs
     , MonadCatch m)
  => FilePath
  -> t m (Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs)
readTableEither = readTableEitherOpt Frames.defaultParser

-- | Stream a table from a file path.
-- Results composed with the @Either Text@ functor. Unparsed fields are returned as a @Left@
-- containing the string that failed to parse.
-- NB:  If the inferred/given rs is different from the actual file row-type, things will go awry.
readTableEitherOpt
  :: forall rs t m.
     (MonadIO m
     , IsStream t
     , Vinyl.RMap rs
     , Frames.ReadRec rs
     , MonadCatch m)
  => Frames.ParserOptions
  -> FilePath
  -> t m (Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs)
readTableEitherOpt opts src = do
  let  handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                    | otherwise                              = id
       doParse = Frames.readRec
  Streamly.map (doParse . Frames.tokenizeRow opts)
    . handleHeader
    . Streamly.map T.pack
    . Streamly.splitOnSuffix (== '\n') Streamly.Fold.toList
    . Streamly.Unicode.decodeUtf8
    $ File.toBytes src    
{-# INLINEABLE readTableEitherOpt #-}


-- | Stream Table from a file path, dropping rows where any field fails to parse
-- | Use default options
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
readTable
  :: forall rs t m.
     (MonadIO m
     , IsStream t
     , Vinyl.RMap rs
     , Frames.ReadRec rs
     , MonadCatch m)
  => FilePath
  -> t m (Frames.Record rs)
readTable = readTableOpt Frames.defaultParser
{-# INLINEABLE readTable #-}

-- | Stream Table from a file path, dropping rows where any field fails to parse
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
readTableOpt
  :: forall rs t m.
     (MonadIO m
     , IsStream t
     , Vinyl.RMap rs
     , Frames.ReadRec rs
     , MonadCatch m)
  => Frames.ParserOptions
  -> FilePath
  -> t m (Frames.Record rs)
readTableOpt opts src =
  Streamly.mapMaybe (Frames.recMaybe . doParse . Frames.tokenizeRow opts)
  . handleHeader
  . Streamly.splitOnSuffix (== '\n') (fmap T.pack $ Streamly.Fold.toList)
  . Streamly.Unicode.decodeUtf8
  $ File.toBytes src
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = recEitherToMaybe . Frames.readRec
{-# INLINEABLE readTableOpt #-}

-- | Convert a stream of `Word8` to a table by decoding to utf8 and splitting the stream
-- on newline ('\n') characters.
-- Each field is returned in an @Either Text@ functor. @Right a@ for successful parses
-- and @Left Text@ when parsing fails, containing the text that failed to Parse.
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTableEither
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs)
    => t m Word8
    -> t m (Vinyl.Rec ((Either T.Text) Vinyl.:. Vinyl.ElField) rs)
streamTableEither = streamTableEitherOpt Frames.defaultParser
{-# INLINEABLE streamTableEither #-}

-- | Convert a stream of `Word8` to a table by decoding to utf8 and splitting the stream
-- on newline ('\n') characters.
-- Each field is returned in an @Either Text@ functor. @Right a@ for successful parses
-- and @Left Text@ when parsing fails, containing the text that failed to Parse.
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTableEitherOpt
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs)
    => Frames.ParserOptions
    -> t m Word8
    -> t m (Vinyl.Rec ((Either T.Text) Vinyl.:. Vinyl.ElField) rs)
streamTableEitherOpt opts =
    Streamly.map (doParse . Frames.tokenizeRow opts)
    . handleHeader
    . Streamly.splitOnSuffix (== '\n') (fmap T.pack $ Streamly.Fold.toList)
    . Streamly.Unicode.decodeUtf8
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = Frames.readRec    
{-# INLINEABLE streamTableEitherOpt #-}


-- | Convert a stream of `Word8` to a table by decoding to utf8 and splitting the stream
-- on newline ('\n') characters. Use default options
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTableMaybe
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs)
    => t m Word8
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs)
streamTableMaybe = streamTableMaybeOpt Frames.defaultParser 
{-# INLINEABLE streamTableMaybe #-}

-- | Convert a stream of `Word8` to a table by decoding to utf8 and splitting the stream
-- on newline ('\n') characters.
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTableMaybeOpt
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs)
    => Frames.ParserOptions
    -> t m Word8
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs)
streamTableMaybeOpt opts = Streamly.map recEitherToMaybe . streamTableEitherOpt opts
{-# INLINEABLE streamTableMaybeOpt #-}

-- | Convert a stream of `Word8` to a table by decoding to utf8 and splitting the stream
-- on newline ('\n') characters, dropping rows where any field fails to parse.
-- Use default options.
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTable
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs
    )
    => t m Word8
    -> t m (Frames.Record rs)
streamTable = streamTableOpt Frames.defaultParser
{-# INLINEABLE streamTable #-}

-- | Convert a stream of `Word8` to a table by decoding to utf8 and splitting the stream
-- on newline ('\n') characters, dropping rows where any field fails to parse.
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTableOpt
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs
    )
    => Frames.ParserOptions
    -> t m Word8
    -> t m (Frames.Record rs)
streamTableOpt opts =
    Streamly.mapMaybe (Frames.recMaybe . doParse . Frames.tokenizeRow opts)
    . handleHeader
    . Streamly.splitOnSuffix (== '\n') (fmap T.pack $ Streamly.Fold.toList)
    . Streamly.Unicode.decodeUtf8
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = recEitherToMaybe . Frames.readRec
{-# INLINE streamTableOpt #-}


recEitherToMaybe :: Vinyl.RMap rs => Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs -> Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs
recEitherToMaybe = Vinyl.rmap (either (const (Vinyl.Compose Nothing)) (Vinyl.Compose . Just) . Vinyl.getCompose)
{-# INLINE recEitherToMaybe #-}


-- tracing fold
runningCountF :: MonadIO m => T.Text -> (Int -> T.Text) -> T.Text -> Streamly.Fold.Fold m a ()
runningCountF startMsg countMsg endMsg = Streamly.Fold.Fold step start done where
  start = liftIO (T.putStr startMsg) >> return 0
  step !n _ = liftIO $ do
    t <- System.Clock.getTime System.Clock.ProcessCPUTime
    putStr $ show t ++ ": "
    T.putStrLn $ countMsg n
    return (n+1)
  done _ = liftIO $ T.putStrLn endMsg

{-  
-- | Given a record of functions to map each field to Text,
-- transform a stream of records into a stream of lines of Text,
-- headers first, with headers/fields separated by the given separator.
streamSV
  :: forall rs t m f.
     (Vinyl.RecordToList rs
     , Vinyl.RApply (Vinyl.Unlabeled rs)
     , Frames.ColumnHeaders rs
     , IsStream t
     , Monad m
     )
  => Frames.Rec (Vinyl.Lift (->) Vinyl.Identity (Vinyl.Const T.Text)) (Vinyl.Unlabeled rs)
  -> T.Text
  -> t m (Frames.Rec f rs)
  -> t m T.Text
streamSV toTextRec sep s = 
  (T.intercalate sep . fmap T.pack $ Frames.columnHeaders (Proxy :: Proxy (Frames.Record rs)))
  `Streamly.cons`
  (Streamly.map (T.intercalate sep . Vinyl.recordToList . Vinyl.rapply toTextRec . Vinyl.rmap (Vinyl.Identity . Vinyl.getField)) s)
{-# INLINEABLE streamSV #-}

-- | Given a record of functions to map each field to Text,
-- transform a stream of records into a (lazy) list of lines of Text,
-- headers first, with headers/fields separated by the given separator.
listSV
  :: forall rs m f.
     (Vinyl.RecordToList rs
     , Vinyl.RApply rs
     , Frames.ColumnHeaders rs
     , Monad m
     )
  => Frames.Rec (Vinyl.Lift (->) f (Vinyl.Const T.Text)) rs
  -> T.Text
  -> Streamly.SerialT m (Frames.Rec f rs)
  -> m [T.Text]
listSV toTextRec sep = Streamly.toList . streamSV toTextRec sep
{-# INLINEABLE listSV #-}
-}
