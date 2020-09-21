{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Frames.Streamly.CSV
    (
      -- * read from File to Stream of Recs 
      readTable
    , readTableOpt
    , readTableMaybe
    , readTableMaybeOpt
    , readTableEither
    , readTableEitherOpt
      -- * convert streaming Text to streaming Records
    , streamTable
    , streamTableOpt
    , streamTableMaybe  
    , streamTableMaybeOpt
    , streamTableEither
    , streamTableEitherOpt
      -- * Produce (streaming) Text from Records
    , streamToCSV
    , streamCSV
    , streamToSV
    , streamSV
    , streamSV'
      -- *  write Records to Text File
    , writeCSV
    , writeSV
    , writeStreamSV
    , writeCSV_Show
    , writeSV_Show
    , writeStreamSV_Show
      -- * Utilities
    , streamToList
    , liftFieldFormatter
    , liftFieldFormatter1
    , formatTextAsIs
    , formatWithShow
    , formatWithShowCSV
    , writeLines
    , writeLines'
    , word8ToTextLines
    )
where

import qualified Streamly.Prelude                       as Streamly
import qualified Streamly                               as Streamly
import           Streamly                                ( IsStream )
import qualified Streamly.Data.Fold                     as Streamly.Fold
import qualified Streamly.Data.Unicode.Stream           as Streamly.Unicode
import qualified Streamly.Internal.FileSystem.File      as Streamly.File
import qualified Streamly.Internal.Data.Unfold          as Streamly.Unfold
import           Control.Monad.Catch                     ( MonadCatch )
import           Control.Monad.IO.Class                  ( MonadIO )

import           Data.Maybe                              (isNothing)
import qualified Data.Text                              as T

import qualified Data.Vinyl                             as Vinyl
import qualified Data.Vinyl.Functor                     as Vinyl
import qualified Data.Vinyl.TypeLevel                   as Vinyl
import qualified Data.Vinyl.Class.Method                as Vinyl
import           Data.Word                               ( Word8 )

import qualified Frames                                 as Frames
import qualified Frames.CSV                             as Frames
import qualified Frames.ShowCSV                         as Frames 

import Data.Proxy (Proxy (..))

  

-- | Given a stream of @Records@, for which all fields satisfy the `ShowCSV` constraint,
-- produce a stream of `Text`, one item (line) per `Record` with the specified separator
-- between fields.
streamToSV
  :: forall rs m t.
     ( Frames.ColumnHeaders rs
     , Monad m
     , Vinyl.RecordToList rs
     , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
     , IsStream t
     )
     => T.Text -> t m (Frames.Record rs) -> t m T.Text
streamToSV = streamSVClass @Frames.ShowCSV Frames.showCSV
{-# INLINEABLE streamToSV #-}

-- | Given a stream of @Records@, for which all fields satisfy the `ShowCSV` constraint,
-- produce a stream of CSV `Text`, one item (line) per `Record`.
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

-- | Given a foldable of @Records@, for which all fields satisfy the `ShowCSV` constraint,
-- produce a stream of `Text`, one item (line) per `Record` with the specified separator
-- between fields. 
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

-- | Given a foldable of @Records@, for which all fields satisfy the `ShowCSV` constraint,
-- produce a stream of CSV `Text`, one item (line) per `Record`.
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

-- | Convert @Rec@s to lines of `Text` using a class (which must have an instance
-- for each type in the record) to covert each field to `Text`.
streamSVClass
  :: forall c rs t m .
      ( Vinyl.RecMapMethod c Vinyl.ElField rs
      , Vinyl.RecordToList rs
      , Frames.ColumnHeaders rs
      , IsStream t
      , Monad m
     )
  => (forall a. c a => a -> T.Text)
  -> T.Text
  -> t m (Frames.Record rs)
  -> t m T.Text
streamSVClass toText sep s =
  (T.intercalate sep . fmap T.pack $ Frames.columnHeaders (Proxy :: Proxy (Frames.Record rs)))
  `Streamly.cons`
  (Streamly.map (T.intercalate sep . Vinyl.recordToList . Vinyl.rmapMethod @c aux) s)
  where
    aux :: (c (Vinyl.PayloadType Vinyl.ElField a))
        => Vinyl.ElField a
        -> Vinyl.Const T.Text a
    aux (Vinyl.Field x) = Vinyl.Const $ toText x

    
-- | Given a record of functions to map each field to Text,
-- transform a stream of records into a stream of lines of Text,
-- headers first, with headers/fields separated by the given separator.
streamSV'
  :: forall rs t m f.
     (Vinyl.RecordToList rs
     , Vinyl.RApply rs
     , Frames.ColumnHeaders rs
     , IsStream t
     , Monad m
     )
  => Frames.Rec (Vinyl.Lift (->) f (Vinyl.Const T.Text)) rs
  -> T.Text
  -> t m (Frames.Rec f rs)
  -> t m T.Text
streamSV' toTextRec sep s = 
  (T.intercalate sep . fmap T.pack $ Frames.columnHeaders (Proxy :: Proxy (Frames.Record rs)))
  `Streamly.cons`
  (Streamly.map (T.intercalate sep . Vinyl.recordToList . Vinyl.rapply toTextRec) s)
{-# INLINEABLE streamSV' #-}

-- | Convert a streamly stream into a (lazy) list
streamToList :: (IsStream t, Monad m) => t m a -> m [a]
streamToList = Streamly.toList . Streamly.adapt

-- | lift a field formatting function into the right form to append to a Rec of formatters
liftFieldFormatter :: Vinyl.KnownField t => (Vinyl.Snd t -> T.Text) -> Vinyl.Lift (->) Vinyl.ElField (Vinyl.Const T.Text) t
liftFieldFormatter toText = Vinyl.Lift $ Vinyl.Const . toText . Vinyl.getField
{-# INLINEABLE liftFieldFormatter #-}

-- | lift a composed-field formatting function into the right form to append to a Rec of formatters
-- | Perhaps to render a parsed file with @Maybe@ or @Either@ composed with @ElField@
liftFieldFormatter1 :: (Functor f, Vinyl.KnownField t)
                        => (f (Vinyl.Snd t) -> T.Text) -> Vinyl.Lift (->) (f Vinyl.:. Vinyl.ElField) (Vinyl.Const T.Text) t
liftFieldFormatter1 toText = Vinyl.Lift $ Vinyl.Const . toText . fmap Vinyl.getField . Vinyl.getCompose
{-# INLINEABLE liftFieldFormatter1 #-}

-- | Format a @Text@ field as-is.
formatTextAsIs :: (Vinyl.KnownField t, Vinyl.Snd t ~ T.Text) => Vinyl.Lift (->) Vinyl.ElField (Vinyl.Const T.Text) t
formatTextAsIs = liftFieldFormatter id
{-# INLINE formatTextAsIs #-}

-- | Format a field using the @Show@ instance of the contained type 
formatWithShow :: (Vinyl.KnownField t, Show (Vinyl.Snd t)) => Vinyl.Lift (->) Vinyl.ElField (Vinyl.Const T.Text) t
formatWithShow = liftFieldFormatter $ T.pack . show
{-# INLINE formatWithShow #-}

-- | Format a field using the @Frames.ShowCSV@ instance of the contained type 
formatWithShowCSV :: (Vinyl.KnownField t, Frames.ShowCSV (Vinyl.Snd t)) => Vinyl.Lift (->) Vinyl.ElField (Vinyl.Const T.Text) t
formatWithShowCSV = liftFieldFormatter Frames.showCSV
{-# INLINE formatWithShowCSV #-}

-- NB: Uses some internal modules from Streamly.  Will have to change when they become stable
-- | write a stream of @Text@ to a file, one line per stream item.
writeLines' :: (Streamly.MonadAsync m, MonadCatch m, Streamly.IsStream t) => FilePath -> t m T.Text -> m ()
writeLines' fp s = do
  Streamly.fold (Streamly.File.write fp)
    $ Streamly.Unicode.encodeUtf8
    $ Streamly.adapt
    $ Streamly.concatUnfold Streamly.Unfold.fromList
    $ Streamly.map T.unpack
    $ Streamly.intersperse "\n" s
{-# INLINEABLE writeLines' #-}

-- | write a stream of @Text@ to a file, one line per stream item.
-- | Monomorphised to serial streams for ease of use.
writeLines :: (Streamly.MonadAsync m, MonadCatch m) => FilePath -> Streamly.SerialT m T.Text -> m ()
writeLines = writeLines'
{-# INLINE writeLines #-}

-- NB: Uses some internal modules from Streamly.  Will have to change when they become stable
-- | write a stream of @Records@ to a file, one line per @Record@.
-- Use the 'Frames.ShowCSV' class to render each field to @Text@
writeStreamSV
  ::  forall rs m t.
   ( Frames.ColumnHeaders rs
   , MonadCatch m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
   , IsStream t
   , Streamly.MonadAsync m
   )
  => T.Text -> FilePath -> t m (Frames.Record rs) -> m ()
writeStreamSV sep fp = writeLines' fp . streamToSV sep 
{-# INLINEABLE writeStreamSV #-}

-- | write a foldable of @Records@ to a file, one line per @Record@.
-- Use the 'Frames.ShowCSV' class to render each field to @Text@
writeSV
  ::  forall rs m f.
   ( Frames.ColumnHeaders rs
   , MonadCatch m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
   , Streamly.MonadAsync m
   , Foldable f
   )
  => T.Text -> FilePath -> f (Frames.Record rs) -> m ()
writeSV sep fp = writeStreamSV sep fp . Streamly.fromFoldable @Streamly.AheadT
{-# INLINEABLE writeSV #-}

-- | write a foldable of @Records@ to a file, one line per @Record@.
-- Use the 'Frames.ShowCSV' class to render each field to @Text@
writeCSV
  ::  forall rs m f.
   ( Frames.ColumnHeaders rs
   , MonadCatch m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
   , Streamly.MonadAsync m
   , Foldable f
   )
  => FilePath -> f (Frames.Record rs) -> m ()
writeCSV fp = writeSV "," fp 
{-# INLINEABLE writeCSV #-}

-- NB: Uses some internal modules from Streamly.  Will have to change when they become stable
-- | write a stream of @Records@ to a file, one line per @Record@.
-- Use the 'Show' class to render each field to @Text@
writeStreamSV_Show
  ::  forall rs m t.
   ( Frames.ColumnHeaders rs
   , MonadCatch m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Show Vinyl.ElField rs
   , IsStream t
   , Streamly.MonadAsync m
   )
  => T.Text -> FilePath -> t m (Frames.Record rs) -> m ()
writeStreamSV_Show sep fp = writeLines' fp . streamSVClass @Show (T.pack . show) sep
{-# INLINEABLE writeStreamSV_Show #-}

-- | write a foldable of @Records@ to a file, one line per @Record@.
-- Use the 'Show' class to render each field to @Text@
writeSV_Show
  ::  forall rs m f.
   ( Frames.ColumnHeaders rs
   , MonadCatch m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Show Vinyl.ElField rs
   , Streamly.MonadAsync m
   , Foldable f
   )
  => T.Text -> FilePath -> f (Frames.Record rs) -> m ()
writeSV_Show sep fp = writeStreamSV_Show sep fp . Streamly.fromFoldable @Streamly.AheadT
{-# INLINEABLE writeSV_Show #-}

-- | write a foldable of @Records@ to a file, one line per @Record@.
-- Use the 'Show' class to render each field to @Text@
writeCSV_Show
  ::  forall rs m f.
   ( Frames.ColumnHeaders rs
   , MonadCatch m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Show Vinyl.ElField rs
   , Streamly.MonadAsync m
   , Foldable f
   )
  => FilePath -> f (Frames.Record rs) -> m ()
writeCSV_Show fp = writeSV_Show "," fp 
{-# INLINEABLE writeCSV_Show #-}

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
readTableEitherOpt opts = streamTableEitherOpt opts . word8ToTextLines . Streamly.File.toBytes 
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
readTableOpt opts = streamTableOpt opts . word8ToTextLines . Streamly.File.toBytes 
{-# INLINEABLE readTableOpt #-}

-- | Convert a stream of lines of `Text` to a table
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
    => t m T.Text
    -> t m (Vinyl.Rec ((Either T.Text) Vinyl.:. Vinyl.ElField) rs)
streamTableEither = streamTableEitherOpt Frames.defaultParser
{-# INLINEABLE streamTableEither #-}

-- | Convert a stream of lines of `Text` to records.
-- Each field is returned in an @Either Text@ functor. @Right a@ for successful parses
-- and @Left Text@ when parsing fails, containing the text that failed to Parse.
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will..go awry.
streamTableEitherOpt
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs)
    => Frames.ParserOptions
    -> t m T.Text
    -> t m (Vinyl.Rec ((Either T.Text) Vinyl.:. Vinyl.ElField) rs)
streamTableEitherOpt opts =
    Streamly.map (doParse . Frames.tokenizeRow opts)
    . handleHeader
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = Frames.readRec    
{-# INLINEABLE streamTableEitherOpt #-}

-- | Convert a stream of lines of `Text` to a table.
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will..go awry.
streamTableMaybe
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs)
    => t m T.Text
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs)
streamTableMaybe = streamTableMaybeOpt Frames.defaultParser 
{-# INLINEABLE streamTableMaybe #-}

-- | Convert a stream of lines of Text to a table .
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will..go awry.
streamTableMaybeOpt
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs)
    => Frames.ParserOptions
    -> t m T.Text
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs)
streamTableMaybeOpt opts = Streamly.map recEitherToMaybe . streamTableEitherOpt opts
{-# INLINEABLE streamTableMaybeOpt #-}

-- | Convert a stream of lines of 'Text' to a table,
-- dropping rows where any field fails to parse.
-- Use default options.
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTable
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs
    )
    => t m T.Text
    -> t m (Frames.Record rs)
streamTable = streamTableOpt Frames.defaultParser
{-# INLINEABLE streamTable #-}

-- | Convert a stream of lines of 'Text' `Word8` to a table,
-- dropping rows where any field fails to parse.
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTableOpt
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs
    )
    => Frames.ParserOptions
    -> t m T.Text
    -> t m (Frames.Record rs)
streamTableOpt opts =
    Streamly.mapMaybe (Frames.recMaybe . doParse . Frames.tokenizeRow opts)
    . handleHeader    
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = recEitherToMaybe . Frames.readRec
{-# INLINE streamTableOpt #-}

recEitherToMaybe :: Vinyl.RMap rs => Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs -> Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs
recEitherToMaybe = Vinyl.rmap (either (const (Vinyl.Compose Nothing)) (Vinyl.Compose . Just) . Vinyl.getCompose)
{-# INLINE recEitherToMaybe #-}

-- | Convert a stream of Word8 to lines of `Text` by decoding as UTF8 and splitting on "\n"
word8ToTextLines :: (IsStream t, Monad m) => t m Word8 -> t m T.Text
word8ToTextLines =  Streamly.splitOnSuffix (== '\n') (fmap T.pack $ Streamly.Fold.toList)
                    . Streamly.Unicode.decodeUtf8
{-# INLINE word8ToTextLines #-}


-- tracing fold
{-
runningCountF :: MonadIO m => T.Text -> (Int -> T.Text) -> T.Text -> Streamly.Fold.Fold m a ()
runningCountF startMsg countMsg endMsg = Streamly.Fold.Fold step start done where
  start = liftIO (T.putStr startMsg) >> return 0
  step !n _ = liftIO $ do
    t <- System.Clock.getTime System.Clock.ProcessCPUTime
    putStr $ show t ++ ": "
    T.putStrLn $ countMsg n
    return (n+1)
  done _ = liftIO $ T.putStrLn endMsg
-}

