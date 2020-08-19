{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module Frames.Streamly
    ( streamTableMaybe
    , streamTable
    , toTableMaybe
    , toTable
    , inCoreSoA
    , inCoreAoS
    , inCoreAoS'
    , inCoreSoA_F
    , inCoreAoS_F
    , inCoreAoS'_F
    , streamTransform
    , streamlyFilter
    , concurrentStreamlyMapM
    , streamlyMapMaybe
    , concurrentStreamlyMapMaybeM
    , unquotedCSV
    , runningCountF
    )
where

import qualified Streamly                               as Streamly
import qualified Streamly.Prelude                       as Streamly
--import qualified Streamly.Internal.Prelude              as Streamly
import           Streamly                                ( IsStream )
import qualified Streamly.Data.Fold                     as Streamly.Fold
import qualified Streamly.Internal.Data.Fold            as Streamly.Fold
--import qualified Streamly.Internal.Data.Fold.Types      as Streamly.Fold
import           Control.Monad.Catch                     ( MonadCatch )
import           Control.Monad.IO.Class                  ( MonadIO(liftIO) )
import qualified Control.Monad.Primitive                as Prim
import Control.Monad.ST (runST)
import qualified Streamly.Internal.FileSystem.File      as File
import           Data.Maybe                              (isNothing)
import qualified Data.Text                              as T
import qualified Data.Text.IO                           as T
import qualified Streamly.Internal.Data.Unicode.Stream  as Streamly.Unicode

import qualified Data.Vinyl                             as Vinyl
import qualified Data.Vinyl.Functor                     as Vinyl
import           Data.Word                               ( Word8 )

import qualified System.Clock

import qualified Frames                                 as Frames
import qualified Frames.CSV                             as Frames 
import qualified Frames.InCore                          as Frames

import Data.Proxy (Proxy(..))

-- match the Pipes interop that comes with Frames

inCoreSoA_F :: forall m rs. (Prim.PrimMonad m, Frames.RecVec rs)
          => Streamly.Fold.Fold m (Frames.Record rs) (Int, Vinyl.Rec (((->) Int) Frames.:. Frames.ElField) rs)
inCoreSoA_F = Streamly.Fold.mkFold feed initial fin
  where feed (!i, !sz, !mvs') row
          | i == sz = Frames.growRec (Proxy::Proxy rs) mvs'
                      >>= flip feed row . (i, sz*2,)
          | otherwise = do Frames.writeRec (Proxy::Proxy rs) i mvs' row
                           return (i+1, sz, mvs')
                         
        initial = do
          mvs <- Frames.allocRec (Proxy :: Proxy rs) Frames.initialCapacity
          return (0, Frames.initialCapacity, mvs)
          
        fin (n, _, mvs') =
          do vs <- Frames.freezeRec (Proxy::Proxy rs) n mvs'
             return . (n,) $ Frames.produceRec (Proxy::Proxy rs) vs
{-# INLINE inCoreSoA_F #-}

inCoreSoA :: forall m rs. (Prim.PrimMonad m, Frames.RecVec rs)
          => Streamly.SerialT m (Frames.Record rs)
          -> m (Int, Vinyl.Rec (((->) Int) Frames.:. Frames.ElField) rs)
inCoreSoA = Streamly.fold inCoreSoA_F
{-# INLINE inCoreSoA #-}

{-
  do
  mvs <- Frames.allocRec (Proxy :: Proxy rs) Frames.initialCapacity
  let feed (!i, !sz, !mvs') row
        | i == sz = Frames.growRec (Proxy::Proxy rs) mvs'
                    >>= flip feed row . (i, sz*2,)
        | otherwise = do Frames.writeRec (Proxy::Proxy rs) i mvs' row
                         return (i+1, sz, mvs')
      fin (n, _, mvs') =
        do vs <- Frames.freezeRec (Proxy::Proxy rs) n mvs'
           return . (n,) $ Frames.produceRec (Proxy::Proxy rs) vs
  let streamFold = Streamly.Fold.mkFold feed (return (0, Frames.initialCapacity, mvs)) fin 
  Streamly.fold streamFold s
-}

inCoreAoS_F :: forall m rs. (Prim.PrimMonad m, Frames.RecVec rs)
          => Streamly.Fold.Fold m (Frames.Record rs) (Frames.FrameRec rs)
inCoreAoS_F = fmap (uncurry Frames.toAoS) inCoreSoA_F
{-# INLINE inCoreAoS_F #-}


inCoreAoS :: forall m rs. (Prim.PrimMonad m, Frames.RecVec rs)
          => Streamly.SerialT m (Frames.Record rs)
          -> m (Frames.FrameRec rs)
inCoreAoS = Streamly.fold inCoreAoS_F --fmap (uncurry Frames.toAoS) . inCoreSoA
{-# INLINE inCoreAoS #-}

inCoreAoS'_F ::  forall ss rs m. (Prim.PrimMonad m, Frames.RecVec rs)
           => (Frames.Rec ((->) Int Frames.:. Frames.ElField) rs -> Frames.Rec ((->) Int Frames.:. Frames.ElField) ss)
           -> Streamly.Fold.Fold m (Frames.Record rs) (Frames.FrameRec ss)
inCoreAoS'_F f  = fmap (uncurry Frames.toAoS . aux) inCoreSoA_F
  where aux (x,y) = (x, f y)
{-# INLINE inCoreAoS'_F #-}  

inCoreAoS' ::  forall ss rs m. (Prim.PrimMonad m, Frames.RecVec rs)
           => (Frames.Rec ((->) Int Frames.:. Frames.ElField) rs -> Frames.Rec ((->) Int Frames.:. Frames.ElField) ss)
           -> Streamly.SerialT m (Frames.Record rs)
           -> m (Frames.FrameRec ss)
inCoreAoS' f = Streamly.fold (inCoreAoS'_F f)
{-# INLINE inCoreAoS' #-}  

-- Some Utilities for taking advantage of streamly to transform frames.
-- These are only worthwhile if the frame is large enough that
-- speculative concurreny is worth the price of converting to and from a
-- stream.
-- However, this is already how filter works in Frames,
-- just with Pipes instead of streamly.

{-
Use streamly to transform a frame.
-}
streamTransform ::
  forall t1 as bs m.
  (IsStream t1
  , Prim.PrimMonad m
  , Frames.RecVec as
  , Frames.RecVec bs
  )
  => (t1 m (Frames.Record as) -> Streamly.SerialT m (Frames.Record bs)) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
streamTransform f = inCoreAoS . f . Streamly.fromFoldable
{-# INLINE streamTransform #-}

-- | Filter using streamly 
streamlyFilter :: (Frames.RecVec as) => (Frames.Record as -> Bool) -> Frames.FrameRec as -> Frames.FrameRec as
streamlyFilter f frame = runST $ streamTransform (Streamly.serially . Streamly.filter f) frame
{-# INLINE streamlyFilter #-}

{- |
map using speculative streams (concurrency that preserves ordering of results).
-}
concurrentStreamlyMapM :: (Prim.PrimMonad m
                          , Streamly.MonadAsync m                  
                          , Frames.RecVec as
                          , Frames.RecVec bs
                          ) => (Frames.Record as -> m (Frames.Record bs)) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
concurrentStreamlyMapM f = streamTransform (Streamly.aheadly . Streamly.mapM f)
{-# INLINE concurrentStreamlyMapM #-}

{- |
mapMaybe using streamly
-}
streamlyMapMaybe :: (Frames.RecVec as
                      , Frames.RecVec bs
                      ) => (Frames.Record as -> Maybe (Frames.Record bs)) -> Frames.FrameRec as -> Frames.FrameRec bs
streamlyMapMaybe f frame = runST $ streamTransform (Streamly.aheadly . Streamly.mapMaybe f) frame
{-# INLINE streamlyMapMaybe #-}

{- |
mapMaybeM using speculative streams (concurrency that preserves ordering of results).
-}
concurrentStreamlyMapMaybeM :: (Prim.PrimMonad m
                               , Streamly.MonadAsync m                  
                               , Frames.RecVec as
                               , Frames.RecVec bs
                               ) => (Frames.Record as -> m (Maybe (Frames.Record bs))) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
concurrentStreamlyMapMaybeM f = streamTransform (Streamly.aheadly . Streamly.mapMaybeM f)
{-# INLINE concurrentStreamlyMapMaybeM #-}



-- Thanks to Tim Pierson for the functions below!

-- | Stream a table from a file path.
-- NB:  If the inferred/given rs is different from the actual file row-type, things will go awry.
-- FIXME : Remove Monad (t m) constraint once debugged.
streamTableMaybe
    :: forall rs t m.
    (MonadIO m
    , IsStream t
--    , Monad (t m)
    , Vinyl.RMap rs
    , Frames.ReadRec rs
    , MonadCatch m)
    => Frames.ParserOptions
    -> FilePath
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs)
streamTableMaybe opts src = do
  let  handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                    | otherwise                              = id
       doParse = recEitherToMaybe . Frames.readRec
  Streamly.map (doParse . Frames.tokenizeRow opts)
    . handleHeader
    . Streamly.map T.pack
    . Streamly.splitOnSuffix (== '\n') Streamly.Fold.toList
    . Streamly.Unicode.decodeUtf8
    $ File.toBytes src    
{-# INLINE streamTableMaybe #-}

-- | Stream Table from a file path, dropping rows where any field fails to parse
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTable
    :: forall rs t m.
      (MonadIO m
      , IsStream t
      , Vinyl.RMap rs
      , Frames.ReadRec rs
      , MonadCatch m)
    => Frames.ParserOptions
    -> FilePath
    -> t m (Frames.Record rs)
streamTable opts src =
  Streamly.mapMaybe (Frames.recMaybe . doParse . Frames.tokenizeRow opts)
  . handleHeader
  . Streamly.splitOnSuffix (== '\n') (fmap T.pack $ Streamly.Fold.toList)
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
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
toTableMaybe
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs)
    => Frames.ParserOptions
    -> t m Word8
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs)
toTableMaybe opts =
    Streamly.map (doParse . Frames.tokenizeRow opts)
    . handleHeader
    . Streamly.splitOnSuffix (== '\n') (fmap T.pack $ Streamly.Fold.toList)
    . Streamly.Unicode.decodeUtf8
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = recEitherToMaybe . Frames.readRec    
{-# INLINE toTableMaybe #-}

-- | Convert a stream of `Word8` to a table by decoding to utf8 and splitting the stream
-- on newline ('\n') characters, dropping rows where any field fails to parse.
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
toTable
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs
    )
    => Frames.ParserOptions
    -> t m Word8
    -> t m (Frames.Record rs)
toTable opts =
    Streamly.mapMaybe (Frames.recMaybe . doParse . Frames.tokenizeRow opts)
    . handleHeader
    . Streamly.splitOnSuffix (== '\n') (fmap T.pack $ Streamly.Fold.toList)
    . Streamly.Unicode.decodeUtf8
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = recEitherToMaybe . Frames.readRec
{-# INLINE toTable #-}


recEitherToMaybe :: Vinyl.RMap rs => Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs -> Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs
recEitherToMaybe = Vinyl.rmap (either (const (Vinyl.Compose Nothing)) (Vinyl.Compose . Just) . Vinyl.getCompose)
{-# INLINE recEitherToMaybe #-}

-- | ParserOptions for a CSV file without quoting
--
unquotedCSV :: Frames.ParserOptions
unquotedCSV = Frames.ParserOptions Nothing "," Frames.NoQuoting
{-# INLINEABLE unquotedCSV #-}

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


-- Fold to Text
--foldToText :: Streamly.Fold.Fold m Char T.Text
--foldToText = Streamly.Fold.Fold step start done where
  
