{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.FromTextCells
  (
    fromTextCells
  , fromTextCellsMapped
  , fromTextCellsMappedE
  )
where

--import qualified Control.Monad.ST as ST

import qualified Data.Text as T
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.Functor            as V
import qualified Frames                        as F
--import qualified Frames.CSV                             as F
import qualified Frames.Streamly.CSV as FS
import qualified Frames.Streamly.InCore as FS
import Frames.Streamly.Streaming.Streamly (StreamlyStream(..), SerialT)
import qualified Streamly.Prelude as Streamly
import qualified Control.Monad
import Control.Exception (throwIO)
import System.IO.Error (userError)

type StreamType = StreamlyStream SerialT

{-
Simplify building a frame from [[Text]], coming, e.g., from a parser,
rather than using the built-in TH machinery.
Builds a stream of lines of text and passes those to the Frames machinery.  Then uses
the ST monad to do the mutable bits.
-}
fromTextCells :: forall rs. (V.RMap rs, FS.StrictReadRec rs, FS.RecVec rs) => [[T.Text]] -> IO (F.FrameRec rs)
fromTextCells parsed = do
    let lineStream = Streamly.fromList parsed -- (IsStream t, Monad m) => t m [Text]
        recEStream = stream $ FS.streamTableEither @rs @StreamType $ StreamlyStream lineStream -- t m (F.Rec (Either T.Text .: ElField) X)
        throwLeft = either (throwIO .  userError . toString) return
        recES = Streamly.mapM (throwLeft . F.rtraverse V.getCompose) recEStream
    FS.inCoreAoS $ StreamlyStream recES
{-
    case recES of
      Left a -> throwIO a
      Right s ->  FS.inCoreAoS s
-}
{-
Simplify building a frame from [[Text]], coming, e.g., from a parser,
rather than using the built-in TH machinery.
Builds a stream of lines of text and passes those to the Frames machinery.  Then uses
the ST monad to do the mutable bits.

This version parses as rs, allows modification while still a stream and then returns the modified records.
-}
fromTextCellsMapped :: forall rs rs'.(V.RMap rs, FS.StrictReadRec rs, FS.RecVec rs')
                    => (F.Record rs -> F.Record rs')
                    -> [[T.Text]]
                    -> IO (F.FrameRec rs')
fromTextCellsMapped recMap parsed = do
    let lineStream = Streamly.fromList parsed -- (IsStream t, Monad m) => t m [Text]
        recEStream = stream $ FS.streamTableEither @rs @StreamType $ StreamlyStream lineStream -- t m (F.Rec (Either T.Text .: ElField) X)
        throwLeft = either (throwIO .  userError . toString) return
        recES = Streamly.mapM (throwLeft . F.rtraverse V.getCompose) recEStream
    FS.inCoreAoS (StreamlyStream $ Streamly.map recMap recES)
{-

    case recES of
      Left a -> throwIO a
      Right s ->  FS.inCoreAoS $ Streamly.map recMap s
-}


fromTextCellsMappedE :: forall rs rs'.(V.RMap rs, FS.StrictReadRec rs, FS.RecVec rs')
                    => (F.Record rs -> Either T.Text (F.Record rs'))
                    -> [[T.Text]]
                    -> IO (F.FrameRec rs')
fromTextCellsMappedE recMapE parsed = do
    let lineStream = Streamly.fromList parsed -- (IsStream t, Monad m) => t m [Text]
        recEStream = stream $ FS.streamTableEither @rs @StreamType $ StreamlyStream  lineStream -- t m (F.Rec (Either T.Text .: ElField) X)
        throwLeft = either (throwIO .  userError . toString) return
        recES = Streamly.mapM (throwLeft . (recMapE Control.Monad.<=< F.rtraverse V.getCompose)) recEStream
    FS.inCoreAoS $ StreamlyStream recES
{-
    case recES of
      Left a -> throwIO a
      Right s ->  FS.inCoreAoS s
-}
