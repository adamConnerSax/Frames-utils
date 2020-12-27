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

import qualified Control.Monad.ST as ST

import qualified Data.Text as T
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.Functor            as V
import qualified Data.Functor.Identity as I
import qualified Frames                        as F
import qualified Frames.CSV                             as F
import qualified Frames.Streamly.CSV as FS
import qualified Frames.Streamly.InCore as FS
import qualified Streamly.Prelude as Streamly
import qualified Streamly.Internal.Prelude as Streamly
import qualified Control.Monad

{-
Simplify building a frame from [[Text]], coming, e.g., from a parser,
rather than using the built-in TH machinery.
Builds a stream of lines of text and passes those to the Frames machinery.  Then uses
the ST monad to do the mutable bits.
-}
fromTextCells :: (V.RMap rs, F.ReadRec rs, FS.RecVec rs) => [[T.Text]] -> Either T.Text (F.FrameRec rs)
fromTextCells parsed = do
    let lineStream = Streamly.fromList $ fmap (T.intercalate ",") parsed -- (IsStream t, Monad m) => t m [Text]
        recEStream = FS.streamTableEither lineStream -- t m (F.Rec (Either T.Text .: ElField) X)
        recES = sequence $ Streamly.map (F.rtraverse V.getCompose) recEStream
    case recES of
      Left a -> Left a
      Right s ->  Right $  ST.runST $ FS.inCoreAoS $ Streamly.hoist (return . I.runIdentity) s

{-
Simplify building a frame from [[Text]], coming, e.g., from a parser,
rather than using the built-in TH machinery.
Builds a stream of lines of text and passes those to the Frames machinery.  Then uses
the ST monad to do the mutable bits.

This version parses as rs, allows modification while still a stream and then returns the modified records.
-}
fromTextCellsMapped :: (V.RMap rs, F.ReadRec rs, FS.RecVec rs')
                    => (F.Record rs -> F.Record rs')
                    -> [[T.Text]]
                    -> Either T.Text (F.FrameRec rs')
fromTextCellsMapped recMap parsed = do
    let lineStream = Streamly.fromList $ fmap (T.intercalate ",") parsed -- (IsStream t, Monad m) => t m [Text]
        recEStream = FS.streamTableEither lineStream -- t m (F.Rec (Either T.Text .: ElField) X)
        recES = sequence $ Streamly.map (F.rtraverse V.getCompose) recEStream
    case recES of
      Left a -> Left a
      Right s ->  Right $  ST.runST $ FS.inCoreAoS $ Streamly.hoist (return . I.runIdentity) $ Streamly.map recMap s



fromTextCellsMappedE :: (V.RMap rs, F.ReadRec rs, FS.RecVec rs')
                    => (F.Record rs -> Either T.Text (F.Record rs'))
                    -> [[T.Text]]
                    -> Either T.Text (F.FrameRec rs')
fromTextCellsMappedE recMapE parsed = do
    let lineStream = Streamly.fromList $ fmap (T.intercalate ",") parsed -- (IsStream t, Monad m) => t m [Text]
        recEStream = FS.streamTableEither lineStream -- t m (F.Rec (Either T.Text .: ElField) X)
        recES = sequence $ Streamly.map (recMapE Control.Monad.<=< F.rtraverse V.getCompose) recEStream
    case recES of
      Left a -> Left a
      Right s ->  Right $  ST.runST $ FS.inCoreAoS $ Streamly.hoist (return . I.runIdentity) s

