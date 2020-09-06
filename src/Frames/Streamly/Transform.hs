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

module Frames.Streamly.Transform
    ( transform
    , filter
    , concurrentMapM
    , mapMaybe
    , concurrentMapMaybeM
    )
where

import qualified Frames.Streamly.InCore as FS
import Prelude hiding (filter)

import qualified Streamly                               as Streamly
import qualified Streamly.Prelude                       as Streamly
import           Streamly                                ( IsStream )
import qualified Control.Monad.Primitive                as Prim
import Control.Monad.ST (runST)
import qualified Frames                                 as Frames
import qualified Frames.InCore                          as Frames

--import Data.Proxy (Proxy(..))


-- Some Utilities for taking advantage of streamly to transform frames.
-- These are only worthwhile if the frame is large enough that
-- speculative concurreny is worth the price of converting to and from a
-- stream.
-- However, this is already how filter works in Frames,
-- just with Pipes instead of streamly.

{-
Use streamly to transform a frame.
-}
transform ::
  forall t1 as bs m.
  (IsStream t1
  , Prim.PrimMonad m
  , Frames.RecVec as
  , Frames.RecVec bs
  )
  => (t1 m (Frames.Record as) -> Streamly.SerialT m (Frames.Record bs)) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
transform f = FS.inCoreAoS . f . Streamly.fromFoldable
{-# INLINE transform #-}

-- | Filter using streamly 
filter :: (Frames.RecVec as) => (Frames.Record as -> Bool) -> Frames.FrameRec as -> Frames.FrameRec as
filter f frame = runST $ transform (Streamly.serially . Streamly.filter f) frame
{-# INLINE filter #-}

{- |
map using speculative streams (concurrency that preserves ordering of results).
-}
concurrentMapM :: (Prim.PrimMonad m
                  , Streamly.MonadAsync m                  
                  , Frames.RecVec as
                  , Frames.RecVec bs
                  ) => (Frames.Record as -> m (Frames.Record bs)) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
concurrentMapM f = transform (Streamly.aheadly . Streamly.mapM f)
{-# INLINE concurrentMapM #-}

{- |
mapMaybe using streamly
-}
mapMaybe :: (Frames.RecVec as
                      , Frames.RecVec bs
                      ) => (Frames.Record as -> Maybe (Frames.Record bs)) -> Frames.FrameRec as -> Frames.FrameRec bs
mapMaybe f frame = runST $ transform (Streamly.aheadly . Streamly.mapMaybe f) frame
{-# INLINE mapMaybe #-}

{- |
mapMaybeM using speculative streams (concurrency that preserves ordering of results).
-}
concurrentMapMaybeM :: (Prim.PrimMonad m
                       , Streamly.MonadAsync m                  
                       , Frames.RecVec as
                       , Frames.RecVec bs
                       ) => (Frames.Record as -> m (Maybe (Frames.Record bs))) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
concurrentMapMaybeM f = transform (Streamly.aheadly . Streamly.mapMaybeM f)
{-# INLINE concurrentMapMaybeM #-}


