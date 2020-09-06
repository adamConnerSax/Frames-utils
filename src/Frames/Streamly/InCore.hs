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

module Frames.Streamly.InCore
    (
      inCoreSoA
    , inCoreAoS
    , inCoreAoS'
    , inCoreSoA_F
    , inCoreAoS_F
    , inCoreAoS'_F
    )
where

import qualified Streamly                               as Streamly
import qualified Streamly.Prelude                       as Streamly
import qualified Streamly.Data.Fold                     as Streamly.Fold
import qualified Streamly.Internal.Data.Fold            as Streamly.Fold

import qualified Control.Monad.Primitive                as Prim

import qualified Data.Vinyl                             as Vinyl

import qualified Frames                                 as Frames
import qualified Frames.InCore                          as Frames

import Data.Proxy (Proxy(..))

{-
Match the pipe interop for inCore operations in Frames.
Develop via folds and expose those folds as well so a user
may use them for their own efficient stream -> frame ops
-}


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

