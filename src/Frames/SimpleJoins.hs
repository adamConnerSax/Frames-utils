{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE DerivingVia         #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.SimpleJoins
  (
    appendFromKeyed
  , leftJoinM
  , leftJoinM3
  ) where

import qualified Control.Foldl as FL
import qualified Data.Vinyl           as V
import           Data.Vinyl.TypeLevel as V --(type (++), Snd)
import qualified Data.Map as M
import qualified Frames               as F
import qualified Frames.InCore as FI
import qualified Frames.Melt as F
import qualified Data.Discrimination.Grouping  as G

-- like a join but I'm not sure of all the properties a join should preserve/have...
-- returns an Either so it can tell you what key was missing upon failure
appendFromKeyed
  :: forall jc ac lc rc. (jc F.⊆ lc, ac F.⊆ lc, jc F.⊆ rc, Ord (F.Record jc), FI.RecVec (rc V.++ ac))
  => F.FrameRec lc -> F.FrameRec rc ->  Either (F.Record jc) (F.FrameRec (rc V.++ ac))
appendFromKeyed source appendTo = do
  let m = FL.fold (FL.premap (\r -> (F.rcast @jc r, F.rcast @ac r)) FL.map) source      
      g r =
        let kr = F.rcast @jc r
        in maybe (Left kr) (Right . V.rappend r) $ M.lookup kr m 
  F.toFrame <$> traverse g (FL.fold FL.list appendTo)


-- I find this to be a more useful interface for the times when I need all join keys present in rhs
leftJoinM
  :: forall ks as bs.
  (
    FI.RecVec (as V.++ (F.RDeleteAll ks bs))
  , ks F.⊆ as
  , ks F.⊆ bs
  , as F.⊆ (as V.++ (F.RDeleteAll ks bs))
  , (F.RDeleteAll ks bs) F.⊆ bs
  , V.RMap as
  , V.RMap (as V.++ (F.RDeleteAll ks bs))
  , V.RecApplicative (F.RDeleteAll ks bs)
  , G.Grouping (F.Record ks)
  , FI.RecVec as
  , FI.RecVec (F.RDeleteAll ks bs)
  )
  => F.FrameRec as
  -> F.FrameRec bs
  -> Maybe (F.FrameRec (as V.++ (F.RDeleteAll ks bs)))
leftJoinM fa fb = fmap F.toFrame $ sequence $ fmap F.recMaybe $ F.leftJoin @ks fa fb
  
-- I've found this useful 
leftJoinM3
  :: forall ks as bs cs.
  (
    FI.RecVec (as V.++ (F.RDeleteAll ks bs))
  , ks F.⊆ as
  , ks F.⊆ bs
  , as F.⊆ (as V.++ (F.RDeleteAll ks bs))
  , (F.RDeleteAll ks bs) F.⊆ bs
  , V.RMap as
  , V.RMap (as V.++ (F.RDeleteAll ks bs))
  , V.RecApplicative (F.RDeleteAll ks bs)
  , G.Grouping (F.Record ks)
  , FI.RecVec as
  , FI.RecVec (F.RDeleteAll ks bs)
  , FI.RecVec ((as V.++ F.RDeleteAll ks bs) V.++ F.RDeleteAll ks cs)
  , ks F.⊆ (as V.++ (F.RDeleteAll ks bs))
  , ks F.⊆ cs
  , (as V.++ F.RDeleteAll ks bs) F.⊆ (as V.++ (F.RDeleteAll ks bs) V.++ (F.RDeleteAll ks cs))
  , (F.RDeleteAll ks cs) F.⊆ cs
  , V.RMap (as V.++ (F.RDeleteAll ks bs) V.++ (F.RDeleteAll ks cs))
  , V.RecApplicative (F.RDeleteAll ks cs)
  , FI.RecVec (F.RDeleteAll ks cs)
  )
  => F.FrameRec as
  -> F.FrameRec bs
  -> F.FrameRec cs
  -> Maybe (F.FrameRec (as V.++ (F.RDeleteAll ks bs) V.++ (F.RDeleteAll ks cs)))
leftJoinM3 fa fb fc = do
  fab <-  leftJoinM @ks fa fb
  fmap F.toFrame $ sequence $ fmap F.recMaybe $ F.leftJoin @ks fab fc
  
