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
{-# LANGUAGE InstanceSigs          #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.MapReduce where

import           Control.MapReduce             as MR

import qualified Control.Foldl                 as FL
import           Data.Functor.Identity          ( Identity(Identity) )
import qualified Data.Map.Monoidal             as MM
import qualified Data.HashMap.Monoidal         as HMM
import           Data.Monoid                    ( (<>)
                                                , Monoid(..)
                                                )
import           Data.Hashable                  ( Hashable )
import           Data.Kind                      ( Type
                                                , Constraint
                                                )
import qualified Data.Profunctor               as P
import qualified Frames                        as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V

assignFrame
  :: forall ks cs rs
   . (Ord (F.Record ks), ks F.⊆ rs, cs F.⊆ rs)
  => MR.Assign Ord (F.Record ks) (F.Record rs) (F.Record cs)
assignFrame = MR.assign F.rcast F.rcast

reduceAndAddKey
  :: FI.RecVec ((ks V.++ cs))
  => (b -> F.Record cs)
  -> MR.ReduceOne (F.Record ks) b (F.FrameRec (ks V.++ cs))
reduceAndAddKey process =
  MR.ReduceOne $ \k b -> F.toFrame $ [V.rappend k (process b)]

aggregateMonoidalF
  :: forall ks rs as b cs f g h
   . ( ks F.⊆ as
     , as F.⊆ as
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Functor f
     , Foldable h
     , Functor h
     , Monoid b
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (F.Record as -> b)
  -> (b -> F.Record cs)
  -> h (F.Rec g rs)
  -> F.FrameRec (ks V.++ cs)
aggregateMonoidalF unpack process extract = MR.mapReduceSimple
  (MR.uagMapAllGatherEach (MR.Unpack unpack)
                          (assignFrame @ks @as)
                          (MR.gatherMonoid @_ @MM.MonoidalMap process)
  )
  (reduceAndAddKey extract)
