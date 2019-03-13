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
module Frames.MapReduce
  ( module Control.MapReduce
  , aggregateMonoidalF
  , assignFrame
  , reduceAndAddKey
  , foldAndAddKey
  , gatherRecordList
  , gatherRecordFrame
  , mapReduceFrame
  )
where

import qualified Control.MapReduce             as MR
import           Control.MapReduce

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
assignFrame = MR.assign (F.rcast @ks) (F.rcast @cs)

reduceAndAddKey
  :: FI.RecVec ((ks V.++ cs))
  => (h x -> F.Record cs)
  -> MR.Reduce 'Nothing (F.Record ks) h x (F.FrameRec (ks V.++ cs))
reduceAndAddKey process =
  fmap (F.toFrame . pure @[]) $ MR.processAndRelabel process V.rappend

foldAndAddKey
  :: (Foldable h, FI.RecVec ((ks V.++ cs)))
  => FL.Fold x (F.Record cs)
  -> MR.Reduce 'Nothing (F.Record ks) h x (F.FrameRec (ks V.++ cs))
foldAndAddKey fld =
  fmap (F.toFrame . pure @[]) $ MR.foldAndRelabel fld V.rappend

-- g is the type we unpack to.  Identity, Maybe, [] are all possible.
gatherRecordList
  :: (Functor g, Foldable g)
  => MR.GroupMap ec mt (F.Record ks) [F.Record cs]
  -> MR.Gather ec g mt (F.Record ks) (F.Record cs) [F.Record cs]
gatherRecordList = MR.gatherLists

gatherRecordFrame
  :: (Functor g, Foldable g, Functor (mt (F.Record ks)), FI.RecVec cs)
  => MR.GroupMap ec mt (F.Record ks) (F.FrameRec cs)
  -> MR.Gather ec g mt (F.Record ks) (F.Record cs) (F.FrameRec cs)
gatherRecordFrame = MR.gatherApplicativeMonoid

mapReduceFrame
  :: ( ec e
     , Functor g
     , Functor (MR.MapFoldT mm x)
     , Monoid e
     , Monoid (mt (F.Record ks) [F.Record cs])
     , Foldable g
     )
  => MR.GroupMap ec mt (F.Record ks) [F.Record cs]
  -> MR.Unpack mm g x y
  -> MR.Assign keyC (F.Record ks) y (F.Record cs)
  -> MR.Reduce mm (F.Record ks) [] (F.Record cs) e
  -> MR.MapFoldT mm x e
mapReduceFrame gm unpack assign reduce = MR.mapGatherReduceFold
  (MR.uagMapAllGatherEachFold gm unpack assign (gatherRecordList gm))
  reduce

-- this is slightly too general to use the above
-- if h x ~ [F.Record as], then these are equivalent
aggregateMonoidalF
  :: forall ks rs as h x cs f g
   . ( ks F.⊆ as
     , as F.⊆ as
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Functor f
     , Foldable h
     , Monoid (h x)
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (F.Record as -> h x)
  -> (h x -> F.Record cs)
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalF unpack process extract = MR.mapGatherReduceFold
  (MR.uagMapAllGatherEachFold MR.groupMap
                              (MR.Unpack unpack)
                              (assignFrame @ks @as)
                              (MR.gatherMonoid MR.groupMap process)
  )
  (reduceAndAddKey extract)
