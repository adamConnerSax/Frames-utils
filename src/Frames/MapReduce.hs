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
  , unpackGoodRows
  , aggregateMonoidalF
  , assignKeysAndData
  , assignKeys
  , splitOnKeys
  , reduceAndAddKey
  , foldAndAddKey
  , makeRecsWithKey
--  , gatherRecordList
--  , gatherRecordFrame
  , mapReduceGF
  , mapRListF
  )
where

import qualified Control.MapReduce             as MR
import           Control.MapReduce

import qualified Control.Foldl                 as FL
import           Data.Functor.Identity          ( Identity(Identity) )
import qualified Data.Map.Monoidal             as MM
import qualified Data.HashMap.Monoidal         as HMM
import qualified Data.Hashable                 as Hash
import           Data.Monoid                    ( (<>)
                                                , Monoid(..)
                                                )
import           Data.Hashable                  ( Hashable )
import           Data.Kind                      ( Type
                                                , Constraint
                                                )
import qualified Data.Profunctor               as P
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V


instance Hash.Hashable (F.Record '[]) where
  hash = const 0
  {-# INLINABLE hash #-}
  hashWithSalt s = const s -- TODO: this seems BAD!!
  {-# INLINABLE hashWithSalt #-}

instance (V.KnownField t, Hash.Hashable (V.Snd t), Hash.Hashable (F.Record rs), rs F.⊆ (t ': rs)) => Hash.Hashable (F.Record (t ': rs)) where
  hashWithSalt s r = s `Hash.hashWithSalt` (F.rgetField @t r) `Hash.hashWithSalt` (F.rcast @rs r)
  {-# INLINABLE hashWithSalt #-}

unpackGoodRows
  :: forall cs rs
   . (cs F.⊆ rs)
  => MR.Unpack
       'Nothing
       Maybe
       (F.Rec (Maybe F.:. F.ElField) rs)
       (F.Record cs)
unpackGoodRows = MR.Unpack $ F.recMaybe . F.rcast

-- | Assign both keys and data cols.  Uses type applications to specify if they cannot be inferred.  Keys usually can't.  Data often can be from the functions
-- that follow
assignKeysAndData
  :: forall ks cs rs
   . (ks F.⊆ rs, cs F.⊆ rs)
  => MR.Assign (F.Record ks) (F.Record rs) (F.Record cs)
assignKeysAndData = MR.assign (F.rcast @ks) (F.rcast @cs)
{-# INLINABLE assignKeysAndData #-}

-- | Assign keys and leave all columns, including the keys, in the data passed to gather and reduce
assignKeys
  :: forall ks rs
   . (ks F.⊆ rs)
  => MR.Assign (F.Record ks) (F.Record rs) (F.Record rs)
assignKeys = MR.assign (F.rcast @ks) id
{-# INLINABLE assignKeys #-}

-- | Assign keys and leave the rest of the columns, excluding the keys, in the data passed to gather and reduce
splitOnKeys
  :: forall ks rs cs
   . (ks F.⊆ rs, cs ~ F.RDeleteAll ks rs, cs F.⊆ rs)
  => MR.Assign (F.Record ks) (F.Record rs) (F.Record cs)
splitOnKeys = assignKeysAndData @ks @cs
{-# INLINABLE splitOnKeys #-}

-- | 
reduceAndAddKey
  :: forall ks cs h x
   . FI.RecVec ((ks V.++ cs))
  => (h x -> F.Record cs)
  -> MR.Reduce 'Nothing (F.Record ks) h x (F.FrameRec (ks V.++ cs))
reduceAndAddKey process =
  fmap (F.toFrame . pure @[]) $ MR.processAndRelabel process V.rappend
{-# INLINABLE reduceAndAddKey #-}

foldAndAddKey
  :: (Foldable h, FI.RecVec ((ks V.++ cs)))
  => FL.Fold x (F.Record cs)
  -> MR.Reduce 'Nothing (F.Record ks) h x (F.FrameRec (ks V.++ cs))
foldAndAddKey fld =
  fmap (F.toFrame . pure @[]) $ MR.foldAndRelabel fld V.rappend  -- is Frame a reasonably fast thing for many appends?
{-# INLINABLE foldAndAddKey #-}

makeRecsWithKey
  :: (Functor g, Foldable g, (FI.RecVec (ks V.++ as)))
  => (y -> F.Record as)
  -> MR.Reduce mm (F.Record ks) h x (g y)
  -> MR.Reduce mm (F.Record ks) h x (F.FrameRec (ks V.++ as))
makeRecsWithKey makeRec reduceToY = fmap F.toFrame
  $ MR.mapReduceWithKey addKey reduceToY
  where addKey k = fmap (V.rappend k . makeRec)


mapReduceGF
  :: ( ec e
     , Functor g
     , Functor (MR.MapFoldT mm x)
     , Monoid e
     , Monoid gt
     , Foldable g
     )
  => MR.Gatherer ec gt (F.Record ks) (F.Record cs) [F.Record cs]
  -> MR.Unpack mm g x y
  -> MR.Assign (F.Record ks) y (F.Record cs)
  -> MR.Reduce mm (F.Record ks) [] (F.Record cs) e
  -> MR.MapFoldT mm x e
mapReduceGF frameGatherer unpacker assigner reducer = MR.mapGatherReduceFold
  (MR.uagMapAllGatherEachFold frameGatherer unpacker assigner)
  reducer

mapRListF
  :: ( Functor g
     , Functor (MR.MapFoldT mm x)
     , Monoid e
     , Foldable g
     , Hashable (F.Record ks)
     , Eq (F.Record ks)
     )
  => MR.Unpack mm g x y
  -> MR.Assign (F.Record ks) y (F.Record cs)
  -> MR.Reduce mm (F.Record ks) [] (F.Record cs) e
  -> MR.MapFoldT mm x e
mapRListF = mapReduceGF (MR.defaultHashableGatherer pure)


-- this is slightly too general to use the above
-- if h x ~ [F.Record as], then these are equivalent
aggregateMonoidalF
  :: forall ks rs as h x cs f g
   . ( ks F.⊆ as
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
  (MR.uagMapAllGatherEachFold (MR.gathererSeqToStrictMap process)
                              (MR.Unpack unpack)
                              (assignKeys @ks)
  )
  (reduceAndAddKey extract)
