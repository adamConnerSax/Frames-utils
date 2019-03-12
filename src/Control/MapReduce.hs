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
module Control.MapReduce where

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

-- | `Unpack` is for melting rows or filtering, e.g.
data Unpack g x y where
  Unpack :: (x -> g y) -> Unpack g x y

noUnpack :: Unpack Identity x x
noUnpack = Unpack Identity

filter :: (x -> Bool) -> Unpack Maybe x x
filter t = Unpack $ \x -> if t x then Just x else Nothing

-- | `Assign` associates a key with a given item/row
-- It takes an extra argument for the key constraint type
data Assign keyC k y c where
  Assign :: keyC k => (y -> (k, c)) -> Assign keyC k y c

assign :: keyC k => (y -> k) -> (y -> c) -> Assign keyC k y c
assign getKey getCols = Assign (\y -> (getKey y, getCols y))

class (Functor (m k), Monoid c, Monoid (m k c)) => GroupMap m k c where
  type KeyConstraint m :: Type -> Constraint
  fromFoldable :: Foldable g => g (k,c) -> m k c
  foldMapWithKey :: Monoid e => (k -> c -> e) -> m k c -> e
  toList :: m k c -> [(k,c)]

instance (Monoid c, Ord k) => GroupMap MM.MonoidalMap k c where
  type KeyConstraint MM.MonoidalMap = Ord
  fromFoldable = MM.fromListWith (<>) . FL.fold FL.list
  foldMapWithKey = MM.foldMapWithKey
  toList = MM.toList

instance (Monoid c, Hashable k, Eq k) => GroupMap HMM.MonoidalHashMap k c where
  type KeyConstraint HMM.MonoidalHashMap = Hashable
  fromFoldable = HMM.fromList . FL.fold FL.list
  foldMapWithKey f = foldMap (uncurry f) . HMM.toList
  toList = HMM.toList

-- | `Gather` assembles items with the same key
data Gather g mt k c d where
  Gather :: GroupMap mt k d => (g (k, c) -> mt k d) -> Gather g mt k c d

gatherMonoid
  :: forall g mt k c d
   . (Functor g, Foldable g, Monoid d, GroupMap mt k d)
  => (c -> d)
  -> Gather g mt k c d
gatherMonoid toMonoid =
  Gather $ fromFoldable . fmap (\(k, c) -> (k, toMonoid c))

gatherApplicativeMonoid
  :: forall g h mt k c
   . (Functor g, Foldable g, GroupMap mt k (h c), Applicative h)
  => Gather g mt k c (h c)
gatherApplicativeMonoid = gatherMonoid pure

gatherLists
  :: forall g mt k c
   . (Functor g, Foldable g, GroupMap mt k [c])
  => Gather g mt k c [c]
gatherLists = gatherApplicativeMonoid

-- | `MapStep` is the map part of MapReduce
-- it will be a combination of Unpack, Assign and Gather
-- they can be combined various ways and which one is best depends on the
-- relative complexity of the various steps
data MapStep h x q  where -- q ~ f k d
  MapStepF :: (h x -> q) -> MapStep h x q
  MapStepFold :: Foldable h => FL.Fold x q -> MapStep h x q

instance Functor (MapStep h x) where
  fmap h (MapStepF g) = MapStepF $ h . g
  fmap h (MapStepFold fld) = MapStepFold $ fmap h fld

instance Functor h => P.Profunctor (MapStep h) where
  dimap l r (MapStepF g) = MapStepF $ r . g . fmap l
  dimap l r (MapStepFold fld) = MapStepFold $ P.dimap l r fld

-- NB: we can only share the fold over h x if both inputs are folds
instance Foldable h => Applicative (MapStep h x) where
  pure y = MapStepFold $ pure y
  MapStepFold fab <*> MapStepFold fa = MapStepFold $ fab <*> fa
  MapStepF hx_fab <*> MapStepF hx_a  = MapStepF $ \hx -> (hx_fab hx) (hx_a hx)
  MapStepFold fab <*> MapStepF hx_a  = MapStepF $ \hx -> (FL.fold fab hx) (hx_a hx)
  MapStepF hx_fab <*> MapStepFold fa = MapStepF $ \hx -> (hx_fab hx) (FL.fold fa hx)

mapStep :: Foldable h => MapStep h x q -> h x -> q
mapStep (MapStepF    g) = g
mapStep (MapStepFold f) = FL.fold f

-- Fundamentally 3 ways to combine these operations to produce a MapStep:
-- group . fmap . <> . fmap : "MapEach "
-- group . <> . fmap . fmap : "MapAllGroupOnce" 
--  <> . group . fmap . fmap : "MapAllGroupEach"

-- NB: For the first 2, we need Monoid (g x), so g cannot be Identity or Maybe

uagMapEach
  :: (Functor h, Foldable h, Monoid (g y), Functor g)
  => Unpack g x y
  -> Assign keyC k y c
  -> Gather g mt k c d
  -> MapStep h x (mt k d)
uagMapEach (Unpack unpack) (Assign assign) (Gather gather) =
  MapStepF $ gather . fmap assign . foldMap id . fmap unpack

uagMapAllGatherOnce
  :: (Functor h, Foldable h, Functor g, Monoid (g (k, c)))
  => Unpack g x y
  -> Assign keyC k y c
  -> Gather g mt k c d
  -> MapStep h x (mt k d)
uagMapAllGatherOnce (Unpack unpack) (Assign assign) (Gather gather) =
  MapStepF $ gather . foldMap id . fmap (fmap assign . unpack)

uagMapAllGatherEach
  :: (Functor h, Foldable h, Functor g)
  => Unpack g x y
  -> Assign keyC k y c
  -> Gather g mt k c d
  -> MapStep h x (mt k d)
uagMapAllGatherEach (Unpack unpack) (Assign assign) (Gather gather) =
  MapStepF $ foldMap id . fmap (gather . fmap assign . unpack)

-- we can "replace" each foldMap id with an FL.Fold FL.mconcat to get Control.Foldl Foldl
-- There is less opportunity to parallelize here but more to share work
uagMapEachFold
  :: (Functor h, Foldable h, Monoid (g y), Functor g)
  => Unpack g x y
  -> Assign keyC k y c
  -> Gather g mt k c d
  -> MapStep h x (mt k d)
uagMapEachFold (Unpack unpack) (Assign assign) (Gather gather) =
  MapStepFold $ P.dimap unpack (gather . fmap assign) FL.mconcat

uagMapAllGatherOnceFold
  :: (Functor h, Foldable h, Monoid (g (k, c)), Functor g)
  => Unpack g x y
  -> Assign keyC k y c
  -> Gather g mt k c d
  -> MapStep h x (mt k d)
uagMapAllGatherOnceFold (Unpack unpack) (Assign assign) (Gather gather) =
  MapStepFold $ P.dimap (fmap assign . unpack) gather FL.mconcat

uagMapAllGatherEachFold
  :: (Functor h, Foldable h, Functor g)
  => Unpack g x y
  -> Assign keyC k y c
  -> Gather g mt k c d
  -> MapStep h x (mt k d)
uagMapAllGatherEachFold (Unpack unpack) (Assign assign) (Gather gather) =
  MapStepFold $ FL.premap (gather . fmap assign . unpack) FL.mconcat

data ReduceOne k d e where
  ReduceOne :: (k -> d -> e) -> ReduceOne k d e

instance Functor (ReduceOne k d) where
  fmap f (ReduceOne g) = ReduceOne $ \k -> f . g k

instance P.Profunctor (ReduceOne k) where
  dimap l r (ReduceOne g)  = ReduceOne $ \k -> P.dimap l r (g k)

instance Applicative (ReduceOne k d) where
  pure x = ReduceOne $ \k -> pure x
  ReduceOne r1 <*> ReduceOne r2 = ReduceOne $ \k -> r1 k <*> r2 k

reduceSimple :: Monoid e => (k -> d -> x) -> (x -> e) -> ReduceOne k d e
reduceSimple reduceRow toMonoid = ReduceOne $ \k -> toMonoid . reduceRow k

reduceList :: (k -> d -> x) -> ReduceOne k d [x]
reduceList f = reduceSimple f pure

processAndRelabel :: (d -> x) -> (k -> x -> y) -> ReduceOne k d [y]
processAndRelabel process relabel = reduceList (\k d -> relabel k (process d))

mapReduceSimple
  :: (GroupMap mt k d, Foldable h, Monoid e)
  => MapStep h x (mt k d)
  -> ReduceOne k d e
  -> h x
  -> e
mapReduceSimple ms (ReduceOne reduceOne) =
  foldMapWithKey reduceOne . mapStep ms

