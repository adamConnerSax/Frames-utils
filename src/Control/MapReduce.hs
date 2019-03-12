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
--{-# LANGUAGE UndecidableSuperClasses #-}
--{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE InstanceSigs #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Control.MapReduce where

import qualified Control.Foldl                 as FL
import           Data.Functor.Identity          ( Identity(Identity) )
import qualified Data.Foldable                 as Foldable
import qualified Data.List                     as List
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

--import           Control.Arrow                  ( second )
-- Let's try types!!

-- | MapReduce is, if you take away all of the details, just a function from one piece of data to another.  We label that here just for completeness
type MapReduceT a b = a -> b

-- | It's "MapReduce" because we perform that function in two steps, one which groups and transforms/subsets
-- and another which reassembles that grouped data into the desired result.
-- This division fits many data analysis tasks.  And creates clear opportunities for taking advantage
-- of shared work and parallel/concurrent algorithms

type MapStepT f k a c = a -> f k c -- this should be a profunctor, contravariant in a and covariant in c
type ReduceStepT f k c b = f k c -> b -- this should be a profunctor, contravariant in c and covariant in b

mapReduce :: MapStepT f k a c -> ReduceStepT f k c b -> MapReduceT a b
mapReduce ms rs = rs . ms


-- | We can share work in multiple places:
-- 1. If `a ~ Foldable f => f x`, we can write the MapReduce step as a Fold x b, and any other MapReductions from a can be done in parallel.
-- 2. If there are multiple interesting reductions from the same mapping, we can share the result of the map step.

-- | Similarly, we can do various parts in parallel:
-- 1.  If `a ~ Foldable f => f x`, we can do a || fold over the x's as long as we have some way of combining the resulting `f k c`
-- 2.  The reduce step often involves considerable work at each key, `k` and that work can be done in ||.

-- | We specialize to the case where f is a MonoidalMap (we could also choose Hash Map?  Which is better?)

-- | Often we can further divide the map step into an "unpacking"--which might be a no-op
-- and then a grouping.  This makes it easier to see opportunities to parallelize/share.
-- In both types below, one strategy for making the parallelism simpler is to choose a monoidal container for h and f.
-- Then each thread can build its own unpacked or grouped output and then combine them at the end of the step.

type UnpackStepT a d = a -> d -- e.g., melting rows or a filter which might take a ~ [x] -> d ~ [Maybe y] 
type GroupStepT f k d c = d -> f k c -- e.g., [Maybe y] -> MonoidalMap k c

-- | Some unpack and group functions
data UnpackF g x y where
  UnpackF :: (x -> g y) -> UnpackF g x y

noUnpack :: UnpackF Identity x x
noUnpack = UnpackF Identity

data AssignF keyC k y c where
  AssignF :: keyC k => (y -> (k, c)) -> AssignF keyC k y c

assign :: keyC k => (y -> k) -> (y -> c) -> AssignF keyC k y c
assign getKey getCols = AssignF (\y -> (getKey y, getCols y))

class (Functor (m k), Monoid c, Monoid (m k c)) => GroupMap m k c where
  type KeyConstraint m :: Type -> Constraint
  fromFoldable :: Foldable g => g (k,c) -> m k c
  foldMapWithKey :: Monoid e => (k -> c -> e) -> m k c -> e

instance (Monoid c, Ord k) => GroupMap MM.MonoidalMap k c where
  type KeyConstraint MM.MonoidalMap = Ord
  fromFoldable = MM.fromListWith (<>) . FL.fold FL.list
  foldMapWithKey = MM.foldMapWithKey

instance (Monoid c, Hashable k, Eq k) => GroupMap HMM.MonoidalHashMap k c where
  type KeyConstraint HMM.MonoidalHashMap = Hashable
  fromFoldable = HMM.fromList . FL.fold FL.list
  foldMapWithKey f = foldMap (uncurry f) . HMM.toList

data Group g mt k c d where
  Group :: GroupMap mt k d => (g (k, c) -> mt k d) -> Group g mt k c d

groupMonoid
  :: (Functor g, Foldable g, Monoid d, GroupMap mt k d)
  => (c -> d)
  -> Group g mt k c d
groupMonoid toMonoid = Group $ fromFoldable . fmap (\(k, c) -> (k, toMonoid c))

groupToApplicativeMonoid
  :: (Functor g, Foldable g, GroupMap mt k (h c), Applicative h)
  => Group g mt k c (h c)
groupToApplicativeMonoid = groupMonoid pure

groupToLists :: (Functor g, Foldable g, GroupMap mt k [c]) => Group g mt k c [c]
groupToLists = groupToApplicativeMonoid

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
  MapStepF hx_fab <*> MapStepF hx_a = MapStepF $ \hx -> (hx_fab hx) (hx_a hx)
  MapStepFold fab <*> MapStepF hx_a = MapStepF $ \hx -> (FL.fold fab hx) (hx_a hx)
  MapStepF hx_fab <*> MapStepFold fa = MapStepF $ \hx -> (hx_fab hx) (FL.fold fa hx)

mapStep :: Foldable h => MapStep h x q -> h x -> q
mapStep (MapStepF    g) = g
mapStep (MapStepFold f) = FL.fold f

-- Fundamentally 3 ways to do this:
-- group . fmap . <> . fmap : "MapEach "
-- group . <> . fmap . fmap : "MapAllGroupOnce" 
--  <> . group . fmap . fmap : "MapAllGroupEach"

-- But we can introduce parallelism at any fmap or foldMap step, though not in a Contro.Foldl fold

uagMapEach
  :: (Functor h, Foldable h, Monoid (g y), Functor g)
  => UnpackF g x y
  -> AssignF keyC k y c
  -> Group g mt k c d
  -> MapStep h x (mt k d)
uagMapEach (UnpackF unpack) (AssignF assign) (Group group) =
  MapStepF $ group . fmap assign . foldMap id . fmap unpack

uagMapAllGroupOnce
  :: (Functor h, Foldable h, Functor g, Monoid (g (k, c)))
  => UnpackF g x y
  -> AssignF keyC k y c
  -> Group g mt k c d
  -> MapStep h x (mt k d)
uagMapAllGroupOnce (UnpackF unpack) (AssignF assign) (Group group) =
  MapStepF $ group . foldMap id . fmap (fmap assign . unpack)

uagMapAllGroupEach
  :: (Functor f, Functor h, Foldable h, Functor g)
  => UnpackF g x y
  -> AssignF keyC k y c
  -> Group g mt k c d
  -> MapStep h x (mt k d)
uagMapAllGroupEach (UnpackF unpack) (AssignF assign) (Group group) =
  MapStepF $ foldMap id . fmap (group . fmap assign . unpack)

-- we can "replace" each foldMap id with an FL.Fold FL.mconcat to get Control.Foldl Foldl
-- There is less opportunity to parallelize here but more to share work
uagMapEachFold
  :: (Functor h, Foldable h, Monoid (g y), Functor g)
  => UnpackF g x y
  -> AssignF keyC k y c
  -> Group g mt k c d
  -> MapStep h x (mt k d)
uagMapEachFold (UnpackF unpack) (AssignF assign) (Group group) =
  MapStepFold $ P.dimap unpack (group . fmap assign) FL.mconcat

uagMapAllGroupOnceFold
  :: (Functor h, Foldable h, Monoid (g (k, c)), Functor g)
  => UnpackF g x y
  -> AssignF keyC k y c
  -> Group g mt k c d
  -> MapStep h x (mt k d)
uagMapAllGroupOnceFold (UnpackF unpack) (AssignF assign) (Group group) =
  MapStepFold $ P.dimap (fmap assign . unpack) group FL.mconcat

uagMapAllGroupEachFold
  :: (Functor f, Functor h, Foldable h, Functor g)
  => UnpackF g x y
  -> AssignF keyC k y c
  -> Group g mt k c d
  -> MapStep h x (mt k d)
uagMapAllGroupEachFold (UnpackF unpack) (AssignF assign) (Group group) =
  MapStepFold $ FL.premap (group . fmap assign . unpack) FL.mconcat

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

mapReduce1
  :: (GroupMap mt k d, Foldable h, Monoid e)
  => MapStep h x (mt k d)
  -> ReduceOne k d e
  -> h x
  -> e
mapReduce1 ms (ReduceOne reduceOne) = foldMapWithKey reduceOne . mapStep ms

