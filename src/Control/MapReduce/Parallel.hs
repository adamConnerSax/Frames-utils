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
{-# LANGUAGE InstanceSigs #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Control.MapReduce.Parallel
  ( module Control.MapReduce
  , parReduceGroupMap
  , parFoldGroupMap
  , parAllGroupMap
  , parFoldMonoid
  , parFoldMonoidDC
  )
where

import           Control.MapReduce
import qualified Control.MapReduce             as MR

import qualified Control.Foldl                 as FL
import qualified Data.Foldable                 as F
import qualified Data.List                     as L
import qualified Data.List.Split               as L
import qualified Data.Map.Monoidal             as MM
import qualified Data.HashMap.Monoidal         as HMM
import           Data.Monoid                    ( Monoid )

import qualified Control.Parallel.Strategies   as PS

-- | Use these group maps when building a Control.MapReduce.Gather or in a call to Control.MapReduce.mapReduceFold

parReduceGroupMap
  :: (Semigroup c, Ord k) => GroupMap PS.NFData MM.MonoidalMap k c
parReduceGroupMap = GroupMap
  (MM.fromListWith (<>) . FL.fold FL.list)
  (\doOne -> foldMap id . parMapEach (uncurry doOne) . MM.toList)
  (\doOneM ->
    fmap (foldMap id)
      . fmap (PS.withStrategy (PS.parTraversable PS.rdeepseq)) -- deepseq each one in ||
      . MM.traverseWithKey doOneM -- hopefully, this just lazily creates thunks
  )
  MM.toList

parFoldGroupMap
  :: forall c k
   . (Semigroup c, Ord k, PS.NFData c)
  => (forall x f . (Monoid x, Foldable f, PS.NFData x) => f x -> x)
  -> GroupMap PS.NFData MM.MonoidalMap k c
parFoldGroupMap foldMonoid = GroupMap
  (MM.fromListWith (<>) . FL.fold FL.list)
  (\doOne -> foldMonoid . fmap snd . MM.toList . MM.mapWithKey doOne)
  (\doOneM ->
    fmap (foldMonoid . fmap snd . MM.toList) . MM.traverseWithKey doOneM
  )
  MM.toList

parAllGroupMap
  :: (Semigroup c, Ord k)
  => (forall x f . (Monoid x, Foldable f, PS.NFData x) => f x -> x)
  -> GroupMap PS.NFData MM.MonoidalMap k c
parAllGroupMap foldMonoid = GroupMap
  (MM.fromListWith (<>) . FL.fold FL.list)
  (\doOne -> foldMonoid . parMapEach (uncurry doOne) . MM.toList)
  (\doOneM ->
    fmap foldMonoid
      . fmap (PS.withStrategy (PS.parTraversable PS.rdeepseq))
      . MM.traverseWithKey doOneM
  )
  MM.toList


parMapEach :: PS.NFData b => (a -> b) -> [a] -> [b]
parMapEach = PS.parMap (PS.rparWith PS.rdeepseq)

{-
parTraverseEach
  :: forall t f a b
   . (PS.NFData (f b), Traversable t, Applicative f)
  => (a -> f b)
  -> t a
  -> f (t b)
parTraverseEach f =
  sequenceA . PS.withStrategy (PS.parTraversable @t PS.rdeepseq) . fmap f
-}

-- | like `foldMap id` but does each chunk of chunkSize in || until list is shorter than chunkSize
parFoldMonoid
  :: forall f m . (Foldable f, Monoid m, PS.NFData m) => Int -> f m -> m
parFoldMonoid chunkSize fm = go (F.toList fm)
 where
  go lm = case L.length lm > chunkSize of
    True ->
      go
        ( PS.parMap (PS.rparWith PS.rdeepseq) (foldMap id)
        $ L.chunksOf chunkSize lm
        )
    False -> foldMap id lm


parFoldMonoidDC :: (Monoid b, PS.NFData b, Foldable h) => Int -> h b -> b
parFoldMonoidDC chunkSize = parFold chunkSize FL.mconcat

-- | do a Control.Foldl fold in Parallel using a divide and conquer strategy
-- we pay (?) to convert to a list, though this may be fused away.
-- We divide the list in half until the chunks are below chunkSize, then we fold and use mappend to build back up
parFold :: (Monoid b, Foldable h) => Int -> FL.Fold a b -> h a -> b
parFold chunkSize fl@(FL.Fold step begin done) ha = divConq
  (FL.fold fl)
  (FL.fold FL.list ha)
  (\x -> L.length x < chunkSize)
  (<>)
  (\l -> Just $ splitAt (L.length l `div` 2) l)

-- | This divide and conquer is from <https://simonmar.github.io/bib/papers/strategies.pdf>
divConq
  :: (a -> b) -- compute the result
  -> a -- the value
  -> (a -> Bool) -- par threshold reached?
  -> (b -> b -> b) -- combine results
  -> (a -> Maybe (a, a)) -- divide
  -> b
divConq f arg threshold conquer divide = go arg
 where
  go arg = case divide arg of
    Nothing       -> f arg
    Just (l0, r0) -> conquer l1 r1 `PS.using` strat
     where
      l1 = go l0
      r1 = go r0
      strat x = do
        r l1
        r r1
        return x
       where
        r | threshold arg = PS.rseq
          | otherwise     = PS.rpar




{-
groupHashMap
  :: (Hashable k, Eq k, Semigroup c) => GroupMap HMM.MonoidalHashMap k c
groupHashMap = GroupMap
  (HMM.fromList . FL.fold FL.list)
  (\f -> foldMap (uncurry f) . HMM.toList)
  (\doOneM -> fmap (foldMap id) . traverse (uncurry doOneM) . HMM.toList)
  HMM.toList
-}


{-
-- | Parallel strategies for mapReduce
-- `foldMonoid` could be `foldMap id` or `parFoldMonoid combineChunkSize` or `parFoldMonoiDC combineChunkSize`
-- `doReduce` could be `fmap` or `parMapEach` etc.
parMapReduceFold
  :: (MR.GroupMap mt k d, Monoid e, PS.NFData e)
  => ([e] -> e) -- foldMap id
  -> (((k, d) -> e) -> [(k, d)] -> [e])
  -> MR.MapStep x (mt k d)
  -> MR.ReduceOne k d e
  -> FL.Fold x e
parMapReduceFold foldMonoid doReduce ms (MR.ReduceOne reduceOne) = fmap
  (parReduce reduceOne)
  (MR.mapFold ms)
  where parReduce f = foldMonoid . doReduce (uncurry reduceOne) . MR.toList
-}
