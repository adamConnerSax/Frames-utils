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
  , parReduceGathererOrd
  , parReduceGathererHashableL
  , parReduceGathererHashableS
  , parReduceGathererMM
  , parFoldMonoid
  , parFoldMonoidDC
  , parallelMapReduce
  )
where

import           Control.MapReduce
import qualified Control.MapReduce             as MR

import qualified Control.Foldl                 as FL
import qualified Data.Foldable                 as F
import qualified Data.List                     as L
import qualified Data.List.Split               as L
import qualified Data.Sequence                 as Seq
import qualified Data.Map                      as ML
import qualified Data.Map.Strict               as MS
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Strict           as HMS
import qualified Data.HashMap.Strict           as HML
import qualified Data.Map.Monoidal.Strict      as MMS
import qualified Data.Map.Monoidal             as MML
import qualified Data.HashMap.Monoidal         as HMM
import           Data.Monoid                    ( Monoid
                                                , mconcat
                                                )

import qualified Control.Parallel.Strategies   as PS

-- | Use these in a call to Control.MapReduce.mapReduceFold
-- for parallel reduction.  For parallel folding after reduction, choose a parFoldMonoid f
parReduceGathererOrd'
  :: (Semigroup d, Ord k)
  => (forall e f . (Monoid e, Foldable f) => f e -> e)  -- use `foldMap id` for sequential folding or use a parallel foldMonoid from below
  -> (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererOrd' foldMonoid = MR.sequenceGatherer
  (MS.fromListWith (<>))
  (\f -> foldMonoid . parMapEach (uncurry f) . MS.toList)
  (\f -> fmap (PS.withStrategy (PS.parTraversable PS.rdeepseq)) -- deepseq each one in ||
    . MS.traverseWithKey f
  )
  foldMonoid

parReduceGathererOrd
  :: (Semigroup d, Ord k)
  => (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererOrd = parReduceGathererOrd' (foldMap id)

parReduceGathererHashableS'
  :: (Semigroup d, Hashable k, Eq k)
  => (forall e f . (Monoid e, Foldable f) => f e -> e)
  -> (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererHashableS' foldMonoid = MR.sequenceGatherer
  (HMS.fromListWith (<>))
  (\doOne -> foldMonoid . parMapEach (uncurry doOne) . HMS.toList)
  (\doOneM -> fmap (PS.withStrategy (PS.parTraversable PS.rdeepseq)) -- deepseq each one in ||
    . HMS.traverseWithKey doOneM
  )
  foldMonoid

parReduceGathererHashableS
  :: (Semigroup d, Hashable k, Eq k)
  => (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererHashableS = parReduceGathererHashableS' (foldMap id)


parReduceGathererHashableL'
  :: (Semigroup d, Hashable k, Eq k)
  => (forall e f . (Monoid e, Foldable f) => f e -> e)
  -> (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererHashableL' foldMonoid = MR.sequenceGatherer
  (HML.fromListWith (<>))
  (\doOne -> foldMonoid . parMapEach (uncurry doOne) . HML.toList)
  (\doOneM -> fmap (PS.withStrategy (PS.parTraversable PS.rdeepseq)) -- deepseq each one in ||
    . HML.traverseWithKey doOneM
  )
  foldMonoid


parReduceGathererHashableL
  :: (Semigroup d, Hashable k, Eq k)
  => (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererHashableL = parReduceGathererHashableL' (foldMap id)


{-

parReduceGathererHashableL
  :: (Semigroup d, Hashable k, Eq k)
  => (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererHashableL toSG
  = let seqToMap =
          HML.fromListWith (<>)
            . fmap (\(k, c) -> (k, toSG c))
            . FL.fold FL.list
    in
      Gatherer
        (FL.fold (FL.Fold (\s x -> s Seq.|> x) Seq.empty id))
        (\doOne ->
          foldMap id . parMapEach (uncurry doOne) . HML.toList . seqToMap
        )
        (\doOneM ->
          fmap (foldMap id)
            . fmap (PS.withStrategy (PS.parTraversable PS.rdeepseq)) -- deepseq each one in ||
            . HML.traverseWithKey doOneM -- hopefully, this just lazily creates thunks
            . seqToMap
        )

-}
parReduceGathererMM
  :: (Semigroup d, Ord k)
  => (c -> d)
  -> MR.Gatherer PS.NFData (MML.MonoidalMap k d) k c d
parReduceGathererMM toSG = Gatherer
  (MML.fromListWith (<>) . fmap (\(k, c) -> (k, toSG c)) . FL.fold FL.list)
  (\doOne -> foldMap id . parMapEach (uncurry doOne) . MML.toList)
  (\doOneM ->
    fmap (foldMap id)
      . fmap (PS.withStrategy (PS.parTraversable PS.rdeepseq)) -- deepseq each one in ||
      . MML.traverseWithKey doOneM -- hopefully, this just lazily creates thunks
  )


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


-- | like `foldMap id` but does sublists in || first
parFoldMonoid'
  :: forall f m . (Foldable f, Monoid m, PS.NFData m) => Int -> f m -> m
parFoldMonoid' threadsToUse fm =
  let asList  = F.toList fm
      chunked = L.divvy threadsToUse threadsToUse asList
  in  mconcat $ parMapEach mconcat $ chunked


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



-- | split input into n chunks, spark each separately
-- use a || foldMap to gather
-- optionally use a || reduction scheme.  This is controlled by the gatherer
parallelMapReduce
  :: forall h x q gt k y z ce e
   . ( Monoid e
     , ce e
     , PS.NFData gt
     , Functor (MR.MapFoldT 'Nothing x)
     , Foldable q
     , Foldable h
     , Monoid gt
     )
  => Int -- 1000 seems optimal on my current machine
  -> Int
  -> Gatherer ce gt k y (h z)
  -> MapStep 'Nothing x gt
  -> Reduce 'Nothing k h z e
  -> q x
  -> e
parallelMapReduce oneSparkMax numThreads gatherer mapStep reduceStep qx
  = let
      chunkedH :: [[x]] = L.divvy numThreads numThreads $ FL.fold FL.list qx -- list divvied into n sublists
      mapped :: [gt]    = parMapEach (FL.fold (mapFold mapStep)) chunkedH -- list of n gt
      merged :: gt      = parFoldMonoid oneSparkMax mapped
      reduced           = case reduceStep of
        Reduce f -> gFoldMapWithKey gatherer f merged
        ReduceFold f ->
          gFoldMapWithKey gatherer (\k hx -> FL.fold (f k) hx) merged
    in
      reduced
{-# INLINABLE parallelMapReduce #-}

