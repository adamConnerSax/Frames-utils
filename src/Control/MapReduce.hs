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
import           Control.Monad                  ( join )
import           Data.Functor.Identity          ( Identity(Identity) )
import qualified Data.Map.Strict               as MS
import qualified Data.Map.Strict               as ML
import qualified Data.HashMap.Lazy             as HML
import qualified Data.HashMap.Strict           as HMS
import qualified Data.Map.Monoidal             as MML
import qualified Data.Map.Monoidal.Strict      as MMS
import qualified Data.HashMap.Monoidal         as MHM
import           Data.Monoid                    ( (<>)
                                                , Monoid(..)
                                                , mconcat
                                                )
import qualified Data.Sequence                 as Seq
import qualified Data.Sort                     as Sort
import           Data.Hashable                  ( Hashable )
import           Data.Kind                      ( Type
                                                , Constraint
                                                )
import qualified Data.Profunctor               as P


-- | MapReduce as folds
-- This is all just wrapping around Control.Foldl so that it's easier to see the map-reduce structure
-- The Mapping step is broken into 3 parts:
-- 1. unpacking, which could include "melting" or filtering,
-- 2. assigning, which assigns a group to each unpacked item.  Could just be choosing a key column(s)
-- 3. gathering, which pulls together the items in each group
-- The reduce step is conceptually simpler, just requiring a function from the (key, grouped data) pair to the result monoid
-- but note that reduce could be as simple as combining the key with a single data row or some very complex function of the grouped data
-- E.g., reduce could itself be a map-reduce on the grouped data.
-- Since these are folds, we can share work by using the Applicative instance of MapStep (just the Applicative instance of Control.Foldl.Fold)
-- and we will loop over the data only once.
-- The Reduce type is also Applicative so there could be work sharing there as well:
-- e.g., if your `reduce :: (k -> d -> e)` has the form `reduce k :: FL.Fold d e` 

-- | I've made some choices.  For example, to use these simplified pieces to build your fold, you will have to
-- use either MonoidalMap or MonoidalHashMap as your storage for grouped items


-- | `Unpack` is for melting rows or filtering, e.g.
data Unpack (mm :: Maybe (Type -> Type)) g x y where
  Unpack :: (x -> g y) -> Unpack 'Nothing g x y
  UnpackM :: Monad m => (x -> m (g y)) -> Unpack ('Just m) g x y

noUnpack :: Unpack 'Nothing Identity x x
noUnpack = Unpack Identity

filter :: (x -> Bool) -> Unpack 'Nothing Maybe x x
filter t = Unpack $ \x -> if t x then Just x else Nothing
{-# INLINABLE filter #-}

-- | `Assign` associates a key with a given item/row
-- It takes an extra argument for the key constraint type
data Assign k y c where
  Assign :: (y -> (k, c)) -> Assign k y c

instance Functor (Assign k y) where
  fmap f (Assign g) = Assign (\y -> let (k,c) = g y in (k, f c))
  {-# INLINABLE fmap #-}

instance P.Profunctor (Assign k) where
  dimap l r (Assign g) = Assign (\z -> let (k,c) = g (l z) in (k, r c))
  {-# INLINABLE dimap #-}

assign :: forall k y c . (y -> k) -> (y -> c) -> Assign k y c
assign getKey getCols = Assign (\y -> (getKey y, getCols y))
{-# INLINABLE assign #-}

-- Not a class because for the same map we may want different methods of folding and traversing
-- E.g., for a parallel mapReduce
-- That is also the reason we carry an extra constraint.  We'll need (NFData e) but only for the parallel version

-- | takes (k,c) and then allows foldMapping over (k,d) where d is some way of combining c's.
-- d could be [c] or Seq c or c itself if c is a monoid
data Gatherer (eConst :: Type -> Constraint) gt k c d =
  Gatherer
  {
    foldInto :: (forall h. Foldable h => h (k,c) -> gt)
  , gFoldMapWithKey :: (forall e. (eConst e, Monoid e) => (k -> d -> e) -> gt -> e)
  , gFoldMapWithKeyM :: (forall e n. (eConst e, Monoid e, Monad n) => (k -> d -> n e) -> gt -> n e)
  }

-- | represent an empty constraint
class Empty x
instance Empty x

-- this one is fastest in simple tests.  And close to linear, prolly the expected N(ln N)
defaultHashableGatherer
  :: (Monoid d, Hashable k, Eq k)
  => (c -> d)
  -> Gatherer Empty (Seq.Seq (k, c)) k c d
defaultHashableGatherer = gathererSeqToStrictHashMap
{-# INLINABLE defaultHashableGatherer #-}

defaultOrdGatherer
  :: (Semigroup d, Ord k) => (c -> d) -> Gatherer Empty (Seq.Seq (k, c)) k c d
defaultOrdGatherer = gathererSeqToStrictMap
{-# INLINABLE defaultOrdGatherer #-}

-- | `MapStep` is the map part of MapReduce
-- it will be a combination of Unpack, Assign and Gather
-- they can be combined various ways and which one is best depends on the
-- relative complexity of the various steps
data MapStep (a :: Maybe (Type -> Type)) x q  where -- q ~ f k d
  MapStepFold :: FL.Fold x q -> MapStep 'Nothing x q
  MapStepFoldM :: Monad m => FL.FoldM m x q -> MapStep ('Just m) x q

mapStepGeneralize :: Monad m => MapStep 'Nothing x q -> MapStep ( 'Just m) x q
mapStepGeneralize (MapStepFold f) = MapStepFoldM $ FL.generalize f
{-# INLINABLE mapStepGeneralize #-}

instance Functor (MapStep mm x) where
  fmap h (MapStepFold fld) = MapStepFold $ fmap h fld
  fmap h (MapStepFoldM fld) = MapStepFoldM $ fmap h fld
  {-# INLINABLE fmap #-}

instance P.Profunctor (MapStep mm) where
  dimap l r (MapStepFold fld) = MapStepFold $ P.dimap l r fld
  dimap l r (MapStepFoldM fld) = MapStepFoldM $ P.dimap l r fld
  {-# INLINABLE dimap #-}

-- NB: we can only share the fold over h x if both inputs are folds
instance Applicative (MapStep 'Nothing x) where
  pure y = MapStepFold $ pure y
  {-# INLINABLE pure #-}
  MapStepFold x <*> MapStepFold y = MapStepFold $ x <*> y
  {-# INLINABLE (<*>) #-}

instance Monad m => Applicative (MapStep ('Just m) x) where
  pure y = MapStepFoldM $ pure y
  {-# INLINABLE pure #-}
  MapStepFoldM x <*> MapStepFoldM y = MapStepFoldM $ x <*> y
  {-# INLINABLE (<*>) #-}

-- we will export this to reserve the possibility of MapStep being something else internally
type family MapFoldT (mm :: Maybe (Type -> Type)) :: (Type -> Type -> Type) where
  MapFoldT 'Nothing = FL.Fold
  MapFoldT ('Just m) = FL.FoldM m


type family WrapMaybe (mm :: Maybe (Type -> Type)) (a :: Type) :: Type where
  WrapMaybe 'Nothing a = a
  WrapMaybe ('Just m) a = m a

mapFold :: MapStep mm x q -> MapFoldT mm x q
mapFold (MapStepFold  f) = f
mapFold (MapStepFoldM f) = f
{-# INLINABLE mapFold #-}

data MapGather mm x ec gt k c d = MapGather { gatherer :: Gatherer ec gt k c d, mapStep :: MapStep mm x gt }

-- Fundamentally 3 ways to combine these operations to produce a MapStep:
-- group . fmap . <> . fmap : "MapEach "
-- group . <> . fmap . fmap : "MapAllGroupOnce" 
--  <> . group . fmap . fmap : "MapAllGroupEach"
uagMapEachFold
  :: (Monoid (g y), Functor g, Foldable g)
  => Gatherer ec gt k c d
  -> Unpack mm g x y
  -> Assign k y c
  -> MapGather mm x ec gt k c d
uagMapEachFold gatherer unpacker (Assign assign) = MapGather gatherer mapStep
 where
  mapStep = case unpacker of
    Unpack unpack ->
      MapStepFold $ P.dimap unpack (foldInto gatherer . fmap assign) FL.mconcat
    UnpackM unpackM ->
      MapStepFoldM
        $ FL.premapM unpackM
        $ fmap (foldInto gatherer . fmap assign)
        $ FL.generalize FL.mconcat
{-# INLINABLE uagMapEachFold #-}

uagMapAllGatherOnceFold
  :: (Monoid (g (k, c)), Functor g, Foldable g)
  => Gatherer ec gt k c d
  -> Unpack mm g x y
  -> Assign k y c
  -> MapGather mm x ec gt k c d --MapStep mm x (mt k d)
uagMapAllGatherOnceFold gatherer unpacker (Assign assign) = MapGather
  gatherer
  mapStep
 where
  mapStep = case unpacker of
    Unpack unpack -> MapStepFold
      $ P.dimap (fmap assign . unpack) (foldInto gatherer) FL.mconcat
    UnpackM unpackM ->
      MapStepFoldM
        $ FL.premapM (fmap (fmap assign) . unpackM)
        $ fmap (foldInto gatherer)
        $ FL.generalize FL.mconcat
{-# INLINABLE uagMapAllGatherOnceFold #-}

uagMapAllGatherEachFold
  :: (Functor g, Foldable g, Monoid gt)
  => Gatherer ec gt k c d
  -> Unpack mm g x y
  -> Assign k y c
  -> MapGather mm x ec gt k c d --MapStep mm x (mt k d)
uagMapAllGatherEachFold gatherer unpacker (Assign assign) = MapGather
  gatherer
  mapStep
 where
  mapStep = case unpacker of
    Unpack unpack -> MapStepFold
      $ FL.premap (foldInto gatherer . fmap assign . unpack) FL.mconcat
    UnpackM unpackM ->
      MapStepFoldM
        $ FL.premapM (fmap (foldInto gatherer . fmap assign) . unpackM)
        $ FL.generalize FL.mconcat
{-# INLINABLE uagMapAllGatherEachFold #-}

data Reduce (mm :: Maybe (Type -> Type)) k h x e where
  Reduce :: (k -> h x -> e) -> Reduce 'Nothing k h x e
  ReduceFold :: Foldable h => (k -> FL.Fold x e) -> Reduce 'Nothing k h x e
  ReduceM :: Monad m => (k -> h x -> m e) -> Reduce ('Just m) k h x e
  ReduceFoldM :: (Monad m, Foldable h) => (k -> FL.FoldM m x e) -> Reduce ('Just m) k h x e

instance Functor (Reduce mm k h x) where
  fmap f (Reduce g) = Reduce $ \k -> f . g k
  fmap f (ReduceFold g) = ReduceFold $ \k -> fmap f (g k)
  fmap f (ReduceM g) = ReduceM $ \k -> fmap f . g k
  fmap f (ReduceFoldM g) = ReduceFoldM $ \k -> fmap f (g k)
  {-# INLINABLE fmap #-}

instance Functor h => P.Profunctor (Reduce mm k h) where
  dimap l r (Reduce g)  = Reduce $ \k -> P.dimap (fmap l) r (g k)
  dimap l r (ReduceFold g) = ReduceFold $ \k -> P.dimap l r (g k)
  dimap l r (ReduceM g)  = ReduceM $ \k -> P.dimap (fmap l) (fmap r) (g k)
  dimap l r (ReduceFoldM g) = ReduceFoldM $ \k -> P.dimap l r (g k)
  {-# INLINABLE dimap #-}

instance Foldable h => Applicative (Reduce 'Nothing k h x) where
  pure x = ReduceFold $ const (pure x)
  {-# INLINABLE pure #-}
  Reduce r1 <*> Reduce r2 = Reduce $ \k -> r1 k <*> r2 k
  ReduceFold f1 <*> ReduceFold f2 = ReduceFold $ \k -> f1 k <*> f2 k
  Reduce r1 <*> ReduceFold f2 = Reduce $ \k -> r1 k <*> (FL.fold $ f2 k)
  ReduceFold f1 <*> Reduce r2 = Reduce $ \k -> (FL.fold $ f1 k) <*> r2 k
  {-# INLINABLE (<*>) #-}

instance Monad m => Applicative (Reduce ('Just m) k h x) where
  pure x = ReduceM $ \k -> pure $ pure x
  {-# INLINABLE pure #-}
  ReduceM r1 <*> ReduceM r2 = ReduceM $ \k -> ((<*>) <$> r1 k <*> r2 k)
  ReduceFoldM f1 <*> ReduceFoldM f2 = ReduceFoldM $ \k -> f1 k <*> f2 k
  ReduceM r1 <*> ReduceFoldM f2 = ReduceM $ \k -> ((<*>) <$> r1 k <*> (FL.foldM $ f2 k))
  ReduceFoldM f1 <*> ReduceM r2 = ReduceM $ \k -> ((<*>) <$> (FL.foldM $ f1 k) <*> r2 k)
  {-# INLINABLE (<*>) #-}

-- | The most common case is that the reduction doesn't depend on the key
-- So we add support functions for processing the data and then relabeling with the key
-- And we do this for the four variations of Reduce
processAndRelabel :: (h x -> y) -> (k -> y -> z) -> Reduce 'Nothing k h x z
processAndRelabel process relabel = Reduce $ \k hx -> relabel k (process hx)
{-# INLINABLE processAndRelabel #-}

processAndRelabelM
  :: Monad m => (h x -> m y) -> (k -> y -> z) -> Reduce ( 'Just m) k h x z
processAndRelabelM processM relabel =
  ReduceM $ \k hx -> fmap (relabel k) (processM hx)
{-# INLINABLE processAndRelabelM #-}

foldAndRelabel
  :: Foldable h => FL.Fold x y -> (k -> y -> z) -> Reduce 'Nothing k h x z
foldAndRelabel fld relabel = ReduceFold $ \k -> fmap (relabel k) fld
{-# INLINABLE foldAndRelabel #-}

foldAndRelabelM
  :: (Monad m, Foldable h)
  => FL.FoldM m x y
  -> (k -> y -> z)
  -> Reduce ( 'Just m) k h x z
foldAndRelabelM fld relabel = ReduceFoldM $ \k -> fmap (relabel k) fld
{-# INLINABLE foldAndRelabelM #-}

mapReduceFold
  :: (Foldable h, Monoid e, ec e, Functor (MapFoldT mm x))
  => Gatherer ec gt k y (h z)
  -> MapStep mm x gt
  -> Reduce mm k h z e
  -> MapFoldT mm x e
mapReduceFold gatherer ms reducer = case reducer of
  Reduce f -> fmap (gFoldMapWithKey gatherer f) $ mapFold ms
  ReduceFold f ->
    fmap (gFoldMapWithKey gatherer (\k hx -> FL.fold (f k) hx)) $ mapFold ms
  ReduceM f -> monadicMapFoldM (gFoldMapWithKeyM gatherer f) $ mapFold ms
  ReduceFoldM f ->
    monadicMapFoldM (gFoldMapWithKeyM gatherer (\k hx -> FL.foldM (f k) hx))
      $ mapFold ms
{-# INLINABLE mapReduceFold #-}

mapGatherReduceFold
  :: (Foldable h, Monoid e, ec e, Functor (MapFoldT mm x))
  => MapGather mm x ec gt k y (h z)
  -> Reduce mm k h z e
  -> MapFoldT mm x e
mapGatherReduceFold (MapGather gatherer mapStep) =
  mapReduceFold gatherer mapStep
{-# INLINABLE mapGatherReduceFold #-}

monadicMapFoldM :: Monad m => (a -> m b) -> FL.FoldM m x a -> FL.FoldM m x b
monadicMapFoldM f (FL.FoldM step begin done) = FL.FoldM step begin done'
  where done' x = done x >>= f
{-# INLINABLE monadicMapFoldM #-}

--
{- Various (serial) gatherer implementations for testing, etc.
Should get moved to their own module
-}

foldToSequence :: Foldable h => h (k, c) -> Seq.Seq (k, c)
foldToSequence = FL.fold (FL.Fold (\s x -> s Seq.|> x) Seq.empty id)
{-# INLINABLE foldToSequence #-}

-- fix Seq as the gatherer type
sequenceGatherer
  :: forall mt k d c ce
   . (Semigroup d, Foldable (mt k))
  => ([(k, d)] -> mt k d)
  -> (forall e . (Monoid e, ce e) => (k -> d -> e) -> mt k d -> e)
  -> (  forall e f
      . (Applicative f, Monoid e, ce e)
     => (k -> d -> f e)
     -> mt k d
     -> f (mt k e)
     )
  -> (forall e . (Monoid e, ce e) => mt k e -> e) -- this might not be foldMap in || case
  -> ((c -> d) -> Gatherer ce (Seq.Seq (k, c)) k c d)
sequenceGatherer fromKeyValueList foldMapWithKey traverseWithKey foldMonoid toSG
  = let seqToMap :: Seq.Seq (k, c) -> mt k d
        seqToMap =
          fromKeyValueList . fmap (\(k, c) -> (k, toSG c)) . FL.fold FL.list -- can we do this as a direct fold?
    in  Gatherer foldToSequence
                 (\f s -> foldMapWithKey f $ seqToMap s)
                 (\f s -> fmap (foldMonoid) . traverseWithKey f $ seqToMap s)
{-# INLINABLE sequenceGatherer #-}

gathererSeqToStrictHashMap
  :: (Monoid d, Hashable k, Eq k)
  => (c -> d)
  -> Gatherer Empty (Seq.Seq (k, c)) k c d
gathererSeqToStrictHashMap = sequenceGatherer
  HMS.fromList
  (\f -> HMS.foldlWithKey' (\e k d -> e <> f k d) mempty)
  HMS.traverseWithKey
  (foldMap id)
{-# INLINABLE gathererSeqToStrictHashMap #-}

gathererSeqToLazyHashMap
  :: (Monoid d, Hashable k, Eq k)
  => (c -> d)
  -> Gatherer Empty (Seq.Seq (k, c)) k c d
gathererSeqToLazyHashMap = sequenceGatherer
  (HML.fromListWith (<>))
  (\f -> HML.foldlWithKey' (\e k d -> e <> f k d) mempty)
  HML.traverseWithKey
  (foldMap id)
{-# INLINABLE gathererSeqToLazyHashMap #-}

gathererSeqToStrictMap
  :: (Semigroup d, Ord k) => (c -> d) -> Gatherer Empty (Seq.Seq (k, c)) k c d
gathererSeqToStrictMap = sequenceGatherer (MS.fromListWith (<>))
                                          MS.foldMapWithKey
                                          MS.traverseWithKey
                                          (foldMap id)
{-# INLINABLE gathererSeqToStrictMap #-}

gathererSeqToLazyMap
  :: (Semigroup d, Ord k) => (c -> d) -> Gatherer Empty (Seq.Seq (k, c)) k c d
gathererSeqToLazyMap = sequenceGatherer (ML.fromListWith (<>))
                                        ML.foldMapWithKey
                                        ML.traverseWithKey
                                        (foldMap id)
{-# INLINABLE gathererSeqToLazyMap #-}


{-
-- This one is slower.  Why?
gathererSeqToSortedList
  :: (Monoid d, Ord k) => (c -> d) -> Gatherer Empty (Seq.Seq (k, c)) k c d
gathererSeqToSortedList toSG =
  let seqToList =
        Sort.monoidSortAssocs . fmap (\(k, c) -> (k, toSG c)) . FL.fold FL.list
  in  Gatherer foldToSequence
               (\f s -> mconcat . fmap (uncurry f) $ seqToList s)
               (\f s -> fmap mconcat . traverse (uncurry f) $ seqToList s)

-}
