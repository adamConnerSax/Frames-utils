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
  , mapReduceParallel
  , uagMapAllGatherOnceP
  )
where

import           Control.MapReduce
import qualified Control.MapReduce             as MR
import qualified Data.Foldable                 as F
import qualified Data.List                     as L
import qualified Data.List.Split               as L
import           Data.Monoid                    ( Monoid )

import qualified Control.Parallel.Strategies   as PS


-- | like `foldMap id` but does each chunk of chunkSize in || until list is shorter than chunkSize
parFoldMonoid :: (Foldable f, Monoid m, PS.NFData m) => Int -> f m -> m
parFoldMonoid chunkSize fm = go (F.toList fm)
 where
  go lm = case L.length lm > chunkSize of
    True ->
      go
        ( PS.parMap (PS.rparWith PS.rdeepseq) (foldMap id)
        $ L.chunksOf chunkSize lm
        )
    False -> foldMap id lm

-- an example of adding parallel evaluation to the map step
-- this one does unpacking and assigning in parallel and then also uses a parallel mconcat
uagMapAllGatherOnceP
  :: (Functor h, Foldable h, Monoid (g (k, c)), PS.NFData (g (k, c)), Functor g)
  => Int
  -> MR.Unpack g x y
  -> MR.Assign keyC k y c
  -> MR.Gather g mt k c d
  -> MR.MapStep h x (mt k d)
uagMapAllGatherOnceP chunkSize (MR.Unpack unpack) (MR.Assign assign) (MR.Gather gather)
  = MR.MapStepF
    $ gather
    . parFoldMonoid chunkSize
    . PS.parMap (PS.rparWith PS.rdeepseq) (fmap assign . unpack)
    . F.toList

-- | This version will spark for each reduction
mapReduceParallel
  :: (MR.GroupMap mt k d, Foldable h, Monoid e, PS.NFData e)
  => MR.MapStep h x (mt k d)
  -> MR.ReduceOne k d e
  -> h x
  -> e
mapReduceParallel ms (MR.ReduceOne reduceOne) =
  foldMap id
    . PS.parMap (PS.rparWith PS.rdeepseq) (\(k, d) -> reduceOne k d)
    . MR.toList
    . MR.mapStep ms
