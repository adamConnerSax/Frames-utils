{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-|
Module      : Frames.Enumerations
Description : Various functions to assis when mapping between a frame with columns for each value of an enumeration and a Data.Array
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

-}
module Frames.Enumerations
  ( makeArrayGeneralMF
  , makeArrayMF
  , makeArrayWithDefaultF
  , recordsToArrayMF
  , foldTotalMonoidArray
  , sumTotalNumArray
  , prodTotalNumArray
  )
where

import qualified Control.Foldl                 as FL
import qualified Data.Array                    as A
import qualified Data.Map                      as M
import qualified Data.Monoid                   as Mon
import           Data.Ix                        ( Ix )
import qualified Data.Profunctor               as PF
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
import qualified Frames.Melt                   as F

import qualified Frames.MapReduce              as MR -- just for postMapM



-- some utilities for mapping between records and arrays (total Maps, basically)
mapAscListAll
  :: forall k v . (Enum k, Ord k, Bounded k) => M.Map k v -> Maybe [(k, v)]
mapAscListAll m = if (length (M.keys m) == length [minBound @k ..])
  then Just $ M.toAscList m
  else Nothing

makeArrayGeneralMF
  :: forall x k v
   . (Enum k, Ord k, Bounded k, A.Ix k)
  => M.Map k v
  -> (x -> k)
  -> (x -> v)
  -> (v -> v -> v)
  -> FL.FoldM Maybe x (A.Array k v)
makeArrayGeneralMF m0 getKey getVal combine =
  fmap (A.array (minBound, maxBound))
    $ MR.postMapM mapAscListAll
    $ FL.generalize
    $ FL.Fold (\m x -> M.insertWith combine (getKey x) (getVal x) m) m0 id

makeArrayMF
  :: forall x k v
   . (Enum k, Ord k, Bounded k, A.Ix k)
  => (x -> k)
  -> (x -> v)
  -> (v -> v -> v)
  -> FL.FoldM Maybe x (A.Array k v)
makeArrayMF = makeArrayGeneralMF M.empty

makeArrayWithDefaultF
  :: forall x k v
   . (Enum k, Ord k, Bounded k, A.Ix k)
  => v
  -> (x -> k)
  -> (x -> v)
  -> (v -> v -> v)
  -> FL.Fold x (A.Array k v)
makeArrayWithDefaultF d getKey getVal combine = FL.Fold
  (\m x -> M.insertWith combine (getKey x) (getVal x) m)
  (M.fromList $ fmap (, d) [minBound ..])
  (A.array (minBound, maxBound) . M.toAscList)

-- This function requires at least one record of each type or else it returns Nothing
recordsToArrayMF
  :: forall kf vf k v rs
   . ( V.KnownField kf
     , V.Snd kf ~ k
     , V.KnownField vf
     , V.Snd vf ~ v
     , F.ElemOf rs kf
     , F.ElemOf rs vf
     , Enum k
     , Ord k
     , Bounded k
     , Ix k
     )
  => FL.FoldM Maybe (F.Record rs) (A.Array k v)
recordsToArrayMF = makeArrayMF (F.rgetField @kf) (F.rgetField @vf) (flip const)

foldTotalMonoidArray
  :: (Monoid a, A.Ix k, Bounded k) => FL.Fold (A.Array k a) (A.Array k a)
foldTotalMonoidArray = FL.Fold
  (\as a -> A.accum (<>) as (A.assocs a))
  (A.listArray (minBound, maxBound) $ repeat mempty)
  id

-- a particularly useful pair 
sumTotalNumArray
  :: (Num a, A.Ix k, Bounded k) => FL.Fold (A.Array k a) (A.Array k a)
sumTotalNumArray =
  PF.dimap (fmap Mon.Sum) (fmap Mon.getSum) $ foldTotalMonoidArray

prodTotalNumArray
  :: (Num a, A.Ix k, Bounded k) => FL.Fold (A.Array k a) (A.Array k a)
prodTotalNumArray =
  PF.dimap (fmap Mon.Product) (fmap Mon.getProduct) $ foldTotalMonoidArray
