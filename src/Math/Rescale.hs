{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Math.Rescale
  (
    RescaleType (..)
  , rescale
  , ScaleAndUnscale(..)
  , scaleAndUnscale
  , weightedScaleAndUnscale
  ) where

import qualified Control.Foldl   as FL
import qualified Data.Foldable   as Foldable
import           Data.Function   (on)
import qualified Data.List       as List
import qualified Data.Profunctor as PF

-- We use a GADT here so that each constructor can carry the proofs of numerical type.  E.g., We don't want RescaleNone to require RealFloat.
data RescaleType a where
  RescaleNone :: RescaleType a
  RescaleMean :: RealFrac a => Double -> RescaleType a
  RescaleMedian :: (Ord a, Real a) => Double -> RescaleType a
  RescaleNormalize :: RealFloat a => Double -> RescaleType a
  RescaleGiven :: (a, Double) -> RescaleType a

rescale :: forall a. Num a  => RescaleType a -> FL.Fold a (a, Double)
rescale RescaleNone = pure (0,1)
rescale (RescaleGiven x) = pure x
rescale (RescaleMean s) = (,) <$> pure 0 <*> (fmap ((/s) . realToFrac) FL.mean)
rescale (RescaleNormalize s) =
  let folds = (,,) <$> FL.mean <*> FL.std <*> FL.length
      sc (_,sd,n) = if n == 1 then 1 else realToFrac sd
      g f@(m,_,_) = (realToFrac m, (sc f)/s)
  in  g <$> folds
rescale (RescaleMedian s) = (,) <$> pure 0 <*> (fmap ((/s) . listToMedian) FL.list) where
  listToMedian unsorted =
    let l = List.sort unsorted
        n = List.length l
    in case n of
      0 -> 1
      _ -> let m = n `div` 2 in if (odd n) then realToFrac (l !! m) else realToFrac (l List.!! m + l List.!! (m - 1))/2.0

wgt :: (Real a, Real w) => (a , w) -> Double
wgt  (x,y) = realToFrac x * realToFrac y

weightedRescale :: forall a w. (Real a, Real w)  => RescaleType a -> FL.Fold (a,w) (a, Double)
weightedRescale RescaleNone = pure (0,1)
weightedRescale (RescaleGiven x) = pure x
weightedRescale (RescaleMean s) =
  let folds = (,) <$> PF.lmap wgt FL.mean <*> PF.lmap snd FL.sum
      f (wm, tw) = (0, wm/(s * realToFrac tw))
  in f <$> folds
weightedRescale (RescaleNormalize s) =
  let folds = (,,,) <$> PF.lmap wgt FL.mean <*> PF.lmap wgt FL.std <*> PF.lmap snd FL.sum <*> FL.length
      weightedMean x n tw =realToFrac n * realToFrac x/realToFrac tw
      weightedSD sd n tw = if n == 1 then 1 else sqrt (realToFrac n) * sd/realToFrac tw
      f (wm, ws, tw, n) = (weightedMean wm n tw, (weightedSD ws n tw)/s)
  in f <$> folds
weightedRescale (RescaleMedian s) = (,) <$> pure 0 <*> (fmap ((/s) . realToFrac . listToMedian) FL.list) where
  listToMedian :: [(a,w)] -> a
  listToMedian unsorted =
    let l = List.sortBy (compare `on` fst) unsorted
        tw = FL.fold (PF.lmap snd FL.sum) l
    in case (List.length l) of
      0 -> 1
      _ -> go 0 l where
        mw = (realToFrac tw)/2
        go :: w -> [(a,w)] -> a
        go _ [] = 0 -- this shouldn't happen
        go wgtSoFar ((a,w) : was) = let wgtSoFar' = wgtSoFar + w in if realToFrac wgtSoFar' > mw then a else go wgtSoFar' was

data ScaleAndUnscale a = ScaleAndUnscale { from :: (a -> Double), backTo :: (Double -> a) }

scaleAndUnscaleHelper :: Real a => (Double -> a) -> ((a, Double), (a,Double)) -> ScaleAndUnscale a
scaleAndUnscaleHelper toA s = ScaleAndUnscale (csF s) (osF s) where
  csF ((csShift,csScale),_) a = realToFrac (a - csShift)/csScale
  uncsF ((csShift, csScale),_) x = (x * csScale) + realToFrac csShift
  osF s@(_q,(osShift, osScale)) x = toA $ (uncsF s x - realToFrac osShift)/osScale

scaleAndUnscale :: Real a => RescaleType a -> RescaleType a -> (Double -> a) -> FL.Fold a (ScaleAndUnscale a)
scaleAndUnscale computeScale outScale toA = fmap (scaleAndUnscaleHelper toA) shifts where
  shifts = (,) <$> rescale computeScale <*> rescale outScale

weightedScaleAndUnscale :: (Real a, Real w) => RescaleType a -> RescaleType a -> (Double -> a) -> FL.Fold (a,w) (ScaleAndUnscale a)
weightedScaleAndUnscale computeScale outScale toA = fmap (scaleAndUnscaleHelper toA) shifts where
  shifts = (,) <$> weightedRescale computeScale <*> weightedRescale outScale



