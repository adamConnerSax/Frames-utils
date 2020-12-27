{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Math.Rescale
  (
    RescaleType (..)
  , rescale
  , ScaleAndUnscale(..)
  , scaleAndUnscale
  , weightedScaleAndUnscale
  ) where

import qualified Control.Foldl            as FL
import qualified Control.Foldl.Statistics as FS
--import qualified Data.Foldable            as Foldable
--import           Data.Function            (on)
import qualified Data.List                as List
import qualified Data.Profunctor          as PF

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
rescale (RescaleMean x) = (0,) <$> fmap ((/x) . realToFrac) FL.mean
rescale (RescaleNormalize x) =
  let folds = (,,) <$> FL.mean <*> FL.std <*> FL.length
      sc (_,sd,n) = if n == 1 then 1 else realToFrac sd
      g f@(mean, _, _) = (realToFrac mean, sc f/x)
  in  g <$> folds
rescale (RescaleMedian x) = (0,) <$> fmap ((/x) . listToMedian) FL.list where
  listToMedian unsorted =
    let l = List.sort unsorted
        n = List.length l
    in case n of
      0 -> 1
      _ -> let middle = n `div` 2 in if odd n then realToFrac (l List.!! middle) else realToFrac (l List.!! middle + l List.!! (middle - 1))/2.0

{-
wgt :: (Real a, Real w) => (a , w) -> Double
wgt  (x,y) = realToFrac x * realToFrac y
-}
toDoubles :: (Real a, Real b) => (a, b) -> (Double, Double)
toDoubles (x, y) = (realToFrac x, realToFrac y)


data WestMeanVar = WestMeanVar !Double !Double !Double !Double


weightedRescale :: forall a w. (Real a, RealFrac w)  => RescaleType a -> FL.Fold (a,w) (a, Double)
weightedRescale RescaleNone = pure (0,1)
weightedRescale (RescaleGiven x) = pure x
weightedRescale (RescaleMean s) =
  let wmF = FL.premap toDoubles FS.meanWeighted
      f wm = (0, wm/s)
  in f <$> wmF
weightedRescale (RescaleNormalize rnX) =
  let westMeanVarF = FL.Fold step (WestMeanVar 0 0 0 0) done
      step (WestMeanVar ws ws2 m s) (x, w) = WestMeanVar ws' ws2' m' s' where
        ws' = ws + w
        ws2' = ws2 + (w*w)
        m' = m + ((w/ws') * (x - m))
        s' = s + (w * (x - m) * (x - m'))
      done (WestMeanVar ws _ m s) = (m, sqrt (s/ws))
  in bimap realToFrac (rnX /) <$> FL.premap toDoubles westMeanVarF
{-
  let folds = (,,,) <$> PF.lmap toDoubles FS.meanWeighted <*> PF.lmap toDoubles FS.std <*> PF.lmap snd FL.sum <*> FL.length
      weightedMean x n tw =realToFrac n * realToFrac x/realToFrac tw
      weightedSD sd n tw = if n == 1 then 1 else sqrt (realToFrac n) * sd/realToFrac tw
      f (wm, ws, tw, n) = (weightedMean wm n tw, (weightedSD ws n tw)/s)
  in f <$> folds
-}
weightedRescale (RescaleMedian s) = (0,) <$> fmap ((/s) . realToFrac . listToMedian) FL.list where
  listToMedian :: [(a,w)] -> a
  listToMedian unsorted =
    let l = List.sortBy (compare `on` fst) unsorted
        tw :: w = FL.fold (PF.lmap snd FL.sum) l
    in case List.length l of
      0 -> 1
      _ -> go 0 l where
        mw :: Double = realToFrac tw / 2
        go :: w -> [(a,w)] -> a
        go _ [] = 0 -- this shouldn't happen
        go wgtSoFar ((a,w) : was) = let wgtSoFar' = wgtSoFar + w in if realToFrac wgtSoFar' > mw then a else go wgtSoFar' was

data ScaleAndUnscale a = ScaleAndUnscale { from :: a -> Double, backTo :: Double -> a }

scaleAndUnscaleHelper :: Real a => (Double -> a) -> ((a, Double), (a,Double)) -> ScaleAndUnscale a
scaleAndUnscaleHelper toA s = ScaleAndUnscale (csF s) (osF s) where
  csF ((csShift,csScale),_) a = realToFrac (a - csShift)/csScale
  uncsF ((csShift, csScale),_) x = (x * csScale) + realToFrac csShift
  osF sc@(_q,(osShift, osScale)) x = toA $ (uncsF sc x - realToFrac osShift)/osScale

scaleAndUnscale :: Real a => RescaleType a -> RescaleType a -> (Double -> a) -> FL.Fold a (ScaleAndUnscale a)
scaleAndUnscale computeScale outScale toA = fmap (scaleAndUnscaleHelper toA) shifts where
  shifts = (,) <$> rescale computeScale <*> rescale outScale

weightedScaleAndUnscale :: (Real a, RealFrac w) => RescaleType a -> RescaleType a -> (Double -> a) -> FL.Fold (a,w) (ScaleAndUnscale a)
weightedScaleAndUnscale computeScale outScale toA = fmap (scaleAndUnscaleHelper toA) shifts where
  shifts = (,) <$> weightedRescale computeScale <*> weightedRescale outScale



