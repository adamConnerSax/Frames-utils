{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.Aggregations
  (
    RescaleType (..)
  , FType
  , DblX
  , DblY
  , goodDataCount
  , goodDataByKey
  , filterField
  , filterMaybeField
  , aggregateToMap
  , aggregateGeneral
  , aggregateFiltered
  , aggregateFM
  , aggregateF
  , aggregateFsM
  , aggregateFs
  , transformEachM
  , transformEach
  , rescale
  , ScaleAndUnscale(..)
  , scaleAndUnscale
  , weightedScaleAndUnscale
  , DataField
  , DataFieldOf
  , TwoColData
  , ThreeColData
  , ThreeDTransformable
  , KeyedRecord
  , reshapeRowSimple
  ) where

import qualified Control.Foldl        as FL
import           Control.Lens         ((^.))
import qualified Control.Lens         as L
import           Data.Traversable    (sequenceA)
import           Control.Monad.State (evalState)
import           Data.Functor.Identity (Identity (Identity), runIdentity)
import qualified Data.Foldable     as Foldable
import qualified Data.List            as List
import qualified Data.Map             as M
import           Data.Maybe           (fromMaybe, isJust, catMaybes, fromJust)
import           Data.Text            (Text)
import qualified Data.Text            as T
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Curry     as V
import qualified Data.Vinyl.Functor   as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.XRec as V
import qualified Data.Vinyl.Class.Method as V
import qualified Data.Vinyl.Core      as V
import           Data.Vinyl.Lens      (type (∈))
import           Data.Kind            (Type,Constraint)
import           Data.Profunctor      as PF
import           Frames               ((:.), (&:))
import qualified Frames               as F
import qualified Frames.CSV           as F
import qualified Frames.ShowCSV           as F
import qualified Frames.Melt           as F
import qualified Frames.InCore        as FI
import qualified Pipes                as P
import qualified Pipes.Prelude        as P
import           Control.Arrow (second)
import           Data.Proxy (Proxy(..))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import           GHC.TypeLits (KnownSymbol)
import           Data.Random.Source.PureMT as R
import           Data.Random as R
import           Data.Function (on)

goodDataByKey :: forall ks rs. (ks F.⊆ rs, Ord (F.Record ks)) => Proxy ks ->  FL.Fold (F.Rec (Maybe F.:. F.ElField) rs) (M.Map (F.Record ks) (Int, Int))
goodDataByKey _ =
  let getKey = F.recMaybe . F.rcast @ks
  in FL.prefilter (isJust . getKey) $ FL.Fold (aggregateToMap (fromJust . getKey) (flip (:)) []) M.empty (fmap $ FL.fold goodDataCount)   

goodDataCount :: FL.Fold (F.Rec (Maybe F.:. F.ElField) rs) (Int, Int)
goodDataCount = (,) <$> FL.length <*> FL.prefilter (isJust . F.recMaybe) FL.length

filterMaybeField :: forall k rs. (F.ElemOf rs k, Eq (V.HKD F.ElField k), (V.IsoHKD F.ElField k))
                 => Proxy k -> V.HKD F.ElField k -> F.Rec (Maybe :. F.ElField) rs -> Bool
filterMaybeField _ kv =
  let maybeTest t = maybe False t
  in maybeTest (== kv) . V.toHKD . F.rget @k

filterField :: forall k rs. (F.ElemOf rs k, Eq (V.HKD F.ElField k), (V.IsoHKD F.ElField k))
                 => Proxy k -> V.HKD F.ElField k -> F.Record rs -> Bool
filterField _ kv = (== kv) . V.toHKD . F.rget @k

aggregateToMap :: Ord k => (a -> k) -> (b -> a -> b) -> b -> M.Map k b -> a -> M.Map k b
aggregateToMap getKey combine initial m r =
  let key = getKey r
      newVal = Just . flip combine r . fromMaybe initial 
  in M.alter newVal key m --M.insert key newVal m 

-- fold over c.  But c may become zero or many a (bad data, or melting rows). So we process c, then fold over the result.
aggregateGeneral :: (Ord k, Foldable f) => (c -> f a) -> (a -> k) -> (b -> a -> b) -> b -> M.Map k b -> c -> M.Map k b
aggregateGeneral unpack getKey combine initial m x =
  let aggregate = FL.Fold (aggregateToMap getKey combine initial) m id
  in FL.fold aggregate (unpack x)

-- Maybe is delightfully foldable!  
aggregateFiltered :: Ord k => (c -> Maybe a) -> (a -> k) -> (b -> a -> b) -> b -> M.Map k b -> c -> M.Map k b
aggregateFiltered = aggregateGeneral

liftCombine :: Applicative m => (a -> b -> a) -> (a -> b -> m a)
liftCombine f a b = pure $ f a b 

-- specific version for our record folds via Control.Foldl
-- extract--the processing of one aggregate's data--may be monadic 
aggregateFsM :: forall rs ks as b cs f g h m. (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs),
                                               Foldable f, Foldable h, Functor h, Applicative m)
  => Proxy ks
  -> (F.Rec g rs -> f (F.Record as))
  -> (b -> F.Record as -> b)
  -> b
  -> (b -> m (h (F.Record cs)))
  -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFsM _ unpack process initial extract =
  let addKey :: (F.Record ks, m (h (F.Record cs))) -> m (h (F.Record (ks V.++ cs)))
      addKey (k, mhcs) = fmap (fmap (V.rappend k)) mhcs
  in FL.FoldM (liftCombine $ aggregateGeneral unpack (F.rcast @ks) process initial)
     (pure M.empty)
     (fmap (F.toFrame . List.concat) . sequenceA . fmap ((fmap Foldable.toList) .addKey . second extract) . M.toList) 

aggregateFs :: (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f, Foldable h, Functor h)
            => Proxy ks
            -> (F.Rec g rs -> f (F.Record as))
            -> (b -> F.Record as -> b)
            -> b
            -> (b -> h (F.Record cs))
            -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFs proxy_ks unpack process initial extract =
  FL.simplify $ aggregateFsM proxy_ks unpack process initial (return . extract)
                   

aggregateFM :: forall rs ks as b cs f g m. (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f, Applicative m)
            => Proxy ks
            -> (F.Rec g rs -> f (F.Record as))
            -> (b -> F.Record as -> b)
            -> b
            -> (b -> m (F.Record cs))
            -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFM pks unpack process initial extract = aggregateFsM pks unpack process initial (fmap V.Identity . extract)


aggregateF :: forall rs ks as b cs f g. (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f)
           => Proxy ks
           -> (F.Rec g rs -> f (F.Record as))
           -> (b -> F.Record as -> b)
           -> b
           -> (b -> F.Record cs)
           -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateF pks unpack process initial extract = aggregateFs pks unpack process initial (V.Identity . extract)

-- This is the anamorphic step.  Is it a co-algebra of []?
-- You could also use meltRow here.  That is also (Record as -> [Record bs])
reshapeRowSimple :: forall ss ts cs ds. (ss F.⊆ ts)
                 => Proxy ss -- id columns
                 -> [F.Record cs] -- list of classifier values
                 -> (F.Record cs -> F.Record ts -> F.Record ds)
                 -> F.Record ts
                 -> [F.Record (ss V.++ cs V.++ ds)]                
reshapeRowSimple _ classifiers newDataF r = 
  let ids = F.rcast r :: F.Record ss
  in flip fmap classifiers $ \c -> (ids F.<+> c) F.<+> newDataF c r  

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

type FType x = V.Snd x

type DblX = "double_x" F.:-> Double
type DblY = "double_y" F.:-> Double

type UseCols ks x y w = ks V.++ '[x,y,w]
type DataField x = (V.KnownField x, Real (FType x))

-- This thing is...unfortunate. Is there something built into Frames or Vinyl that would do this?
class (DataField x, x ∈ rs) => DataFieldOf rs x
instance (DataField x, x ∈ rs) => DataFieldOf rs x

type TwoColData x y = F.AllConstrained (DataFieldOf [x,y]) '[x, y]
type ThreeColData x y z = ([x,z] F.⊆ [x,y,z], [y,z] F.⊆ [x,y,z], [x,y] F.⊆ [x,y,z], F.AllConstrained (DataFieldOf [x,y,z]) '[x, y, z])

type KeyedRecord ks rs = (ks F.⊆ rs, Ord (F.Record ks))

type ThreeDTransformable rs ks x y w = (ThreeColData x y w, FI.RecVec (ks V.++ [x,y,w]),
                                        KeyedRecord ks rs,
                                        ks F.⊆ (ks V.++ [x,y,w]), (ks V.++ [x,y,w]) F.⊆ rs,
                                        F.ElemOf (ks V.++ [x,y,w]) x, F.ElemOf (ks V.++ [x,y,w]) y, F.ElemOf (ks V.++ [x,y,w]) w)

transformEachM :: forall rs ks x y w m. (ThreeDTransformable rs ks x y w, Applicative m)
               => Proxy ks
               -> Proxy [x,y,w]
               -> ([F.Record '[x,y,w]] -> m [F.Record '[x,y,w]])
               -> FL.FoldM m (F.Record rs) (F.FrameRec (UseCols ks x y w))
transformEachM proxy_ks _ doOne =
  let combine l r = F.rcast @[x,y,w] r : l
  in aggregateFsM proxy_ks (V.Identity . (F.rcast @(ks V.++ [x,y,w]))) combine [] doOne

transformEach :: forall rs ks x y w m. (ThreeDTransformable rs ks x y w)
                  => Proxy ks
                  -> Proxy [x,y,w]
                  -> ([F.Record '[x,y,w]] -> [F.Record '[x,y,w]])
                  -> FL.Fold (F.Record rs) (F.FrameRec (UseCols ks x y w))
transformEach proxy_ks proxy_xyw doOne = FL.simplify $ transformEachM proxy_ks proxy_xyw (Identity . doOne)
--  let combine l r = F.rcast @[x,y,w] r : l
--  in aggregateFs proxy_ks (V.Identity . (F.rcast @(ks V.++ [x,y,w]))) combine [] doOne


