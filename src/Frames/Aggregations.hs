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
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.Aggregations
  (
    FType
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
  , aggregateAndAnalyzeEachM
  , aggregateAndAnalyzeEachM'
  , aggregateAndAnalyzeEach
  , aggregateToMonoidalMap
  , aggregateMonoidalGeneral
  , aggregateMonoidalFiltered
  , aggregateMonoidalFM
  , aggregateMonoidalF
  , aggregateMonoidalFsM
  , aggregateMonoidalFs
  , RealField
  , RealFieldOf
  , TwoColData
  , ThreeColData
  , ThreeDTransformable
  , KeyedRecord
  ) where

import qualified Control.Foldl        as FL
import           Data.Traversable    (sequenceA)
import           Data.Functor.Identity (Identity (Identity))
import qualified Data.Foldable     as Foldable
import qualified Data.List            as List
import qualified Data.Map             as M
import qualified Data.Map.Monoidal    as MM
import           Data.Maybe           (fromMaybe, isJust, fromJust)
import           Data.Monoid         ((<>), Monoid(..))
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Functor   as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.XRec as V
import           Data.Vinyl.Lens      (type (∈))
import           Frames               ((:.))
import qualified Frames               as F
import qualified Frames.Melt           as F
import qualified Frames.InCore        as FI
import           Control.Arrow (second)

-- THe functions below shoudl move.  To MaybeUtils?  What about filterField?
-- returns a map, keyed by F.Record ks, of (number of rows, number of rows with all cols parsed)
-- required TypeApplications for ks
goodDataByKey :: forall ks rs. (ks F.⊆ rs, Ord (F.Record ks))
  => FL.Fold (F.Rec (Maybe F.:. F.ElField) rs) (M.Map (F.Record ks) (Int, Int))
goodDataByKey =
  let getKey = F.recMaybe . F.rcast @ks
  in FL.prefilter (isJust . getKey) $ FL.Fold (aggregateToMap (fromJust . getKey) (flip (:)) []) M.empty (fmap $ FL.fold goodDataCount)   

goodDataCount :: FL.Fold (F.Rec (Maybe F.:. F.ElField) rs) (Int, Int)
goodDataCount = (,) <$> FL.length <*> FL.prefilter (isJust . F.recMaybe) FL.length

filterField :: forall k rs. (V.KnownField k, F.ElemOf rs k) => (V.Snd k -> Bool) -> F.Record rs -> Bool
filterField test = test . F.rgetField @k

filterMaybeField :: forall k rs. (F.ElemOf rs k, V.IsoHKD F.ElField k)
                 => (V.HKD F.ElField k -> Bool) -> F.Rec (Maybe :. F.ElField) rs -> Bool
filterMaybeField test = maybe False test . V.toHKD . F.rget @k


aggregateToMap :: Ord k => (a -> k) -> (b -> a -> b) -> b -> M.Map k b -> a -> M.Map k b
aggregateToMap getKey combine initial m r =
  let key = getKey r
      newVal = Just . flip combine r . fromMaybe initial 
  in M.alter newVal key m --M.insert key newVal m 

aggregateToMonoidalMap :: (Ord k, Monoid b) => (a -> k) -> (a -> b) -> MM.MonoidalMap k b -> a -> MM.MonoidalMap k b
aggregateToMonoidalMap getKey getB m r =
  let key = getKey r
      newVal = Just .  (<> getB r) . fromMaybe mempty 
  in MM.alter newVal key m --M.insert key newVal m 

-- fold over c.  But c may become zero or many a (bad data, or melting rows). So we process c, then fold over the result.
aggregateGeneral :: (Ord k, Foldable f) => (c -> f a) -> (a -> k) -> (b -> a -> b) -> b -> M.Map k b -> c -> M.Map k b
aggregateGeneral unpack getKey combine initial m x =
  let aggregate = FL.Fold (aggregateToMap getKey combine initial) m id
  in FL.fold aggregate (unpack x)

aggregateMonoidalGeneral :: (Ord k, Foldable f, Monoid b) => (c -> f a) -> (a -> k) -> (a -> b) -> MM.MonoidalMap k b -> c -> MM.MonoidalMap k b
aggregateMonoidalGeneral unpack getKey getB m x =
  let aggregate = FL.Fold (aggregateToMonoidalMap getKey getB) m id
  in FL.fold aggregate (unpack x)

-- Maybe is delightfully foldable!  
aggregateFiltered :: Ord k => (c -> Maybe a) -> (a -> k) -> (b -> a -> b) -> b -> M.Map k b -> c -> M.Map k b
aggregateFiltered = aggregateGeneral

-- Maybe is delightfully foldable!  
aggregateMonoidalFiltered :: (Ord k, Monoid b) => (c -> Maybe a) -> (a -> k) -> (a -> b) -> MM.MonoidalMap k b -> c -> MM.MonoidalMap k b
aggregateMonoidalFiltered = aggregateMonoidalGeneral


liftCombine :: Applicative m => (a -> b -> a) -> (a -> b -> m a)
liftCombine f a  = pure . f a  

-- specific version for our record folds via Control.Foldl
-- extract--the processing of one aggregate's data--may be monadic 
aggregateFsM :: forall ks rs as b cs f g h m. (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs),
                                               Foldable f, Foldable h, Functor h, Applicative m)
  => (F.Rec g rs -> f (F.Record as))
  -> (b -> F.Record as -> b)
  -> b
  -> (b -> m (h (F.Record cs)))
  -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFsM unpack process initial extract =
  let addKey :: (F.Record ks, m (h (F.Record cs))) -> m (h (F.Record (ks V.++ cs)))
      addKey (k, mhcs) = fmap (fmap (V.rappend k)) mhcs
  in FL.FoldM (liftCombine $ aggregateGeneral unpack (F.rcast @ks) process initial)
     (pure M.empty)
     (fmap (F.toFrame . List.concat) . sequenceA . fmap ((fmap Foldable.toList) .addKey . second extract) . M.toList) 

aggregateMonoidalFsM :: forall ks rs as b cs f g h m. (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Monoid b,
                                                       Foldable f, Foldable h, Functor h, Applicative m)
  => (F.Rec g rs -> f (F.Record as))
  -> (F.Record as -> b)
  -> (b -> m (h (F.Record cs)))
  -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalFsM unpack toMonoid extract =
  let addKey :: (F.Record ks, m (h (F.Record cs))) -> m (h (F.Record (ks V.++ cs)))
      addKey (k, mhcs) = fmap (fmap (V.rappend k)) mhcs
  in FL.FoldM (liftCombine $ aggregateMonoidalGeneral unpack (F.rcast @ks) toMonoid)
     (pure MM.empty)
     (fmap (F.toFrame . List.concat) . sequenceA . fmap ((fmap Foldable.toList) . addKey . second extract) . MM.toList) 


aggregateFs :: forall ks rs as cs b f g h. (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f, Foldable h, Functor h)
            => (F.Rec g rs -> f (F.Record as))
            -> (b -> F.Record as -> b)
            -> b
            -> (b -> h (F.Record cs))
            -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFs unpack process initial extract =
  FL.simplify $ aggregateFsM @ks unpack process initial (return . extract)

aggregateMonoidalFs :: forall ks rs as cs b f g h. (ks F.⊆ as
                                                   , Monoid b
                                                   , Ord (F.Record ks)
                                                   , FI.RecVec (ks V.++ cs)
                                                   , Foldable f
                                                   , Foldable h
                                                   , Functor h)
            => (F.Rec g rs -> f (F.Record as))
            -> (F.Record as -> b)
            -> (b -> h (F.Record cs))
            -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalFs unpack toMonoid extract =
  FL.simplify $ aggregateMonoidalFsM @ks unpack toMonoid (return . extract)

  
aggregateFM :: forall ks rs as b cs f g m. (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f, Applicative m)
            => (F.Rec g rs -> f (F.Record as))
            -> (b -> F.Record as -> b)
            -> b
            -> (b -> m (F.Record cs))
            -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFM unpack process initial extract = aggregateFsM @ks unpack process initial (fmap V.Identity . extract)

aggregateMonoidalFM :: forall ks rs as b cs f g m. (ks F.⊆ as
                                                   , Monoid b
                                                   , Ord (F.Record ks)
                                                   , FI.RecVec (ks V.++ cs)
                                                   , Foldable f
                                                   , Applicative m)
            => (F.Rec g rs -> f (F.Record as))
            -> (F.Record as -> b)
            -> (b -> m (F.Record cs))
            -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalFM unpack toMonoid extract = aggregateMonoidalFsM @ks unpack toMonoid (fmap V.Identity . extract)

aggregateF :: forall ks rs as b cs f g. (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f)
           => (F.Rec g rs -> f (F.Record as))
           -> (b -> F.Record as -> b)
           -> b
           -> (b -> F.Record cs)
           -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateF unpack process initial extract = aggregateFs @ks unpack process initial (V.Identity . extract)

aggregateMonoidalF :: forall ks rs as b cs f g. (ks F.⊆ as, Monoid b, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f)
           => (F.Rec g rs -> f (F.Record as))
           -> (F.Record as -> b)
           -> (b -> F.Record cs)
           -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalF unpack toMonoid extract = aggregateMonoidalFs @ks unpack toMonoid (V.Identity . extract)



type FType x = V.Snd x

type DblX = "double_x" F.:-> Double
type DblY = "double_y" F.:-> Double

type UseCols ks x y w = ks V.++ '[x,y,w]
type RealField x = (V.KnownField x, Real (FType x))

-- This thing is...unfortunate. Is there something built into Frames or Vinyl that would do this?
class (RealField x, x ∈ rs) => RealFieldOf rs x
instance (RealField x, x ∈ rs) => RealFieldOf rs x

type TwoColData x y = F.AllConstrained (RealFieldOf [x,y]) '[x, y]
type ThreeColData x y z = ([x,z] F.⊆ [x,y,z], [y,z] F.⊆ [x,y,z], [x,y] F.⊆ [x,y,z], F.AllConstrained (RealFieldOf [x,y,z]) '[x, y, z])

type KeyedRecord ks rs = (ks F.⊆ rs, Ord (F.Record ks))

type ThreeDTransformable rs ks x y w = (ThreeColData x y w, FI.RecVec (ks V.++ [x,y,w]),
                                        KeyedRecord ks rs,
                                        ks F.⊆ (ks V.++ [x,y,w]), (ks V.++ [x,y,w]) F.⊆ rs,
                                        F.ElemOf (ks V.++ [x,y,w]) x, F.ElemOf (ks V.++ [x,y,w]) y, F.ElemOf (ks V.++ [x,y,w]) w)

aggregateAndAnalyzeEachM :: forall ks x y w rs m. (ThreeDTransformable rs ks x y w, Applicative m)
               => ([F.Record '[x,y,w]] -> m [F.Record '[x,y,w]])
               -> FL.FoldM m (F.Record rs) (F.FrameRec (UseCols ks x y w))
aggregateAndAnalyzeEachM doOne =
  let combine l r = F.rcast @[x,y,w] r : l
  in aggregateFsM @ks (V.Identity . (F.rcast @(ks V.++ [x,y,w]))) combine [] doOne

aggregateAndAnalyzeEach :: forall ks x y w rs. (ThreeDTransformable rs ks x y w)
                  => ([F.Record '[x,y,w]] -> [F.Record '[x,y,w]])
                  -> FL.Fold (F.Record rs) (F.FrameRec (UseCols ks x y w))
aggregateAndAnalyzeEach doOne = FL.simplify $ aggregateAndAnalyzeEachM @ks (Identity . doOne)
--  let combine l r = F.rcast @[x,y,w] r : l
--  in aggregateFs proxy_ks (V.Identity . (F.rcast @(ks V.++ [x,y,w]))) combine [] doOne

type UnKeyed rs ks = F.Record (F.RDeleteAll ks rs)
aggregateAndAnalyzeEachM' :: forall ks rs as m. (KeyedRecord ks rs, FI.RecVec (ks V.++ as), Applicative m)
               => ([F.Record rs] -> m [F.Record as])
               -> FL.FoldM m (F.Record rs) (F.FrameRec (ks V.++ as))
aggregateAndAnalyzeEachM' doOne =
  let combine l r = r : l
  in aggregateFsM @ks V.Identity combine [] doOne

