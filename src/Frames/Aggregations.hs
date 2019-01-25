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
import           GHC.TypeLits (Symbol,KnownSymbol)
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

aggregateAndAnalyzeEachM :: forall rs ks x y w m. (ThreeDTransformable rs ks x y w, Applicative m)
               => Proxy ks
               -> Proxy [x,y,w]
               -> ([F.Record '[x,y,w]] -> m [F.Record '[x,y,w]])
               -> FL.FoldM m (F.Record rs) (F.FrameRec (UseCols ks x y w))
aggregateAndAnalyzeEachM proxy_ks _ doOne =
  let combine l r = F.rcast @[x,y,w] r : l
  in aggregateFsM proxy_ks (V.Identity . (F.rcast @(ks V.++ [x,y,w]))) combine [] doOne

aggregateAndAnalyzeEach :: forall rs ks x y w m. (ThreeDTransformable rs ks x y w)
                  => Proxy ks
                  -> Proxy [x,y,w]
                  -> ([F.Record '[x,y,w]] -> [F.Record '[x,y,w]])
                  -> FL.Fold (F.Record rs) (F.FrameRec (UseCols ks x y w))
aggregateAndAnalyzeEach proxy_ks proxy_xyw doOne = FL.simplify $ aggregateAndAnalyzeEachM proxy_ks proxy_xyw (Identity . doOne)
--  let combine l r = F.rcast @[x,y,w] r : l
--  in aggregateFs proxy_ks (V.Identity . (F.rcast @(ks V.++ [x,y,w]))) combine [] doOne

type UnKeyed rs ks = F.Record (F.RDeleteAll ks rs)
aggregateAndAnalyzeEachM' :: forall rs ks as m. (KeyedRecord ks rs, FI.RecVec (ks V.++ as), Applicative m)
               => Proxy ks
               -> ([F.Record rs] -> m [F.Record as])
               -> FL.FoldM m (F.Record rs) (F.FrameRec (ks V.++ as))
aggregateAndAnalyzeEachM' proxy_ks doOne =
  let combine l r = r : l
  in aggregateFsM proxy_ks V.Identity combine [] doOne

