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
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.Aggregations.Monoidal
  ( aggregateMonoidalFM
  , aggregateMonoidalF
  , aggregateMonoidalFsM
  , aggregateMonoidalFs
  , aggregateAndAnalyzeEachM
  , aggregateAndAnalyzeEachM'
  , aggregateAndAnalyzeEach
  )
where

import qualified Frames.Utils                  as FU
import qualified Control.Aggregations          as CA
--import qualified Frame.Aggregations.Core       as FAC

import qualified Control.Foldl                 as FL
import qualified Control.Newtype               as N
import           Data.Traversable               ( sequenceA )
import           Data.Functor.Identity          ( Identity(Identity) )
import qualified Data.Foldable                 as Foldable
import qualified Data.List                     as List
import qualified Data.Map                      as M
import qualified Data.Map.Monoidal             as MM
import           Data.Monoid                    ( (<>)
                                                , Monoid(..)
                                                )
import qualified Data.Profunctor               as P
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.Functor            as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import           Control.Arrow                  ( second )

liftCombine :: Applicative m => (a -> b -> a) -> (a -> b -> m a)
liftCombine f a = pure . f a

aggregateMonoidalFsM
  :: forall ks rs as b cs f g h m
   . ( ks F.⊆ as
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Monoid b
     , Foldable f
     , Foldable h
     , Functor h
     , Applicative m
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (F.Record as -> b)
  -> (b -> m (h (F.Record cs)))
  -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalFsM unpack toMonoid extract =
  let addKey
        :: (F.Record ks, m (h (F.Record cs))) -> m (h (F.Record (ks V.++ cs)))
      addKey (k, mhcs) = fmap (fmap (V.rappend k)) mhcs
  in  FL.FoldM
        ( liftCombine
        $ CA.aggregateMonoidalGeneral unpack (F.rcast @ks) toMonoid
        )
        (pure MM.empty)
        ( fmap (F.toFrame . List.concat)
        . sequenceA
        . fmap ((fmap Foldable.toList) . addKey . second extract)
        . MM.toList
        )

aggregateMonoidalFs
  :: forall ks rs as cs b f g h
   . ( ks F.⊆ as
     , Monoid b
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Foldable h
     , Functor h
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (F.Record as -> b)
  -> (b -> h (F.Record cs))
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalFs unpack toMonoid extract =
  FL.simplify $ aggregateMonoidalFsM @ks unpack toMonoid (return . extract)

aggregateMonoidalFM
  :: forall ks rs as b cs f g m
   . ( ks F.⊆ as
     , Monoid b
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Applicative m
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (F.Record as -> b)
  -> (b -> m (F.Record cs))
  -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalFM unpack toMonoid extract =
  aggregateMonoidalFsM @ks unpack toMonoid (fmap V.Identity . extract)

aggregateMonoidalF
  :: forall ks rs as b cs f g
   . ( ks F.⊆ as
     , Monoid b
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (F.Record as -> b)
  -> (b -> F.Record cs)
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalF unpack toMonoid extract =
  aggregateMonoidalFs @ks unpack toMonoid (V.Identity . extract)

-- These use the monoid instance of [], since the first argument is ([F.Record '[x,y,w]] -> ...)
-- could we use a smarter structure here?  Sequence? The frame itself?

type UseCols ks x y w = ks V.++ '[x,y,w]
type KeyedRecord ks rs = (ks F.⊆ rs, Ord (F.Record ks))


aggregateAndAnalyzeEachM
  :: forall ks x y w rs m
   . (FU.ThreeDTransformable rs ks x y w, Applicative m)
  => ([F.Record '[x, y, w]] -> m [F.Record '[x, y, w]])
  -> FL.FoldM m (F.Record rs) (F.FrameRec (UseCols ks x y w))
aggregateAndAnalyzeEachM doOne = aggregateMonoidalFsM @ks
  (V.Identity . (F.rcast @(ks V.++ '[x, y, w])))
  (pure . F.rcast @'[x, y, w])
  doOne

aggregateAndAnalyzeEach
  :: forall ks x y w rs
   . (FU.ThreeDTransformable rs ks x y w)
  => ([F.Record '[x, y, w]] -> [F.Record '[x, y, w]])
  -> FL.Fold (F.Record rs) (F.FrameRec (UseCols ks x y w))
aggregateAndAnalyzeEach doOne =
  FL.simplify $ aggregateAndAnalyzeEachM @ks (Identity . doOne)

aggregateAndAnalyzeEachM'
  :: forall ks rs as m
   . (KeyedRecord ks rs, FI.RecVec (ks V.++ as), Applicative m)
  => ([F.Record rs] -> m [F.Record as])
  -> FL.FoldM m (F.Record rs) (F.FrameRec (ks V.++ as))
aggregateAndAnalyzeEachM' doOne =
  aggregateMonoidalFsM @ks V.Identity pure doOne
