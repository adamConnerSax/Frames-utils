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
module Frames.Aggregations.Core
  ( aggregateFM
  , aggregateF
  , aggregateFsM
  , aggregateFs
  )
where

import qualified Frames.Utils                  as FU
import qualified Control.Aggregations          as CA

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

-- specific version for our record folds via Control.Foldl
-- extract--the processing of one aggregate's data--may be monadic 
aggregateFsM
  :: forall ks rs as b cs f g h m
   . ( ks F.⊆ as
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Foldable h
     , Functor h
     , Applicative m
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (b -> F.Record as -> b)
  -> b
  -> (b -> m (h (F.Record cs)))
  -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFsM unpack process initial extract =
  let addKey
        :: (F.Record ks, m (h (F.Record cs))) -> m (h (F.Record (ks V.++ cs)))
      addKey (k, mhcs) = fmap (fmap (V.rappend k)) mhcs
  in  FL.FoldM
        (liftCombine $ CA.aggregateGeneral unpack (F.rcast @ks) process initial
        )
        (pure M.empty)
        ( fmap (F.toFrame . List.concat)
        . sequenceA
        . fmap ((fmap Foldable.toList) . addKey . second extract)
        . M.toList
        )

aggregateFs
  :: forall ks rs as cs b f g h
   . ( ks F.⊆ as
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Foldable h
     , Functor h
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (b -> F.Record as -> b)
  -> b
  -> (b -> h (F.Record cs))
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFs unpack process initial extract =
  FL.simplify $ aggregateFsM @ks unpack process initial (return . extract)

aggregateFM
  :: forall ks rs as b cs f g m
   . ( ks F.⊆ as
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Applicative m
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (b -> F.Record as -> b)
  -> b
  -> (b -> m (F.Record cs))
  -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFM unpack process initial extract =
  aggregateFsM @ks unpack process initial (fmap V.Identity . extract)

aggregateF
  :: forall ks rs as b cs f g
   . (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f)
  => (F.Rec g rs -> f (F.Record as))
  -> (b -> F.Record as -> b)
  -> b
  -> (b -> F.Record cs)
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateF unpack process initial extract =
  aggregateFs @ks unpack process initial (V.Identity . extract)




