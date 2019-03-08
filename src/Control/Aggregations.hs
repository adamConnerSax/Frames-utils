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
module Control.Aggregations
  ( aggregateToMap
  , aggregateGeneral
  , aggregateFiltered
  , aggregateToMonoidalMap
  , aggregateMonoidalGeneral
  , aggregateMonoidalFiltered
  )
where

import qualified Control.Foldl                 as FL
import qualified Control.Newtype               as N
import           Data.Traversable               ( sequenceA )
import           Data.Functor.Identity          ( Identity(Identity) )
import qualified Data.Foldable                 as Foldable
import qualified Data.List                     as List
import qualified Data.Map                      as M
import qualified Data.Map.Monoidal             as MM
import           Data.Maybe                     ( fromMaybe
                                                , isJust
                                                , fromJust
                                                )
import           Data.Monoid                    ( (<>)
                                                , Monoid(..)
                                                )
import qualified Data.Profunctor               as P
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.Functor            as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.XRec               as V
import qualified Data.Vinyl.Class.Method       as V
import           Frames                         ( (:.) )
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import           Control.Arrow                  ( second )



aggregateToMap
  :: Ord k => (a -> k) -> (b -> a -> b) -> b -> M.Map k b -> a -> M.Map k b
aggregateToMap getKey combine initial m r =
  let key    = getKey r
      newVal = Just . flip combine r . fromMaybe initial
  in  M.alter newVal key m --M.insert key newVal m 

aggregateToMonoidalMap
  :: (Ord k, Monoid b)
  => (a -> k)
  -> (a -> b)
  -> MM.MonoidalMap k b
  -> a
  -> MM.MonoidalMap k b
aggregateToMonoidalMap getKey getB m r =
  let key    = getKey r
      newVal = Just . (<> getB r) . fromMaybe mempty
  in  MM.alter newVal key m --M.insert key newVal m 

-- fold over c.  But c may become zero or many a (bad data, or melting rows). So we process c, then fold over the result.
aggregateGeneral
  :: (Ord k, Foldable f)
  => (c -> f a)
  -> (a -> k)
  -> (b -> a -> b)
  -> b
  -> M.Map k b
  -> c
  -> M.Map k b
aggregateGeneral unpack getKey combine initial m x =
  let aggregate = FL.Fold (aggregateToMap getKey combine initial) m id
  in  FL.fold aggregate (unpack x)

aggregateMonoidalGeneral
  :: (Ord k, Foldable f, Monoid b)
  => (c -> f a)
  -> (a -> k)
  -> (a -> b)
  -> MM.MonoidalMap k b
  -> c
  -> MM.MonoidalMap k b
aggregateMonoidalGeneral unpack getKey getB m x =
  let aggregate = FL.Fold (aggregateToMonoidalMap getKey getB) m id
  in  FL.fold aggregate (unpack x)

-- Maybe is delightfully foldable!  
aggregateFiltered
  :: Ord k
  => (c -> Maybe a)
  -> (a -> k)
  -> (b -> a -> b)
  -> b
  -> M.Map k b
  -> c
  -> M.Map k b
aggregateFiltered = aggregateGeneral

-- Maybe is delightfully foldable!  
aggregateMonoidalFiltered
  :: (Ord k, Monoid b)
  => (c -> Maybe a)
  -> (a -> k)
  -> (a -> b)
  -> MM.MonoidalMap k b
  -> c
  -> MM.MonoidalMap k b
aggregateMonoidalFiltered = aggregateMonoidalGeneral

