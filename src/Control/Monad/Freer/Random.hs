{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Control.Monad.Freer.Random
  (
    Random
  , Control.Monad.Freer.Random.rvar
  , runRandomInBase
  ) where

import Data.Random as R
import Data.Random.Internal.Source as R
import qualified Control.Monad.Freer         as FR
import qualified Control.Monad.Freer.Writer  as FR

data Random r where
  RVar :: R.RVar a -> Random a

rvar :: FR.Member Random effs => R.RVar a -> FR.Eff effs a
rvar = FR.send . RVar

runRandomInBase :: forall m s effs a. (R.RandomSource m s, FR.LastMember m effs)
  => s -> FR.Eff (Random ': effs) a -> FR.Eff effs a
runRandomInBase s = FR.interpretM f where
  f :: forall x. (Random x -> m x)
  f r = case r of
    RVar rv -> runRVar rv s

instance (R.MonadRandom m, FR.LastMember m effs) => R.MonadRandom (FR.Eff effs) where
  getRandomPrim = FR.sendM . R.getRandomPrim


{-
data Random r where
  GetRandomPrim   :: R.Prim t -> Random t
  
getRandom :: FR.Member Random effs => FR.Eff effs a
getRandom = FR.send GetRandom

getRandoms :: FR.Member Random effs => FR.Eff effs [a]
getRandoms = FR.send GetRandoms

getRandomR :: FR.Member Random effs => (a,a) -> FR.Eff effs a
getRandomR = FR.send . GetRandomR

getRandomRs :: FR.Member Random effs => (a,a) -> FR.Eff effs [a]
getRandomRs = FR.send . GetRandomRs

runRandomBase :: (R.MonadRandom m, FR.Lastmember m effs) => FR.Eff (Random ': effs) a -> FR.Eff effs a
runRandomBase = interpretM f where
  f r = case r of
    GetRandom -> R.getRandom
    GetRanoms -> R.getRandoms
    GetRandomR -> R.getRandomR
    GetRandomRs -> R.getRandomRs

-}
    
