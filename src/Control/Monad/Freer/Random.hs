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
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Control.Monad.Freer.Random
  (
    Random
  , sampleRVar
  , sampleDist
--  , Control.Monad.Freer.Random.rvar
  , runRandomInIO
  ) where

import qualified Data.Random as R
import qualified Data.Random.Internal.Source as R
import qualified Control.Monad.Freer         as FR

import           Control.Monad.IO.Class (MonadIO(..))

data Random r where
  SampleRVar ::  R.RVar t -> Random t 
  GetRandomPrim :: R.Prim t -> Random t 

sampleRVar :: (FR.Member Random effs) => R.RVar t -> FR.Eff effs t
sampleRVar = FR.send . SampleRVar

sampleDist :: (FR.Member Random effs, R.Distribution d t) => d t -> FR.Eff effs t
sampleDist = sampleRVar . R.rvar

getRandomPrim :: FR.Member Random effs => R.Prim t -> FR.Eff effs t
getRandomPrim = FR.send . GetRandomPrim

runRandomInIO :: forall effs a. MonadIO (FR.Eff effs) => FR.Eff (Random ': effs) a -> FR.Eff effs a
runRandomInIO = FR.interpret f where
  f :: forall x. (Random x -> FR.Eff effs x)
  f r = case r of
    SampleRVar dt -> liftIO $ R.sample dt
    GetRandomPrim pt -> liftIO $ R.getRandomPrim pt 



{-
runRandomInState :: forall s m effs a. s -> FR.Eff (Random s ': effs) a -> FR.Eff effs a
runRandomInState source =  

instance (FR.Member (Random s) effs) => R.RandomSource (FR.Eff effs) s where
  getRandomPrimFrom = getRandPrim

-}

instance FR.Member Random effs => R.MonadRandom (FR.Eff effs) where
  getRandomPrim = getRandomPrim


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
    
