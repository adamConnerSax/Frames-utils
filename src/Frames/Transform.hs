{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.Transform
  (
    mutate
  , BinaryFunction(..)
  , bfApply
  , bfPlus
  , bfLHS
  , bfRHS
  ) where
--import Data.Functor.Identity (Identity(..))
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Derived as V
import Data.Vinyl.TypeLevel (type (++))
import Data.Vinyl.Functor (Lift(..), Identity(..))
import qualified Frames               as F
--import Frames.RecF (UnColumn)
--import qualified Data.Monoid.Endo     as E
import Frames.Melt (RDeleteAll)

import GHC.TypeLits (Symbol)

--  mutation functions
mutate :: forall rs as bs. (as F.⊆ rs, RDeleteAll as rs F.⊆ rs)
             => (F.Record as -> F.Record bs) -> F.Record rs -> F.Record (RDeleteAll as rs ++ bs)
mutate f xs = F.rcast @(RDeleteAll as rs) xs `F.rappend` f (F.rcast xs)

newtype BinaryFunction a = BinaryFunction { appBinaryFunction :: a -> a -> a } 

bfApply :: forall (rs :: [(Symbol,*)]). (V.RMap (V.Unlabeled rs), V.RApply (V.Unlabeled rs), V.StripFieldNames rs)
         => F.Rec BinaryFunction (V.Unlabeled rs) -> (F.Record rs -> F.Record rs -> F.Record rs)
bfApply binaryFunctions xs ys = V.withNames $ V.rapply applyLHS (V.stripNames ys) where
  applyLHS = V.rzipWith (\bf ia -> Lift (\ib -> Identity $ appBinaryFunction bf (getIdentity ia) (getIdentity ib))) binaryFunctions (V.stripNames xs)

bfPlus :: Num a => BinaryFunction a
bfPlus = BinaryFunction (+)

bfLHS :: BinaryFunction a
bfLHS = BinaryFunction const

bfRHS :: BinaryFunction a
bfRHS = BinaryFunction $ flip const
