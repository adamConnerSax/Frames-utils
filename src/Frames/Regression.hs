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
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE UndecidableInstances      #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE UndecidableSuperClasses #-}
module Frames.Regression
  (
    leastSquaresRegression
  ) where

import qualified Frames.Aggregations as FA
import qualified Frames.Transform as FT
import qualified Math.Rescale as MR
import qualified System.PipesLogger as SL

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
import qualified Frames.Melt           as F
import qualified Pipes                as P
import qualified Pipes.Prelude        as P
import           Control.Arrow (second)
import           Data.Proxy (Proxy(..))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import           Data.Random.Source.PureMT as R
import           Data.Random as R

import qualified MachineLearning            as ML
import qualified MachineLearning.Regression as ML
import qualified Numeric.LinearAlgebra.Data as HM
import           Numeric.LinearAlgebra.Data (R)

-- make X, y from the data 
prepRegression :: forall y as f rs. ( Foldable f
                                    , as F.⊆ rs
                                    , F.ElemOf rs y
                                    , FA.RealField y
                                    , V.AllConstrained (FA.RealFieldOf rs) as
                                    , V.RMap as
                                    , V.RecordToList as
                                    , V.ReifyConstraint Real F.ElField as
                                    , V.NatToInt (V.RLength as))
               => f (F.Record rs) -> (HM.Matrix R, HM.Vector R)
prepRegression dat =
  let nCols = V.natToInt @(V.RLength as)
      yListF = PF.dimap (realToFrac . F.rgetField @y) List.reverse FL.list
      toListOfDoubles :: F.Record as -> [Double]
      toListOfDoubles xs = V.recordToList . V.rmap (\(V.Compose (V.Dict x)) -> V.Const $ realToFrac x) $ V.reifyConstraint @Real xs 
      mListF = PF.dimap (toListOfDoubles . F.rcast) (List.concat . List.reverse) FL.list
      (yList, mList) = FL.fold ((,) <$> yListF <*> mListF) dat
  in (HM.matrix nCols mList, HM.vector yList) 


-- explain y in terms of as
leastSquaresRegression :: forall y as rs f. (Foldable f
                                            , as F.⊆ rs
                                            , F.ElemOf rs y
                                            , FA.RealField y
                                            , V.AllConstrained (FA.RealFieldOf rs) as
                                            , V.RMap as
                                            , V.RecordToList as
                                            , V.ReifyConstraint Real F.ElField as
                                            , V.NatToInt (V.RLength as))
                       => [R] -> f (F.Record rs) -> [R]
leastSquaresRegression guess dat =
  let (mX,y) = prepRegression @y @as dat
      mX1 = ML.addBiasDimension mX
      (solution, _) = ML.minimize (ML.ConjugateGradientFR 0.1 0.1) ML.LeastSquares (0.001) 20 ML.RegNone mX1 y (HM.fromList guess)
  in HM.toList solution

{-
-- this is sort of ugly but I think it will work.  And it has a pretty narrow purpose
-- But what will we do when we want logistic regression or whatever?
class (V.AllConstrained RealFrac (V.Unlabeled rs), Real a) => RealFracRecordFromList a rs where
  recordFromList :: [a] -> Maybe (F.Record rs)

instance Real a => RealFracRecordFromList a '[] where
  recordFromList _ = Just $ V.RNil

instance ( V.AllConstrained RealFrac (V.Unlabeled (r : rs))
         , V.AllConstrained RealFrac (V.Unlabeled rs)
         , RealFracRecordFromList a rs
         , V.KnownField r
         , RealFrac (V.Snd r)
         , Real a) => RealFracRecordFromList a (r : rs) where
  recordFromList [] = Nothing
  recordFromList (a : as) = case recordFromList as of
    Nothing -> Nothing 
    Just xs -> let x = (realToFrac a) :: V.Snd r in Just $ x &: xs
-}

