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
module Frames.Regression where

import qualified Frames.Aggregations as FA
import qualified Frames.Transform as FT
import qualified Frames.VegaLite as FV
import qualified Math.Rescale as MR
import qualified Math.Regression.Regression as MR
import qualified Math.Regression.LeastSquares as MR
import qualified System.PipesLogger as SL

import qualified Colonnade                      as C
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
import qualified Data.Vector.Storable as VS
--import qualified Data.Vector.Unboxed as U
import           Data.Random.Source.PureMT as R
import           Data.Random as R
import           Data.Void (Void)

import qualified MachineLearning            as ML
import qualified MachineLearning.Regression as ML
import qualified Numeric.LinearAlgebra      as LA
import qualified Numeric.LinearAlgebra.Data as LA
import           Numeric.LinearAlgebra.Data (R)
import qualified Statistics.Types               as S


import GHC.TypeLits (Symbol)
import Data.Kind (Type)

type Unweighted = "unweighted" F.:-> Void

type family NonVoidField (f :: (Symbol, Type)) :: Bool where
  NonVoidField '(a,Void) = False
  NonVoidField _         = True

class BoolVal (w :: Bool) where
  asBool :: Bool

instance BoolVal 'True where
  asBool = True

instance BoolVal 'False where
  asBool = False

data FrameRegressionResult (y :: (Symbol, Type)) (wc :: Bool) (xs :: [(Symbol, Type)]) (w :: (Symbol,Type)) where
  FrameUnweightedRegressionResult :: (w ~ Unweighted) => MR.RegressionResult R -> FrameRegressionResult y wc xs Unweighted
  FrameWeightedRegressionResult :: (V.Snd w -> R) -> MR.RegressionResult R -> FrameRegressionResult y wc xs w

realRecToList :: ( RealFrac b
                 , V.RMap as
                 , V.RecordToList as
                 , V.ReifyConstraint Real F.ElField as
                 , V.NatToInt (V.RLength as))
  => F.Record as -> [b]
realRecToList = V.recordToList . V.rmap (\(V.Compose (V.Dict x)) -> V.Const $ realToFrac x) . V.reifyConstraint @Real 

instance ( MR.Predictor S.NormalErr a (LA.Vector a) (MR.RegressionResult R)
         , BoolVal wc
         , LA.Element a
         , LA.Numeric a
         , RealFloat a
         , xs F.⊆ rs
         , V.RMap xs
         , V.RecordToList xs
         , V.ReifyConstraint Real F.ElField xs
         , V.NatToInt (V.RLength xs)
         ) => MR.Predictor S.NormalErr a (F.Record rs) (FrameRegressionResult y wc xs w) where
  predict frr r =
    let rr = regressionResult frr
        addBias l = if asBool @wc then 1 : l else l -- if we regressed with a constant term, we need to put it into the xs for prediction
        va :: LA.Vector a = VS.fromList $ addBias $ realRecToList (F.rcast @xs r)        
    in MR.predict rr va

regressionResult :: FrameRegressionResult y wc xs w -> MR.RegressionResult R
regressionResult (FrameUnweightedRegressionResult x) = x
regressionResult (FrameWeightedRegressionResult _ x) = x

withConstant :: forall y wc xs w. (BoolVal wc) => FrameRegressionResult y wc xs w -> Bool  
withConstant _ = asBool @wc

weightedRegression :: forall y wc xs w. (BoolVal (NonVoidField w)) => FrameRegressionResult y wc xs w -> Bool
weightedRegression _ = asBool @(NonVoidField w)

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
               => f (F.Record rs) -> (LA.Matrix R, LA.Vector R)
prepRegression dat =
  let nCols = V.natToInt @(V.RLength as)
      yListF = PF.dimap (realToFrac . F.rgetField @y) List.reverse FL.list
      toListOfDoubles :: F.Record as -> [Double] = realRecToList
      mListF = PF.dimap (toListOfDoubles . F.rcast) (List.concat . List.reverse) FL.list
      (yList, mList) = FL.fold ((,) <$> yListF <*> mListF) dat
  in (LA.matrix nCols mList, LA.vector yList) 


-- make X, y, w from the data 
prepWeightedRegression :: forall y as w f rs. ( Foldable f
                                              , as F.⊆ rs
                                              , F.ElemOf rs y
                                              , FA.RealField y
                                              , F.ElemOf rs w
                                              , FA.RealField w
                                              , V.AllConstrained (FA.RealFieldOf rs) as
                                              , V.RMap as
                                              , V.RecordToList as
                                              , V.ReifyConstraint Real F.ElField as
                                              , V.NatToInt (V.RLength as))
               => f (F.Record rs) -> (LA.Matrix R, LA.Vector R, LA.Vector R)
prepWeightedRegression dat =
  let nCols = V.natToInt @(V.RLength as)
      yListF = PF.dimap (realToFrac . F.rgetField @y) List.reverse FL.list
      wListF = PF.dimap (realToFrac . F.rgetField @w) List.reverse FL.list
      toListOfDoubles :: F.Record as -> [Double] = realRecToList
      mListF = PF.dimap (toListOfDoubles . F.rcast) (List.concat . List.reverse) FL.list
      (yList, mList, wList) = FL.fold ((,,) <$> yListF <*> mListF <*> wListF) dat
  in (LA.matrix nCols mList, LA.vector yList, LA.vector wList) 


prettyPrintRegressionResult :: forall y wc as w. ( F.ColumnHeaders '[y]
                                                 , F.ColumnHeaders '[w]
                                                 , F.ColumnHeaders as
                                                 , BoolVal wc)
  => (T.Text -> T.Text -> T.Text) -> FrameRegressionResult y wc as w -> Double -> T.Text
prettyPrintRegressionResult headerF res ci = 
  let yName = FV.colName @y
      wName = FV.colName @w
      xNames' = fmap T.pack $ F.columnHeaders (Proxy :: Proxy (F.Record as))
      xNames = if withConstant res then "intercept" : xNames' else xNames'
  in MR.prettyPrintRegressionResult (headerF yName wName) xNames (regressionResult res) ci

prettyPrintRegressionResults :: forall y wc as w ks f. ( F.ColumnHeaders '[y]
                                                       , F.ColumnHeaders '[w]
                                                       , F.ColumnHeaders as
                                                       , BoolVal wc
                                                       , BoolVal (NonVoidField w)
                                                       , V.ReifyConstraint Show F.ElField ks
                                                       , V.RecordToList ks
                                                       , V.RMap ks
                                                       , Foldable f)
  => f (F.Record ks, FrameRegressionResult y wc as w) -> R -> T.Text
prettyPrintRegressionResults keyed ci =
  let keyValuesAsText rks = V.recordToList . V.rmap (\(V.Compose (V.Dict x)) -> V.Const $ T.pack $ show x) $ V.reifyConstraint @Show rks
      namedKeys rks = T.intercalate "; " $ keyValuesAsText rks
      headerF res keyRec yName wName = case weightedRegression res of
        True -> "Explaining " <> yName <> " (" <> namedKeys keyRec <> "; weights from " <> wName <> ")"     
        False -> "Explaining " <> yName <> " (" <> namedKeys keyRec <> ")"     
  in FL.fold (FL.Fold (\t (rk,res) -> t <> prettyPrintRegressionResult (headerF res rk) res ci) "\n" id) keyed

-- explain y in terms of as
leastSquaresByMinimization :: forall y as rs f. (Foldable f
                                                , as F.⊆ rs
                                                , F.ElemOf rs y
                                                , FA.RealField y
                                                , V.AllConstrained (FA.RealFieldOf rs) as
                                                , V.RMap as
                                                , V.RecordToList as
                                                , V.ReifyConstraint Real F.ElField as
                                                , V.NatToInt (V.RLength as))
                           => Bool -> [R] -> f (F.Record rs) -> [R]
leastSquaresByMinimization withConstant guess dat =
  let (mX,y) = prepRegression @y @as dat
      mX1 = if withConstant then ML.addBiasDimension mX else mX
      (solution, _) = ML.minimize (ML.ConjugateGradientFR 0.1 0.1) ML.LeastSquares (0.001) 20 ML.RegNone mX1 y (LA.fromList guess)
  in LA.toList solution


ordinaryLeastSquares :: forall y wc as rs f m. ( Monad m
                                               , Foldable f
                                               , as F.⊆ rs
                                               , F.ElemOf rs y
                                               , FA.RealField y
                                               , V.AllConstrained (FA.RealFieldOf rs) as
                                               , V.RMap as
                                               , V.RecordToList as
                                               , V.ReifyConstraint Real F.ElField as
                                               , BoolVal wc
                                               , V.NatToInt (V.RLength as))
                     =>  f (F.Record rs) -> SL.Logger m (FrameRegressionResult y wc as Unweighted)
ordinaryLeastSquares dat = do
  let (mA, vB) = prepRegression @y @as dat
      withConstant = asBool @wc
  FrameUnweightedRegressionResult <$> MR.ordinaryLS withConstant mA vB

weightedLeastSquares :: forall y wc as w rs f m. ( Monad m
                                                 , Foldable f
                                                 , as F.⊆ rs
                                                 , F.ElemOf rs y
                                                 , FA.RealField y
                                                 , F.ElemOf rs w
                                                 , FA.RealField w
                                                 , BoolVal wc
                                                 , V.AllConstrained (FA.RealFieldOf rs) as
                                                 , V.RMap as
                                                 , V.RecordToList as
                                                 , V.ReifyConstraint Real F.ElField as
                                                 , V.NatToInt (V.RLength as))
                     =>  f (F.Record rs) -> SL.Logger m (FrameRegressionResult y wc as w)
weightedLeastSquares dat = do
  let (mA, vB, vW) = prepWeightedRegression @y @as @w dat
      withConstant = asBool @wc
  FrameWeightedRegressionResult realToFrac <$> MR.weightedLS withConstant mA vB vW  

-- special case when weights come from observations being population averages of different populations
popWeightedLeastSquares :: forall y wc as w rs f m .( Monad m
                                                    , Foldable f
                                                    , as F.⊆ rs
                                                    , F.ElemOf rs y
                                                    , FA.RealField y
                                                    , F.ElemOf rs w
                                                    , FA.RealField w
                                                    , BoolVal wc
                                                    , V.AllConstrained (FA.RealFieldOf rs) as
                                                    , V.RMap as
                                                    , V.RecordToList as
                                                    , V.ReifyConstraint Real F.ElField as
                                                    , V.NatToInt (V.RLength as))
                        =>  f (F.Record rs) -> SL.Logger m (FrameRegressionResult y wc as w)
popWeightedLeastSquares dat = do
  let (mA, vB, vW) = prepWeightedRegression @y @as @w dat
      withConstant = asBool @wc
      vWpop = LA.cmap sqrt vW -- this is the correct weight for population average, the sqrt of the number averaged in that sample 
  FrameWeightedRegressionResult (sqrt . realToFrac) <$> MR.weightedLS withConstant mA vB vWpop  


-- special case when we know residuals are heteroscedastic with variances proportional to given numbers
varWeightedLeastSquares :: forall y wc as w rs f m .( Monad m
                                                    , Foldable f
                                                    , as F.⊆ rs
                                                    , F.ElemOf rs y
                                                    , FA.RealField y
                                                    , F.ElemOf rs w
                                                    , FA.RealField w
                                                    , BoolVal wc
                                                    , V.AllConstrained (FA.RealFieldOf rs) as
                                                    , V.RMap as
                                                    , V.RecordToList as
                                                    , V.ReifyConstraint Real F.ElField as
                                                    , V.NatToInt (V.RLength as))
                        =>  f (F.Record rs) -> SL.Logger m (FrameRegressionResult y wc as w)
varWeightedLeastSquares dat = do
  let (mA, vB, vW) = prepWeightedRegression @y @as @w dat
      withConstant = asBool @wc
      vWvar = LA.cmap (\x -> 1/sqrt x) vW -- this is the correct weight for given variance
  FrameWeightedRegressionResult (\x -> 1/(sqrt $ realToFrac x)) <$> MR.weightedLS withConstant mA vB vWvar  

totalLeastSquares :: forall y wc as rs f m .( Monad m
                                            , Foldable f
                                            , as F.⊆ rs
                                            , F.ElemOf rs y
                                            , FA.RealField y
                                            , BoolVal wc
                                            , V.AllConstrained (FA.RealFieldOf rs) as
                                            , V.RMap as
                                            , V.RecordToList as
                                            , V.ReifyConstraint Real F.ElField as
                                            , V.NatToInt (V.RLength as))
                     =>  f (F.Record rs) -> SL.Logger m (FrameRegressionResult y wc as Unweighted)
totalLeastSquares dat = do
  let (mA, vB) = prepRegression @y @as dat
      withConstant = asBool @wc
  FrameUnweightedRegressionResult <$> MR.totalLS withConstant mA vB


weightedTLS :: forall y wc as w rs f m .( Monad m
                                        , Foldable f
                                        , as F.⊆ rs
                                        , F.ElemOf rs y
                                        , FA.RealField y
                                        , BoolVal wc
                                        , F.ElemOf rs w
                                        , FA.RealField w
                                        , V.AllConstrained (FA.RealFieldOf rs) as
                                        , V.RMap as
                                        , V.RecordToList as
                                        , V.ReifyConstraint Real F.ElField as
                                        , V.NatToInt (V.RLength as))
            =>  f (F.Record rs) -> SL.Logger m (FrameRegressionResult y wc as w)
weightedTLS dat = do
  let (mA, vB, vW) = prepWeightedRegression @y @as @w dat
      withConstant = asBool @wc  
  FrameWeightedRegressionResult realToFrac <$> MR.weightedTLS withConstant mA vB vW

popWeightedTLS :: forall y wc as w rs f m .( Monad m
                                           , Foldable f
                                           , as F.⊆ rs
                                           , F.ElemOf rs y
                                           , FA.RealField y
                                           , BoolVal wc
                                           , F.ElemOf rs w
                                           , FA.RealField w
                                           , V.AllConstrained (FA.RealFieldOf rs) as
                                           , V.RMap as
                                           , V.RecordToList as
                                           , V.ReifyConstraint Real F.ElField as
                                           , V.NatToInt (V.RLength as))
               =>  f (F.Record rs) -> SL.Logger m (FrameRegressionResult y wc as w)
popWeightedTLS dat = do
  let (mA, vB, vW) = prepWeightedRegression @y @as @w dat
      withConstant = asBool @wc  
      vWpop = LA.cmap sqrt vW -- this is the correct weight for population average, the sqrt of the number averaged in that sample 
  FrameWeightedRegressionResult (sqrt . realToFrac) <$> MR.weightedTLS withConstant mA vB vWpop

varWeightedTLS :: forall y wc as w rs f m .( Monad m
                                           , Foldable f
                                           , as F.⊆ rs
                                           , F.ElemOf rs y
                                           , FA.RealField y
                                           , BoolVal wc
                                           , F.ElemOf rs w
                                           , FA.RealField w                                        
                                           , V.AllConstrained (FA.RealFieldOf rs) as
                                           , V.RMap as
                                           , V.RecordToList as
                                           , V.ReifyConstraint Real F.ElField as
                                           , V.NatToInt (V.RLength as))
            =>  f (F.Record rs) -> SL.Logger m (FrameRegressionResult y wc as w)
varWeightedTLS dat = do
  let (mA, vB, vW) = prepWeightedRegression @y @as @w dat
      withConstant = asBool @wc    
      vWvar = LA.cmap (\x -> 1/sqrt x) vW -- this is the correct weight for given variance
  FrameWeightedRegressionResult (\x -> 1/(sqrt $ realToFrac x)) <$> MR.weightedTLS withConstant mA vB vWvar

  
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

