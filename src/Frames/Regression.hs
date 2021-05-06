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

import qualified Frames.Misc                  as FU
import qualified Frames.VegaLite.Utils         as FV
import qualified Math.Regression.Regression    as MR
import qualified Math.Regression.LeastSquares  as MR

import qualified Lucid                         as LH
import qualified Text.Blaze.Html               as BH
import qualified Control.Foldl                 as FL
import qualified Data.List                     as List
import qualified Data.Text                     as T
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.Functor            as V
import qualified Data.Vinyl.TypeLevel          as V
import           Data.Profunctor               as PF
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Data.Vector.Storable          as VS

import qualified MachineLearning               as ML
import qualified MachineLearning.Regression    as ML
import qualified Numeric.LinearAlgebra         as LA
import           Numeric.LinearAlgebra.Data     ( R )
import qualified Statistics.Types              as S

import           GHC.TypeLits                   ( Symbol )

type Unweighted = "unweighted" F.:-> Void

type family NonVoidField (f :: (Symbol, Type)) :: Bool where
  NonVoidField '(a,Void) = 'False
  NonVoidField _         = 'True

class BoolVal (w :: Bool) where
  asBool :: Bool

instance BoolVal 'True where
  asBool = True

instance BoolVal 'False where
  asBool = False

data FrameRegressionResult (y :: (Symbol, Type)) (wc :: Bool) (xs :: [(Symbol, Type)]) (w :: (Symbol,Type)) (rs :: [(Symbol, Type)]) where
  FrameUnweightedRegressionResult :: (w ~ Unweighted)
    => MR.RegressionResult R -> FrameRegressionResult y wc xs Unweighted rs
  FrameWeightedRegressionResult :: (V.KnownField w, F.ElemOf rs w)
    => (V.Snd w -> R) -> MR.RegressionResult R -> FrameRegressionResult y wc xs w rs

realRecToList
  :: ( RealFrac b
     , V.RMap as
     , V.RecordToList as
     , V.ReifyConstraint Real F.ElField as
     , V.NatToInt (V.RLength as)
     )
  => F.Record as
  -> [b]
realRecToList =
  V.recordToList
    . V.rmap (\(V.Compose (V.Dict x)) -> V.Const $ realToFrac x)
    . V.reifyConstraint @Real

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
         ) => MR.Predictor S.NormalErr a (F.Record rs) (FrameRegressionResult y wc xs w as) where
  predict frr r =
    let rr = regressionResult frr
        addBias l = if asBool @wc then 1 : l else l -- if we regressed with a constant term, we need to put it into the xs for prediction
        va :: LA.Vector a = VS.fromList $ addBias $ realRecToList (F.rcast @xs r)
    in MR.predict rr va

regressionResult :: FrameRegressionResult y wc xs w rs -> MR.RegressionResult R
regressionResult (FrameUnweightedRegressionResult x) = x
regressionResult (FrameWeightedRegressionResult _ x) = x

withConstant
  :: forall y wc xs w rs
   . (BoolVal wc)
  => FrameRegressionResult y wc xs w rs
  -> Bool
withConstant _ = asBool @wc

weightedRegression
  :: forall y wc xs w rs
   . (BoolVal (NonVoidField w))
  => FrameRegressionResult y wc xs w rs
  -> Bool
weightedRegression _ = asBool @(NonVoidField w)

-- make X, y from the data
prepRegression
  :: forall y as f rs
   . ( Foldable f
     , as F.⊆ rs
     , F.ElemOf rs y
     , FU.RealField y
     , V.AllConstrained (FU.RealFieldOf rs) as
     , V.RMap as
     , V.RecordToList as
     , V.ReifyConstraint Real F.ElField as
     , V.NatToInt (V.RLength as)
     )
  => f (F.Record rs)
  -> (LA.Matrix R, LA.Vector R)
prepRegression dat =
  let
    nCols  = V.natToInt @(V.RLength as)
    yListF = PF.dimap (realToFrac . F.rgetField @y) List.reverse FL.list
    toListOfDoubles :: F.Record as -> [Double] = realRecToList
    mListF = PF.dimap (toListOfDoubles . F.rcast)
                      (List.concat . List.reverse)
                      FL.list
    (yList, mList) = FL.fold ((,) <$> yListF <*> mListF) dat
  in
    (LA.matrix nCols mList, LA.vector yList)


-- make X, y, w from the data
prepWeightedRegression
  :: forall y as w f rs
   . ( Foldable f
     , as F.⊆ rs
     , F.ElemOf rs y
     , FU.RealField y
     , F.ElemOf rs w
     , FU.RealField w
     , V.AllConstrained (FU.RealFieldOf rs) as
     , V.RMap as
     , V.RecordToList as
     , V.ReifyConstraint Real F.ElField as
     , V.NatToInt (V.RLength as)
     )
  => f (F.Record rs)
  -> (LA.Matrix R, LA.Vector R, LA.Vector R)
prepWeightedRegression dat =
  let
    nCols  = V.natToInt @(V.RLength as)
    yListF = PF.dimap (realToFrac . F.rgetField @y) List.reverse FL.list
    wListF = PF.dimap (realToFrac . F.rgetField @w) List.reverse FL.list
    toListOfDoubles :: F.Record as -> [Double] = realRecToList
    mListF = PF.dimap (toListOfDoubles . F.rcast)
                      (List.concat . List.reverse)
                      FL.list
    (yList, mList, wList) = FL.fold ((,,) <$> yListF <*> mListF <*> wListF) dat
  in
    (LA.matrix nCols mList, LA.vector yList, LA.vector wList)


prettyPrintRegressionResult
  :: forall y wc as w rs
   . ( F.ColumnHeaders '[y]
     , F.ColumnHeaders '[w]
     , F.ColumnHeaders as
     , BoolVal wc
     )
  => (T.Text -> T.Text -> T.Text)
  -> FrameRegressionResult y wc as w rs
  -> S.CL Double
  -> T.Text
prettyPrintRegressionResult headerF res cl =
  let yName   = FV.colName @y
      wName   = FV.colName @w
      xNames' = T.pack <$> F.columnHeaders (Proxy :: Proxy (F.Record as))
      xNames  = if withConstant res then "intercept" : xNames' else xNames'
  in  MR.prettyPrintRegressionResult (headerF yName wName)
                                     xNames
                                     (regressionResult res)
                                     cl

prettyPrintRegressionResultLucid
  :: forall y wc as w rs
   . ( F.ColumnHeaders '[y]
     , F.ColumnHeaders '[w]
     , F.ColumnHeaders as
     , BoolVal wc
     )
  => (T.Text -> T.Text -> T.Text)
  -> FrameRegressionResult y wc as w rs
  -> S.CL Double
  -> LH.Html ()
prettyPrintRegressionResultLucid headerF res cl =
  let yName   = FV.colName @y
      wName   = FV.colName @w
      xNames' = T.pack <$> F.columnHeaders (Proxy :: Proxy (F.Record as))
      xNames  = if withConstant res then "intercept" : xNames' else xNames'
  in  MR.prettyPrintRegressionResultLucid (headerF yName wName)
                                          xNames
                                          (regressionResult res)
                                          cl

prettyPrintRegressionResultBlaze
  :: forall y wc as w rs
   . ( F.ColumnHeaders '[y]
     , F.ColumnHeaders '[w]
     , F.ColumnHeaders as
     , BoolVal wc
     )
  => (T.Text -> T.Text -> T.Text)
  -> FrameRegressionResult y wc as w rs
  -> S.CL Double
  -> BH.Html
prettyPrintRegressionResultBlaze headerF res cl =
  let yName   = FV.colName @y
      wName   = FV.colName @w
      xNames' = T.pack <$> F.columnHeaders (Proxy :: Proxy (F.Record as))
      xNames  = if withConstant res then "intercept" : xNames' else xNames'
  in  MR.prettyPrintRegressionResultBlaze (headerF yName wName)
                                          xNames
                                          (regressionResult res)
                                          cl


keyRecordText
  :: (V.ReifyConstraint Show F.ElField ks, V.RecordToList ks, V.RMap ks)
  => F.Record ks
  -> T.Text
keyRecordText keyRec =
  let keyValuesAsText rks =
        V.recordToList
          . V.rmap (\(V.Compose (V.Dict x)) -> V.Const $ T.pack $ show x)
          $ V.reifyConstraint @Show rks
  in  T.intercalate "; " $ keyValuesAsText keyRec

prettyPrintRegressionResults
  :: forall y wc as w rs k f a
   . ( F.ColumnHeaders '[y]
     , F.ColumnHeaders '[w]
     , F.ColumnHeaders as
     , BoolVal wc
     , BoolVal (NonVoidField w)
     , Foldable f
     , Monoid a
     )
  => (k -> T.Text)
  -> f (k, FrameRegressionResult y wc as w rs)
  -> S.CL R
  -> (  (T.Text -> T.Text -> T.Text)
     -> FrameRegressionResult y wc as w rs
     -> S.CL Double
     -> a
     )
  -> a
  -> a
prettyPrintRegressionResults keyText keyed cl printOne sepEach =
  let headerF res key yName wName = if weightedRegression res
                                    then
                                      "Explaining "
                                        <> yName
                                        <> " ("
                                        <> keyText key
                                        <> "; weights from "
                                        <> wName
                                        <> ")"
                                    else "Explaining " <> yName <> " (" <> keyText key <> ")"
  in  FL.fold
        (FL.Fold (\t (rk, res) -> t <> printOne (headerF res rk) res cl)
                 sepEach
                 id
        )
        keyed

-- explain y in terms of as
leastSquaresByMinimization
  :: forall y as rs f
   . ( Foldable f
     , as F.⊆ rs
     , F.ElemOf rs y
     , FU.RealField y
     , V.AllConstrained (FU.RealFieldOf rs) as
     , V.RMap as
     , V.RecordToList as
     , V.ReifyConstraint Real F.ElField as
     , V.NatToInt (V.RLength as)
     )
  => Bool
  -> [R]
  -> f (F.Record rs)
  -> [R]
leastSquaresByMinimization wc guess dat =
  let (mX, y)       = prepRegression @y @as dat
      mX1           = if wc then ML.addBiasDimension mX else mX
      (solution, _) = ML.minimize (ML.ConjugateGradientFR 0.1 0.1)
                                  ML.LeastSquares
                                  0.001
                                  20
                                  ML.RegNone
                                  mX1
                                  y
                                  (LA.fromList guess)
  in  LA.toList solution


ordinaryLeastSquares
  :: forall y wc as rs f m
   . ( Foldable f
     , MonadIO m
     , as F.⊆ rs
     , F.ElemOf rs y
     , FU.RealField y
     , V.AllConstrained (FU.RealFieldOf rs) as
     , V.RMap as
     , V.RecordToList as
     , V.ReifyConstraint Real F.ElField as
     , BoolVal wc
     , V.NatToInt (V.RLength as)
     )
  => f (F.Record rs)
  -> m (FrameRegressionResult y wc as Unweighted rs)
ordinaryLeastSquares dat = do
  let (mA, vB)  = prepRegression @y @as dat
      withConst = asBool @wc
  FrameUnweightedRegressionResult <$> MR.ordinaryLS withConst mA vB

weightedLeastSquares
  :: forall y wc as w rs f m
   . ( MonadIO m
     , Foldable f
     , as F.⊆ rs
     , F.ElemOf rs y
     , FU.RealField y
     , F.ElemOf rs w
     , FU.RealField w
     , BoolVal wc
     , V.AllConstrained (FU.RealFieldOf rs) as
     , V.RMap as
     , V.RecordToList as
     , V.ReifyConstraint Real F.ElField as
     , V.NatToInt (V.RLength as)
     )
  => f (F.Record rs)
  -> m (FrameRegressionResult y wc as w rs)
weightedLeastSquares dat = do
  let (mA, vB, vW) = prepWeightedRegression @y @as @w dat
      withConst    = asBool @wc
  FrameWeightedRegressionResult realToFrac <$> MR.weightedLS withConst mA vB vW

-- special case when weights come from observations being population averages of different populations
popWeightedLeastSquares
  :: forall m y wc as w rs f
   . ( MonadIO m
     , Foldable f
     , as F.⊆ rs
     , F.ElemOf rs y
     , FU.RealField y
     , F.ElemOf rs w
     , FU.RealField w
     , BoolVal wc
     , V.AllConstrained (FU.RealFieldOf rs) as
     , V.RMap as
     , V.RecordToList as
     , V.ReifyConstraint Real F.ElField as
     , V.NatToInt (V.RLength as)
     )
  => f (F.Record rs)
  -> m (FrameRegressionResult y wc as w rs)
popWeightedLeastSquares dat = do
  let (mA, vB, vW) = prepWeightedRegression @y @as @w dat
      withConst    = asBool @wc
      vWpop        = LA.cmap sqrt vW -- this is the correct weight for population average, the sqrt of the number averaged in that sample
  FrameWeightedRegressionResult (sqrt . realToFrac)
    <$> MR.weightedLS withConst mA vB vWpop


-- special case when we know residuals are heteroscedastic with variances proportional to given numbers
varWeightedLeastSquares
  :: forall m y wc as w rs f
   . ( MonadIO m
     , Foldable f
     , as F.⊆ rs
     , F.ElemOf rs y
     , FU.RealField y
     , F.ElemOf rs w
     , FU.RealField w
     , BoolVal wc
     , V.AllConstrained (FU.RealFieldOf rs) as
     , V.RMap as
     , V.RecordToList as
     , V.ReifyConstraint Real F.ElField as
     , V.NatToInt (V.RLength as)
     )
  => f (F.Record rs)
  -> m (FrameRegressionResult y wc as w rs)
varWeightedLeastSquares dat = do
  let (mA, vB, vW) = prepWeightedRegression @y @as @w dat
      withConst    = asBool @wc
      vWvar        = LA.cmap (\x -> 1 / sqrt x) vW -- this is the correct weight for given variance
  FrameWeightedRegressionResult (\x -> 1 / sqrt (realToFrac x))
    <$> MR.weightedLS withConst mA vB vWvar

totalLeastSquares
  :: forall m y wc as rs f
   . ( MonadIO m
     , Foldable f
     , as F.⊆ rs
     , F.ElemOf rs y
     , FU.RealField y
     , BoolVal wc
     , V.AllConstrained (FU.RealFieldOf rs) as
     , V.RMap as
     , V.RecordToList as
     , V.ReifyConstraint Real F.ElField as
     , V.NatToInt (V.RLength as)
     )
  => f (F.Record rs)
  -> m (FrameRegressionResult y wc as Unweighted rs)
totalLeastSquares dat = do
  let (mA, vB)  = prepRegression @y @as dat
      withConst = asBool @wc
  FrameUnweightedRegressionResult <$> MR.totalLS withConst mA vB


weightedTLS
  :: forall m y wc as w rs f
   . ( MonadIO m
     , Foldable f
     , as F.⊆ rs
     , F.ElemOf rs y
     , FU.RealField y
     , BoolVal wc
     , F.ElemOf rs w
     , FU.RealField w
     , V.AllConstrained (FU.RealFieldOf rs) as
     , V.RMap as
     , V.RecordToList as
     , V.ReifyConstraint Real F.ElField as
     , V.NatToInt (V.RLength as)
     )
  => f (F.Record rs)
  -> m (FrameRegressionResult y wc as w rs)
weightedTLS dat = do
  let (mA, vB, vW) = prepWeightedRegression @y @as @w dat
      withConst    = asBool @wc
  FrameWeightedRegressionResult realToFrac <$> MR.weightedTLS withConst mA vB vW

popWeightedTLS
  :: forall m y wc as w rs f
   . ( MonadIO m
     , Foldable f
     , as F.⊆ rs
     , F.ElemOf rs y
     , FU.RealField y
     , BoolVal wc
     , F.ElemOf rs w
     , FU.RealField w
     , V.AllConstrained (FU.RealFieldOf rs) as
     , V.RMap as
     , V.RecordToList as
     , V.ReifyConstraint Real F.ElField as
     , V.NatToInt (V.RLength as)
     )
  => f (F.Record rs)
  -> m (FrameRegressionResult y wc as w rs)
popWeightedTLS dat = do
  let (mA, vB, vW) = prepWeightedRegression @y @as @w dat
      withConst    = asBool @wc
      vWpop        = LA.cmap sqrt vW -- this is the correct weight for population average, the sqrt of the number averaged in that sample
  FrameWeightedRegressionResult (sqrt . realToFrac)
    <$> MR.weightedTLS withConst mA vB vWpop

varWeightedTLS
  :: forall m y wc as w rs f
   . ( MonadIO m
     , Foldable f
     , as F.⊆ rs
     , F.ElemOf rs y
     , FU.RealField y
     , BoolVal wc
     , F.ElemOf rs w
     , FU.RealField w
     , V.AllConstrained (FU.RealFieldOf rs) as
     , V.RMap as
     , V.RecordToList as
     , V.ReifyConstraint Real F.ElField as
     , V.NatToInt (V.RLength as)
     )
  => f (F.Record rs)
  -> m (FrameRegressionResult y wc as w rs)
varWeightedTLS dat = do
  let (mA, vB, vW) = prepWeightedRegression @y @as @w dat
      withConst    = asBool @wc
      vWvar        = LA.cmap (\x -> 1 / sqrt x) vW -- this is the correct weight for given variance
  FrameWeightedRegressionResult (\x -> 1 / sqrt (realToFrac x))
    <$> MR.weightedTLS withConst mA vB vWvar


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
