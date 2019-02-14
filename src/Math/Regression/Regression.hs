{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}

module Math.Regression.Regression where

import qualified Math.HMatrixUtils                as HU

import qualified Colonnade                        as C
import qualified Control.Foldl                    as FL
import qualified Control.Foldl.Statistics         as FS
import qualified Data.Foldable                    as Foldable
import           Data.Function                    (on)
import qualified Data.List                        as List
import qualified Data.Profunctor                  as PF
import qualified Data.Text                        as T
import qualified Data.Vector.Storable             as V
import qualified Lucid                            as H
import qualified Lucid.Colonnade                  as C
import qualified Lucid.Html5                      as H
import qualified Statistics.Distribution          as S
import qualified Statistics.Distribution.Normal   as S
import qualified Statistics.Distribution.StudentT as S
import qualified Statistics.Types                 as S
import qualified System.PipesLogger               as SL
import qualified Text.Printf                      as TP


import           Numeric.LinearAlgebra            (( #> ), (<#), (<.>), (<\>))
import qualified Numeric.LinearAlgebra            as LA
import           Numeric.LinearAlgebra.Data       (Matrix, R, Vector)
import qualified Numeric.LinearAlgebra.Data       as LA

import           Data.Kind                        (Type)

-- NB: for normal errors, we will request the ci and get it.  For interval errors, we may request it but we can't produce
-- what we request, we can only produce what we have.  So we make an interface for both.  We take CL as input to predict
-- *and* return it as output, in the prediction.

class Predictor e a b p where
  predict :: p -> b -> S.Estimate e a -- do we need a predict :: (b, b) -> (a, a) for uncertain inputs?

predictFromEstimateAtConfidence :: (RealFrac a, Predictor S.NormalErr a b p) => Int -> p -> S.CL Double -> b -> (a, a)
predictFromEstimateAtConfidence dof p cl b =
  let S.Estimate pt (S.NormalErr sigma) = predict p b
      prob = S.confidenceLevel $ S.mkCLFromSignificance (S.significanceLevel cl /2)
      predCI = S.quantile (S.studentTUnstandardized (realToFrac dof) 0 (realToFrac $ sigma)) prob
  in (pt, realToFrac predCI)

data RegressionResult a = RegressionResult
                          {
                            parameterEstimates :: [S.Estimate S.NormalErr a]
                          , degreesOfFreedom   :: Int
                          , meanSquaredError   :: a
                          , rSquared           :: Double
                          , adjRSquared        :: Double
                          , covariances        :: Matrix Double
                          } deriving (Show)

instance (LA.Element a, LA.Numeric a, RealFloat a) => Predictor S.NormalErr a (Vector a) (RegressionResult a) where
  predict rr va =
    let sigmaPred = sqrt (va <.> ((LA.cmap realToFrac $ covariances rr) #> va))
        pred = va LA.<.> (V.fromList $ fmap S.estPoint $ parameterEstimates rr)
    in S.estimateNormErr pred sigmaPred

data NamedEstimate a = NamedEstimate { regressorName :: T.Text, regressorEstimate :: a, regressorCI :: a, regressorPI :: a }

namedEstimates :: [T.Text] -> RegressionResult R -> R -> [NamedEstimate R]
namedEstimates pNames res ciPct =
  let sigma e = S.normalError $ S.estError e
      dof = realToFrac $ degreesOfFreedom res
      ci e = 2 * S.quantile (S.normalDistr 0 (sigma e)) ciPct
      pi e = 2 * S.quantile (S.studentTUnstandardized dof 0 (sigma e)) (1 - (1 -ciPct)/2)
  in List.zipWith (\n e -> NamedEstimate n (S.estPoint e) (ci e) (pi e)) pNames (parameterEstimates res)

namedEstimatesColonnade :: R -> C.Colonnade C.Headed (NamedEstimate R) T.Text
namedEstimatesColonnade  ci = C.headed "parameter" regressorName
                           <> C.headed "estimate" (T.pack . TP.printf "%4.3f" . regressorEstimate)
                           <> C.headed ((T.pack $ TP.printf "%.0f" (100 * ci)) <> "% confidence") (T.pack . TP.printf "%4.3f" . regressorCI)
                           <> C.headed ((T.pack $ TP.printf "%.0f" (100 * ci)) <> "% prediction") (T.pack . TP.printf "%4.3f" . regressorPI)

namedSummaryStats :: RegressionResult R -> [(T.Text, R)]
namedSummaryStats r =
  [
    ("R-Squared",(rSquared r))
  , ("Adj. R-squared",(adjRSquared r))
  , ("Mean Squared Error",(meanSquaredError r))
  ]

namedSummaryStatsColonnade :: C.Colonnade C.Headed (T.Text,Double) T.Text
namedSummaryStatsColonnade  = C.headed "Summary Stat." fst <> C.headed "Value" (T.pack . TP.printf "%.2g" . snd)

prettyPrintRegressionResult :: T.Text -> [T.Text] -> RegressionResult R -> R -> T.Text
prettyPrintRegressionResult header xNames r ci =
  let nEsts = namedEstimates xNames r ci
      nSS = namedSummaryStats r
  in header <> "\n" <>
     (T.pack $ C.ascii (fmap T.unpack $ namedEstimatesColonnade ci) nEsts)
     <> (T.pack $ C.ascii (fmap T.unpack namedSummaryStatsColonnade) nSS)

prettyPrintRegressionResultHtml :: T.Text -> [T.Text] -> RegressionResult R -> R -> H.Html ()
prettyPrintRegressionResultHtml header xNames r ci = do
  let nEsts = namedEstimates xNames r ci
      nSS = namedSummaryStats r
      toCell t = C.Cell [H.style_ "border: 1px solid black"] (H.toHtmlRaw t)
  H.p_ (H.toHtmlRaw header)
  C.encodeCellTable [H.style_ "border: 1px solid black"] (fmap toCell $ namedEstimatesColonnade ci) nEsts
  C.encodeCellTable [H.style_ "border: 1px solid black"] (fmap toCell $ namedSummaryStatsColonnade) nSS


goodnessOfFit :: Int -> Vector R -> Vector R -> (R, R)
goodnessOfFit p vB vU =
  let n = LA.size vB
      meanB = LA.sumElements vB / (realToFrac $ LA.size vB)
      ssTot = let x = LA.cmap (\y -> y - meanB) vB in x <.> x
      ssRes = vU <.> vU
      rSq = 1 - (ssRes/ssTot)
      arSq = 1 - (1 - rSq)*(realToFrac $ (n - 1))/(realToFrac $ (n - p - 1))
  in (rSq, arSq)

estimates :: Matrix R -> Vector R -> [S.Estimate S.NormalErr R]
estimates cov means =
  let sigmas = LA.cmap sqrt (LA.takeDiag cov)
  in List.zipWith (\m s -> S.estimateNormErr m s) (LA.toList means) (LA.toList sigmas)

eickerHeteroscedasticityEstimator :: Monad m => Matrix R -> Vector R -> Vector R -> SL.Logger m (Matrix R)
eickerHeteroscedasticityEstimator mA vB vB' = do
  HU.checkVectorMatrix "b" "A" vB mA
  HU.checkVectorMatrix "b'" "A" vB' mA
  let u = vB - vB'
      diagU = LA.diag $ V.zipWith (*) u u
      mA' = LA.tr mA
      mC = LA.inv (mA' LA.<> mA)
  return $ mC LA.<> (mA' LA.<> diagU LA.<> mA) LA.<> mC
