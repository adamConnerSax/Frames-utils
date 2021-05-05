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

import qualified Math.HMatrixUtils             as HU

import qualified Colonnade                     as C

import qualified Data.List                     as List
import qualified Data.Text                     as T
import qualified Data.Vector.Storable          as V
import qualified Lucid                         as LH
import qualified Lucid.Colonnade               as LC
import qualified Statistics.Distribution       as S
import qualified Statistics.Distribution.FDistribution
                                               as S
import qualified Statistics.Distribution.StudentT
                                               as S
import qualified Statistics.Types              as S
import qualified Text.Blaze.Colonnade          as BC
import qualified Text.Blaze.Html5              as BH
import qualified Text.Blaze.Html5.Attributes   as BHA
import qualified Text.Printf                   as TP


import           Numeric.LinearAlgebra          ( (#>)
                                                , (<.>)
                                                )
import qualified Numeric.LinearAlgebra         as LA
import           Numeric.LinearAlgebra.Data     ( Matrix
                                                , R
                                                , Vector
                                                )
import qualified Say
-- NB: for normal errors, we will request the ci and get it.  For interval errors, we may request it but we can't produce
-- what we request, we can only produce what we have.  So we make an interface for both.  We take CL as input to predict
-- *and* return it as output, in the prediction.

class Predictor e a b p where
  predict :: p -> b -> S.Estimate e a -- do we need a predict :: (b, b) -> (a, a) for uncertain inputs?

predictFromEstimateAtConfidence
  :: (RealFrac a, Predictor S.NormalErr a b p)
  => Double
  -> p
  -> S.CL Double
  -> b
  -> (a, a)
predictFromEstimateAtConfidence dof p cl b =
  let
    S.Estimate pt (S.NormalErr sigma) = predict p b
    prob =
      S.confidenceLevel $ S.mkCLFromSignificance (S.significanceLevel cl / 2)
    predCI =
      2 * S.quantile (S.studentTUnstandardized dof 0 (realToFrac sigma)) prob
  in
    (pt, realToFrac predCI)

data RegressionResult a = RegressionResult
                          {
                            parameterEstimates :: [S.Estimate S.NormalErr a]
                          , degreesOfFreedom   :: Double -- since N may be an effective N
                          , meanSquaredError   :: a
                          , rSquared           :: Double
                          , adjRSquared        :: Double
                          , fStatistic         :: Maybe Double -- we can't compute this for fits with one parameter
                          , covariances        :: Matrix Double
                          } deriving (Show)

instance (LA.Element a, LA.Numeric a, RealFloat a) => Predictor S.NormalErr a (Vector a) (RegressionResult a) where
  predict rr va =
    let mse = meanSquaredError rr
        sigmaPred = sqrt (mse + va <.> (LA.cmap realToFrac (covariances rr) #> va))
        prediction = va LA.<.> V.fromList (S.estPoint <$> parameterEstimates rr)
    in S.estimateNormErr prediction sigmaPred

data NamedEstimate a = NamedEstimate { regressorName :: T.Text, regressorEstimate :: a, regressorCI :: a, regressorPValue :: a }

namedEstimates :: [T.Text] -> RegressionResult R -> S.CL R -> [NamedEstimate R]
namedEstimates pNames res cl =
  let prob = S.confidenceLevel
        $ S.mkCLFromSignificance (S.significanceLevel cl / 2)
      dof = degreesOfFreedom res
      sigma e = S.normalError $ S.estError e
      dist e = S.studentTUnstandardized dof 0 (sigma e)
      ci e = 2 * S.quantile (dist e) prob
      pValue e = S.complCumulative (dist e) (abs $ S.estPoint e)
  in  List.zipWith (\n e -> NamedEstimate n (S.estPoint e) (ci e) (pValue e))
                   pNames
                   (parameterEstimates res)

namedEstimatesColonnade
  :: S.CL R -> C.Colonnade C.Headed (NamedEstimate R) T.Text
namedEstimatesColonnade cl =
  C.headed "parameter" regressorName
    <> C.headed "estimate" (T.pack . TP.printf "%4.3f" . regressorEstimate)
    <> C.headed
         (  T.pack (TP.printf "%.0f" (100 * S.confidenceLevel cl))
         <> "% confidence"
         )
         (T.pack . TP.printf "%4.3f" . regressorCI)
    <> C.headed "p-value" (T.pack . TP.printf "%4.3f" . regressorPValue)

namedSummaryStats :: RegressionResult R -> [(T.Text, T.Text)]
namedSummaryStats r =
  let
    p             = List.length (parameterEstimates r)
    effN          = degreesOfFreedom r + realToFrac p
    d1            = realToFrac $ p - 1
    d2            = effN - realToFrac p
    pValM = fmap (S.complCumulative (S.fDistributionReal d1 d2)) (fStatistic r)
    printNum      = T.pack . TP.printf "%4.3f"
    printMaybeNum = maybe "N/A" printNum
  in
    [ ("R-Squared"         , printNum (rSquared r))
    , ("Adj. R-squared"    , printNum (adjRSquared r))
    , ("F-stat"            , printMaybeNum (fStatistic r))
    , ("p-value"           , printMaybeNum pValM)
    , ("Mean Squared Error", printNum (meanSquaredError r))
    ]

namedSummaryStatsColonnade :: C.Colonnade C.Headed (T.Text, T.Text) T.Text
namedSummaryStatsColonnade =
  C.headed "Summary Stat." fst <> C.headed "Value" snd

prettyPrintRegressionResult
  :: T.Text -> [T.Text] -> RegressionResult R -> S.CL R -> T.Text
prettyPrintRegressionResult header xNames r cl =
  let nEsts = namedEstimates xNames r cl
      nSS   = namedSummaryStats r
  in  header
      <> "\n"
      <> T.pack (C.ascii (T.unpack <$> namedEstimatesColonnade cl) nEsts)
      <> T.pack (C.ascii (fmap T.unpack namedSummaryStatsColonnade) nSS)

prettyPrintRegressionResultLucid
  :: T.Text -> [T.Text] -> RegressionResult R -> S.CL R -> LH.Html ()
prettyPrintRegressionResultLucid header xNames r cl = do
  let nEsts = namedEstimates xNames r cl
      nSS   = namedSummaryStats r
      toCell t = LC.Cell [LH.style_ "border: 1px solid black"] (LH.toHtmlRaw t)
  LH.div_
      [ LH.style_
          "display: inline-block; padding: 7px; border-collapse: collapse"
      ]
    $ do
        LH.span_ (LH.toHtmlRaw header)
        LC.encodeCellTable
          [LH.style_ "border: 1px solid black; border-collapse: collapse"]
          (toCell <$> namedEstimatesColonnade cl)
          nEsts
        LC.encodeCellTable
          [LH.style_ "border: 1px solid black; border-collapse: collapse"]
          (fmap toCell namedSummaryStatsColonnade)
          nSS

prettyPrintRegressionResultBlaze
  :: T.Text -> [T.Text] -> RegressionResult R -> S.CL R -> BH.Html
prettyPrintRegressionResultBlaze header xNames r cl = do
  let nEsts = namedEstimates xNames r cl
      nSS   = namedSummaryStats r
      toCell t = BC.Cell (BHA.style "border: 1px solid black") (BH.toHtml t)
  BH.div
    BH.! BHA.style
           "display: inline-block; padding: 7px; border-collapse: collapse"
    $    do
           BH.span (BH.toHtml header)
           BC.encodeCellTable
             (BHA.style "border: 1px solid black; border-collapse: collapse")
             (toCell <$> namedEstimatesColonnade cl)
             nEsts
           BC.encodeCellTable
             (BHA.style "border: 1px solid black; border-collapse: collapse")
             (fmap toCell namedSummaryStatsColonnade)
             nSS


data FitStatistics a = FitStatistics { fsRSquared :: a, fsAdjRSquared :: a, fsFStatistic :: Maybe a}

goodnessOfFit
  :: MonadIO m
  => Int
  -> Vector R
  -> Maybe (Vector R)
  -> Vector R
  -> m (FitStatistics R)
goodnessOfFit pInt vB vWM vU = do
  let
    n  = LA.size vB
    p  = realToFrac pInt
    vW = maybe (V.fromList $ List.replicate n (1.0 / realToFrac n))
               (\v -> LA.scale (1 / realToFrac (LA.sumElements v)) v)
               vWM
    mW     = LA.diag vW
    vWB    = mW #> vB
--      vWU = mW #> vU
    meanWB = LA.sumElements vWB
    effN   = 1 / (vW <.> vW)
    ssTot  = let x = LA.cmap (\y -> y - meanWB) vB in x <.> (mW #> x)
    ssRes  = vU <.> (mW #> vU) -- weighted ssq of residuals
    rSq    = 1 - (ssRes / ssTot)
    arSq =
      1 - (1 - rSq) * realToFrac (effN - 1) / realToFrac (effN - p - 1)
    fStatM = if pInt > 1
      then Just (((ssTot - ssRes) / (p - 1.0)) / (ssRes / (effN - p)))
      else Nothing
  Say.say $ "n=" <> show n
  Say.say $ "p=" <> show p
--  Log.log Log.Diagnostic $ "vW=" <> show vW
  Say.say $ "effN=" <> show effN
  Say.say $ "ssTot=" <> show ssTot
  Say.say $ "ssRes=" <> show ssRes
  return $ FitStatistics rSq arSq fStatM

estimates :: Matrix R -> Vector R -> [S.Estimate S.NormalErr R]
estimates cov means =
  let sigmas = LA.cmap sqrt (LA.takeDiag cov)
  in  List.zipWith S.estimateNormErr
                   (LA.toList means)
                   (LA.toList sigmas)

eickerHeteroscedasticityEstimator
  :: MonadIO m
  => Matrix R
  -> Vector R
  -> Vector R
  -> m (Matrix R)
eickerHeteroscedasticityEstimator mA vB vB' = do
  HU.checkVectorMatrix "b" "A" vB mA
  HU.checkVectorMatrix "b'" "A" vB' mA
  let u     = vB - vB'
      diagU = LA.diag $ V.zipWith (*) u u
      mA'   = LA.tr mA
      mC    = LA.inv (mA' LA.<> mA)
  return $ mC LA.<> (mA' LA.<> diagU LA.<> mA) LA.<> mC
