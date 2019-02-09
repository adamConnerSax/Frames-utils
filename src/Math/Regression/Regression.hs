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

import qualified Math.HMatrixUtils              as HU

import qualified Colonnade                      as C
import qualified Control.Foldl                  as FL
import qualified Control.Foldl.Statistics       as FS
import qualified Data.Foldable                  as Foldable
import           Data.Function                  (on)
import qualified Data.List                      as List
import qualified Data.Profunctor                as PF
import qualified Data.Text                      as T
import qualified Data.Vector.Storable           as V
import qualified Statistics.Distribution        as S
import qualified Statistics.Distribution.Normal as S
import qualified Statistics.Types               as S
import qualified System.PipesLogger             as SL
import qualified Text.Printf                    as TP


import           Numeric.LinearAlgebra          (( #> ), (<#), (<.>), (<\>))
import qualified Numeric.LinearAlgebra          as LA
import           Numeric.LinearAlgebra.Data     (Matrix, R, Vector)
import qualified Numeric.LinearAlgebra.Data     as LA

data RegressionResult a = RegressionResult
                          {
                            parameterEstimates :: [S.Estimate S.NormalErr a]
                          , meanSquaredError   :: a
                          , rSquared           :: Double
                          , adjRSquared        :: Double
                          } deriving (Show)

data NamedEstimate a = NamedEstimate { regressorName :: T.Text, regressorEstimate :: a, regressorCI :: a }

namedEstimates :: [T.Text] -> RegressionResult R -> R -> [NamedEstimate R]
namedEstimates pNames res ciPct =
  let sigma e = S.normalError $ S.estError e
  in List.zipWith (\n e -> NamedEstimate n (S.estPoint e) (S.quantile (S.normalDistr 0 (sigma e)) ciPct)) pNames (parameterEstimates res)

namedEstimatesColonnade :: R -> C.Colonnade C.Headed (NamedEstimate R) T.Text
namedEstimatesColonnade  ci = C.headed "parameter" regressorName
                           <> C.headed "estimate" (T.pack . TP.printf "%.2f" . regressorEstimate)
                           <> C.headed ((T.pack $ TP.printf "%.0f" (100 * ci)) <> "% interval") (T.pack . TP.printf "%.2f" . regressorCI)

namedSummaryStats :: RegressionResult R -> [(T.Text, R)]
namedSummaryStats r =
  [
    ("R-Squared",(rSquared r))
  , ("Adj. R-squared",(adjRSquared r))
  , ("Mean Squared Error",(meanSquaredError r))
  ]

namedSummaryStatsColonnade :: C.Colonnade C.Headed (T.Text,Double) T.Text
namedSummaryStatsColonnade  = C.headed "Summary Stat." fst <> C.headed "Value" (T.pack . TP.printf "%.2f" . snd)

prettyPrintRegressionResult :: T.Text -> [T.Text] -> RegressionResult R -> R -> T.Text
prettyPrintRegressionResult yName xNames r ci =
  let header = "Explaining " <> yName <> ":\n"
      nEsts = namedEstimates xNames r ci
      nSS = namedSummaryStats r
  in header <>
     (T.pack $ C.ascii (fmap T.unpack $ namedEstimatesColonnade ci) nEsts)
     <> (T.pack $ C.ascii (fmap T.unpack namedSummaryStatsColonnade) nSS)


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
