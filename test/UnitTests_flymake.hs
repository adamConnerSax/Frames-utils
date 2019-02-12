{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import qualified Data.List                    as List
import qualified Data.Text                    as T
import qualified Data.Vector.Storable         as V
import qualified Data.Vinyl                   as V
import qualified Frames                       as F

import           Numeric.LinearAlgebra        (( #> ), (<#), (<.>), (<\>))
import qualified Numeric.LinearAlgebra        as LA
import           Numeric.LinearAlgebra.Data   (Matrix, R, Vector)
import qualified Numeric.LinearAlgebra.Data   as LA

import           Control.Applicative
import           Control.Monad
import           EasyTest

import qualified Math.Regression.LeastSquares as LS
import qualified System.PipesLogger           as SL

main = runOnly "all" suite

suite :: Test ()
suite = tests
  [
    scope "all.Regression" $ do
      testOLS
      testTLS
  ]

logged = SL.runLoggerIO SL.logAll
-- regression tests

-- build some data for testing
-- uniformly distributed measurements, normally distributed noise.  Separate noise amplitudes for xs and ys
-- also allow building heteroscedastic data
buildRegressable :: [Double] -> Double -> LA.Vector R -> Double -> IO (LA.Vector R, LA.Matrix R)
buildRegressable variances noiseObs coeffs noiseMeas = do
  -- generate random measurements
  let d = LA.size coeffs
      nObs = List.length variances
  xs0 <- LA.rand nObs d
  xNoise <- LA.randn nObs d
  let xs = xs0 + LA.scale noiseMeas xNoise
  let ys0 = xs LA.<> LA.asColumn coeffs
  yNoise <- fmap (List.head . LA.toColumns) (LA.randn nObs 1)
  let ys = ys0 + LA.asColumn (LA.scale noiseObs (V.zipWith (*) yNoise (LA.cmap sqrt (LA.fromList variances))))
  return (List.head (LA.toColumns ys), xs)

unweighted :: Int -> [Double] = flip List.replicate 1

showText = T.pack . show

testOLS :: Test ()
testOLS =  scope "OLS" $ do
  let errR2 res = let r2 = LS.rSquared res in (r2 <= 1 && r2 > 0.8)
      coeffs :: LA.Vector R = LA.fromList [1.0, 2.2, 0.3]
  scope "No noise" $ logged $ do
    (ys, xs) <- SL.liftLog $  io $ buildRegressable (unweighted 100) 0 coeffs 0
    result <- LS.ordinaryLS False xs ys
    SL.log SL.Info $ "OLS (0 noise)\n" <> T.pack (show result)
    SL.liftLog $ expect $ errR2 result
  scope "(0.01,0) (obs,meas) Noise" $ logged $ do
    (ys, xs) <- SL.liftLog $  io $ buildRegressable (unweighted 100) 0.01 coeffs 0
    result <- LS.ordinaryLS False xs ys
    SL.log SL.Info $ "OLS (1% noise in ys)\n" <> T.pack (show result)
    SL.liftLog $ expect $ errR2 result
  scope "(0.1,0) (obs,meas) Noise" $ logged $ do
    (ys, xs) <- SL.liftLog $  io $ buildRegressable (unweighted 100) 0.1 coeffs 0
    result <- LS.ordinaryLS False xs ys
    SL.log SL.Info $ "OLS (10% noise in ys)\n" <> T.pack (show result)
    SL.liftLog $ expect $ errR2 result
  scope "(0.01,0.01) (obs,meas) Noise" $ logged $ do
    (ys, xs) <- SL.liftLog $  io $ buildRegressable (unweighted 100) 0.01 coeffs 0.01
    result <- LS.ordinaryLS False xs ys
    SL.log SL.Info $ "OLS (1% noise in ys, 1% noise in xs)\n" <> (T.pack $ show result)
    SL.liftLog $ expect $ errR2 result
  scope "(0.1,0.1) (obs,meas) Noise" $ logged $ do
    (ys, xs) <- SL.liftLog $  io $ buildRegressable (unweighted 100) 0.1 coeffs 0.1
    result <- LS.ordinaryLS False xs ys
    SL.log SL.Info $ "OLS (10% noise in ys, 10% noise in xs)\n" <> (T.pack $ show result)
    SL.liftLog $ expect $ errR2 result
  scope "(0.3,0.3) (obs,meas) Noise" $ logged $ do
    (ys, xs) <- SL.liftLog $  io $ buildRegressable (unweighted 100) 0.3 coeffs 0.3
    result <- LS.ordinaryLS False xs ys
    SL.log SL.Info $ "OLS (30% noise in ys, 30% noise in xs)\n" <> (T.pack $ show result)
    SL.liftLog $ expect $ errR2 result

testTLS :: Test ()
testTLS = scope "TLS" $ do
  let errR2 res = let r2 = LS.rSquared res in (r2 <= 1 && r2 > 0.8)
      coeffs :: LA.Vector R = LA.fromList [1.0, 2.2, 0.3]
  scope "No noise" $ logged $ do
    (ys, xs) <- SL.liftLog $  io $ buildRegressable (unweighted 100) 0 coeffs 0
    result <- LS.totalLeastSquares False xs ys
    SL.log SL.Info $ "TLS (0 noise)\n" <> (T.pack $ show result)
    SL.liftLog $ expect $ errR2 result
  scope "0.01 Noise" $ logged $ do
    (ys, xs) <- SL.liftLog $  io $ buildRegressable (unweighted 100) 0.01 coeffs 0
    result <- LS.totalLeastSquares False xs ys
    SL.log SL.Info $ "TLS (1% noise in ys)\n" <> (T.pack $ show result)
    SL.liftLog $ expect $ errR2 result
  scope "0.1 Noise" $ logged $ do
    (ys, xs) <- SL.liftLog $  io $ buildRegressable (unweighted 100) 0.1 coeffs 0
    result <- LS.totalLeastSquares False xs ys
    SL.log SL.Info $ "TLS (10% noise in ys)\n" <> (T.pack $ show result)
    SL.liftLog $ expect $ errR2 result
  scope "(0.01,0.01) (obs,meas) Noise" $ logged $ do
    (ys, xs) <- SL.liftLog $  io $ buildRegressable (unweighted 100) 0.01 coeffs 0.01
    result <- LS.totalLeastSquares False xs ys
    SL.log SL.Info $ "TLS (1% noise in ys, 1% noise in xs)\n" <> (T.pack $ show result)
    SL.liftLog $ expect $ errR2 result
  scope "(0.1,0.1) (obs,meas) Noise" $ logged $ do
    (ys, xs) <- SL.liftLog $  io $ buildRegressable (unweighted 100) 0.1 coeffs 0.1
    result <- LS.totalLeastSquares False xs ys
    SL.log SL.Info $ "TLS (10% noise in ys, 10% noise in xs)\n" <> (T.pack $ show result)
    SL.liftLog $ expect $ errR2 result
  scope "(0.3,0.3) (obs,meas) Noise" $ logged $ do
    (ys, xs) <- SL.liftLog $  io $ buildRegressable (unweighted 100) 0.3 coeffs 0.3
    result <- LS.totalLeastSquares False xs ys
    SL.log SL.Info $ "TLS (30% noise in ys, 30% noise in xs)\n" <> (T.pack $ show result)
    SL.liftLog $ expect $ errR2 result

