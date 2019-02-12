{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import qualified Control.Foldl                as FL
import qualified Data.List                    as List
import           Data.Maybe                   (fromMaybe)
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
import qualified Math.Regression.Regression   as RE

import qualified System.PipesLogger           as SL

main = runOnly "all" suite

suite :: Test ()
suite = tests
  [
    scope "all.Regression" $ do
      testOLS
      testWOLS
      testTLS
      testWTLS
  ]

logged = SL.runLoggerIO SL.logAll
-- regression tests

-- build some data for testing
-- uniformly distributed measurements, normally distributed noise.  Separate noise amplitudes for xs and ys
-- also allow building heteroscedastic data
buildRegressable :: [Double] -> Maybe (LA.Vector R) -> Double -> LA.Vector R -> Double -> IO (LA.Vector R, LA.Matrix R)
buildRegressable variances offsetsM noiseObs coeffs noiseMeas = do
  -- generate random measurements
  let d = LA.size coeffs
      nObs = List.length variances
      xsO = LA.asColumn (LA.fromList (List.replicate nObs 1)) LA.<> LA.asRow (fromMaybe (LA.fromList $ List.replicate d 0) offsetsM)
  xs0 <- LA.rand nObs d
  xNoise <- LA.randn nObs d
  let xs = xs0 + xsO + LA.scale noiseMeas xNoise
  let ys0 = (xs0 + xsO) LA.<> LA.asColumn coeffs
  yNoise <- fmap (List.head . LA.toColumns) (LA.randn nObs 1)
  let ys = ys0 + LA.asColumn (LA.scale noiseObs (V.zipWith (*) yNoise (LA.cmap sqrt (LA.fromList variances))))
  return (List.head (LA.toColumns ys), xs)

unweighted :: Int -> [Double]
unweighted n = List.replicate n (1.0)

coneWeighted :: Int -> Double -> [Double]
coneWeighted n increment =
  let w0 = [1 + (realToFrac i) * increment | i <- [0..n]]
      s = realToFrac n/FL.fold FL.sum w0
  in fmap (*s) w0

showText :: Show a => a -> T.Text
showText = T.pack . show

errR2 res = let r2 = RE.rSquared res in (r2 <= 1 && r2 > 0.0)
coeffs :: LA.Vector R = LA.fromList [1.0, 2.2, 0.3]
offsets :: LA.Vector R = LA.fromList [0,1,0]
vars = coneWeighted 100 0.1
varListToWeights = LA.cmap (\x -> 1/sqrt x) . LA.fromList
wgts = varListToWeights vars

testRegression :: Double -> Double -> Bool -> Bool -> (Matrix R -> Vector R -> Vector R -> SL.Logger Test (RE.RegressionResult R)) -> Test ()
testRegression yNoise xNoise weighted offset f = do
  let scopeT = "Vy=" <> showText yNoise <> "; Vx=" <> showText xNoise <> (if weighted then "; cone weights" else "") <> (if offset then "; w/offsets" else "")
      vars = if weighted then coneWeighted 100 0.1 else unweighted 100
      wgts = varListToWeights vars
      offsetM = if offset then (Just offsets) else Nothing
  logged $ do
    (ys, xs) <- SL.liftLog $ io $ buildRegressable vars offsetM yNoise coeffs xNoise
    result <- f xs ys wgts
    SL.liftLog $ note $ scopeT <> "\n" <> RE.prettyPrintRegressionResult "y" ["x1","x2","x3"] result 0.95
    SL.liftLog $ ok -- expect $ errR2 result

const3 f x y z = f x y

testOLS :: Test ()
testOLS =  scope "OLS" $ do
  let regress xs ys ws = LS.ordinaryLS False xs ys
  note "Ordinary Least Squares"
  testRegression 0 0 False False regress
  testRegression 0.1 0 False False regress
  testRegression 0.5 0 False False regress
  testRegression 0.1 0 False True regress
  testRegression 0.3 0.3 False False regress
  testRegression 0.3 0 True False regress
  testRegression 0.3 0.3 True False regress

testWOLS :: Test ()
testWOLS =  scope "WOLS" $ do
  let regress = LS.weightedLS False
  note "Weighted Ordinary Least Squares"
  testRegression 0 0 True False regress
  testRegression 0.1 0 True False regress
  testRegression 0.1 0 False False regress
  testRegression 0.3 0 True False regress
  testRegression 0.1 0.1 True False regress
  testRegression 0.1 0.1 True True regress
  testRegression 0.3 0.3 True False regress

testTLS :: Test ()
testTLS = scope "TLS" $ do
  let regress xs ys ws = LS.totalLS False xs ys
  note "Total Least Squares"
  testRegression 0 0 False False regress
  testRegression 0.1 0 False False regress
  testRegression 0.5 0 False False regress
  testRegression 0.1 0 False True regress
  testRegression 0.3 0.3 False False regress
  testRegression 0.3 0 True False regress
  testRegression 0.3 0.3 True False regress

testWTLS :: Test ()
testWTLS =  scope "WTLS" $ do
  let regress = LS.weightedTLS False
  note "Weighted Total Least Squares"
  testRegression 0 0 True False regress
  testRegression 0.1 0 True False regress
  testRegression 0.1 0 False False regress
  testRegression 0.3 0 True False regress
  testRegression 0.1 0.1 True False regress
  testRegression 0.1 0.1 True True regress
  testRegression 0.3 0.3 True False regress
