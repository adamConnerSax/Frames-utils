{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
module Main where

import qualified Control.Foldl                as FL
--import           Control.Monad.Trans          (lift)
import           Control.Monad.Morph          (generalize, hoist, lift)
import qualified Data.List                    as List
import           Data.Maybe                   (fromMaybe)
import qualified Data.Text                    as T
import qualified Data.Text.IO                 as T
import qualified Data.Text.Lazy               as TL
import qualified Data.Vector.Storable         as V
import qualified Data.Vinyl                   as V
import qualified Frames                       as F

import           Numeric.LinearAlgebra        (( #> ), (<#), (<.>), (<\>))
import qualified Numeric.LinearAlgebra        as LA
import           Numeric.LinearAlgebra.Data   (Matrix, R, Vector)
import qualified Numeric.LinearAlgebra.Data   as LA

import qualified Frames.Regression            as FR
import qualified Frames.VegaLiteTemplates     as FV
import qualified Html.Report                  as H
import qualified Lucid                        as H
import qualified Math.Regression.LeastSquares as LS
import qualified Math.Regression.Regression   as RE
import qualified System.PipesLogger           as SL



main :: IO ()
main = do
  htmlAsText <- (H.makeReportHtmlAsText "Frame Regression Examples") $ do
    logged $ do
      testOLS
      testWOLS
      testTLS
      testWTLS
  T.writeFile "examples/html/FrameRegressions.html" $ TL.toStrict $ htmlAsText

logged = SL.runLoggerIO SL.logAll
-- regression tests

-- build some data for testing
-- uniformly distributed measurements, normally distributed noise.  Separate noise amplitudes for xs and ys
-- also allow building heteroscedastic data

type Y = "y" F.:-> Double
type X1 = "x1" F.:-> Double
type X2 = "x2" F.:-> Double
type Weight = "weight" F.:-> Double

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


makeFrame :: [Double] -> (LA.Vector R, LA.Matrix R) -> IO (F.FrameRec [Y,X1,X2,Weight])
makeFrame vars (ys, xs) = do
  if snd (LA.size xs) /= 2 then error "Matrix of xs wrong size for makeFrame" else return ()
  let rows = LA.size ys
      rowIndex = [0..(rows - 1)]
      makeRecord :: Int -> F.Record [Y,X1,X2,Weight]
      makeRecord n = ys `LA.atIndex` n F.&: xs `LA.atIndex` (n,0) F.&: xs `LA.atIndex` (n,1) F.&: vars List.!! n F.&: V.RNil
  return $ F.toFrame $ makeRecord <$> rowIndex


unweighted :: Int -> [Double]
unweighted n = List.replicate n (1.0)

coneWeighted :: Int -> Double -> [Double]
coneWeighted n increment =
  let w0 = [1 + (realToFrac i) * increment | i <- [0..n]]
      s = realToFrac n/FL.fold FL.sum w0
  in fmap (*s) w0

showText :: Show a => a -> T.Text
showText = T.pack . show

coeffs :: LA.Vector R = LA.fromList [1.0, 2.2]
offsets :: LA.Vector R = LA.fromList [0,1]
vars = coneWeighted 100 0.1
varListToWeights = LA.cmap (\x -> 1/sqrt x) . LA.fromList
wgts = varListToWeights vars

htmlLift :: SL.Logger IO a -> SL.Logger (H.HtmlT IO) a
htmlLift = hoist (hoist lift)

type AllCols = [Y,X1,X2,Weight]

testRegression :: forall w.
                  Double
               -> Double
               -> Bool
               -> Bool
               -> (F.FrameRec AllCols -> SL.Logger IO (FR.FrameRegressionResult Y False [X1,X2] w AllCols))
               -> SL.Logger (H.HtmlT IO) (T.Text, FR.FrameRegressionResult Y False [X1,X2] w AllCols, F.FrameRec AllCols)
testRegression yNoise xNoise weighted offset f = do
  let scopeT = "Vy=" <> showText yNoise <> "; Vx=" <> showText xNoise <> (if weighted then "; cone weights" else "") <> (if offset then "; w/offsets" else "")
      vars = if weighted then coneWeighted 100 0.1 else unweighted 100
      wgts = varListToWeights vars
      offsetM = if offset then (Just offsets) else Nothing
  frame <- htmlLift $ SL.liftLog $ (buildRegressable vars offsetM yNoise coeffs xNoise >>= makeFrame vars)
  result <- htmlLift $ f frame
  return (scopeT, result, frame)

reportRegressionUW :: T.Text -> (T.Text, FR.FrameRegressionResult Y False [X1,X2] FR.Unweighted AllCols, F.FrameRec AllCols)
                   -> SL.Logger (H.HtmlT IO) ()
reportRegressionUW vizId (scope, result, frame) = do
  let header _ _ = scope
  SL.liftLog $ hoist generalize $ FR.prettyPrintRegressionResultHtml header result 0.95
  SL.liftLog $ hoist generalize $ H.placeVisualization vizId $ FV.scatterPlot scope (Just vizId) result 0.95 frame
  htmlLift $ SL.log SL.Info $ FR.prettyPrintRegressionResult header result 0.95
  return ()

reportRegressionW :: T.Text -> (T.Text, FR.FrameRegressionResult Y False [X1,X2] Weight AllCols, F.FrameRec AllCols)
                  -> SL.Logger (H.HtmlT IO) ()
reportRegressionW vizId (scope, result, frame) = do
  let header _ _ = scope
  SL.liftLog $ hoist generalize $ FR.prettyPrintRegressionResultHtml header result 0.95
  SL.liftLog $ hoist generalize $ H.placeVisualization vizId $ FV.scatterPlot scope (Just vizId) result 0.95 frame
  htmlLift $ SL.log SL.Info $ FR.prettyPrintRegressionResult header result 0.95
  return ()


const3 f x y z = f x y

testOLS :: SL.Logger (H.HtmlT IO) ()
testOLS =  SL.wrapPrefix "OLS" $ do
  let regress = FR.ordinaryLeastSquares
  htmlLift $ SL.log SL.Info "Ordinary Least Squares"
  testRegression 0 0 False False regress >>= reportRegressionUW "OLS1"
  testRegression 0.1 0 False False regress >>= reportRegressionUW "OLS2"
  testRegression 0.5 0 False False regress >>= reportRegressionUW "OLS3"
  testRegression 0.1 0 False True regress >>= reportRegressionUW "OLS4"
  testRegression 0.3 0.3 False False regress >>= reportRegressionUW "OLS5"
  testRegression 0.3 0 True False regress >>= reportRegressionUW "OLS6"
  testRegression 0.3 0.3 True False regress >>= reportRegressionUW "OLS7"

testWOLS :: SL.Logger (H.HtmlT IO) ()
testWOLS =  SL.wrapPrefix "WOLS" $ do
  let regress = FR.weightedLeastSquares
  htmlLift $ SL.log SL.Info "Weighted Ordinary Least Squares"
  testRegression 0 0 True False regress >>= reportRegressionW "WOLS1"
  testRegression 0.1 0 True False regress >>= reportRegressionW "WOLS2"
  testRegression 0.1 0 False False regress >>= reportRegressionW "WOLS3"
  testRegression 0.3 0 True False regress >>= reportRegressionW "WOLS4"
  testRegression 0.1 0.1 True False regress >>= reportRegressionW "WOLS5"
  testRegression 0.1 0.1 True True regress >>= reportRegressionW "WOLS6"
  testRegression 0.3 0.3 True False regress >>= reportRegressionW "WOLS7"

testTLS :: SL.Logger (H.HtmlT IO) ()
testTLS = SL.wrapPrefix "TLS" $ do
  let regress = FR.totalLeastSquares
  htmlLift $ SL.log SL.Info "Total Least Squares"
  testRegression 0 0 False False regress >>= reportRegressionUW "TLS1"
  testRegression 0.1 0 False False regress >>= reportRegressionUW "TLS2"
  testRegression 0.5 0 False False regress >>= reportRegressionUW "TLS3"
  testRegression 0.1 0 False True regress >>= reportRegressionUW "TLS4"
  testRegression 0.3 0.3 False False regress >>= reportRegressionUW "TLS5"
  testRegression 0.3 0 True False regress >>= reportRegressionUW "TLS6"
  testRegression 0.3 0.3 True False regress >>= reportRegressionUW "TLS7"

testWTLS :: SL.Logger (H.HtmlT IO) ()
testWTLS =  SL.wrapPrefix "WTLS" $ do
  let regress = FR.weightedTLS
  htmlLift $ SL.log SL.Info "Weighted Total Least Squares"
  testRegression 0 0 True False regress >>= reportRegressionW "WTLS1"
  testRegression 0.1 0 True False regress >>= reportRegressionW "WTLS2"
  testRegression 0.1 0 False False regress >>= reportRegressionW "WTLS3"
  testRegression 0.3 0 True False regress >>= reportRegressionW "WTLS4"
  testRegression 0.1 0.1 True False regress >>= reportRegressionW "WTLS5"
  testRegression 0.1 0.1 True True regress >>= reportRegressionW "WTLS6"
  testRegression 0.3 0.3 True False regress >>= reportRegressionW "WTLS7"

