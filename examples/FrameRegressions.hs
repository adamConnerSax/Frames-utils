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
import           System.IO                    (BufferMode (..), hSetBuffering,
                                               stdout)
import qualified System.PipesLogger           as SL



main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
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
type X = "x" F.:-> Double
--type X2 = "x2" F.:-> Double
type Weight = "weight" F.:-> Double

buildRegressable :: [Double] -> Maybe (LA.Vector R) -> Double -> LA.Vector R -> Double -> IO (LA.Vector R, LA.Matrix R)
buildRegressable variances offsetsM noiseObs coeffs noiseMeas = do
  -- generate random measurements
  putStrLn $ show variances
  let d = LA.size coeffs
      nObs = List.length variances
      xsO = LA.asColumn (LA.fromList (List.replicate nObs 1)) LA.<> LA.asRow (fromMaybe (LA.fromList $ List.replicate (d-1) 0) offsetsM)
--  xs0 <- LA.cmap (\x-> x - 0.5) <$> LA.rand nObs (d-1) -- 0 centered uniformly distributed random numbers
  let xs0 :: Matrix R = LA.asColumn $ LA.fromList $ [-0.5 + (realToFrac i/realToFrac nObs) | i <- [0..(nObs-1)]]
  xNoise <- LA.randn nObs (d-1) -- 0 centered normally (sigma=1) distributed random numbers
  let xsC = 1 LA.||| (xs0 + xsO)
      xsN = 1 LA.||| (xs0 + xsO + LA.scale noiseMeas xNoise)
  let ys0 = xsC LA.<> LA.asColumn coeffs
  yNoise <- fmap (List.head . LA.toColumns) (LA.randn nObs 1) -- 0 centered normally (sigma=1) distributed random numbers
  let ys = ys0 + LA.asColumn (LA.scale noiseObs (V.zipWith (*) yNoise (LA.cmap sqrt (LA.fromList variances))))
  return (List.head (LA.toColumns ys), xsN)

type AllCols = [Y,X,Weight]

makeFrame :: [Double] -> (LA.Vector R, LA.Matrix R) -> IO (F.FrameRec AllCols)
makeFrame vars (ys, xs) = do
  if snd (LA.size xs) /= 2 then error ("Matrix of xs wrong size (" ++ show (LA.size xs) ++ ") for makeFrame") else return ()
  let rows = LA.size ys
      rowIndex = [0..(rows - 1)]
      makeRecord :: Int -> F.Record AllCols
      makeRecord n = ys `LA.atIndex` n F.&: xs `LA.atIndex` (n,1) F.&: vars List.!! n F.&: V.RNil -- skip bias column
  return $ F.toFrame $ makeRecord <$> rowIndex


unweighted :: Int -> [Double]
unweighted n = List.replicate n (1.0)

coneWeighted :: Int -> Double -> [Double]
coneWeighted n increment =
  let w0 = [(realToFrac i) * increment | i <- [1..n]]
      s = realToFrac n/FL.fold FL.sum w0
  in fmap (*s) w0

showText :: Show a => a -> T.Text
showText = T.pack . show

coeffs :: LA.Vector R = LA.fromList [1.0, 2.2]
offsets :: LA.Vector R = LA.fromList [1]
vars = coneWeighted 100 0.1
varListToWeights = LA.cmap (\x -> 1/sqrt x) . LA.fromList
wgts = varListToWeights vars

htmlLift :: SL.Logger IO a -> SL.Logger (H.HtmlT IO) a
htmlLift = hoist (hoist lift)



testRegression :: (F.ColumnHeaders '[w], V.KnownField w)
               => Double
               -> Double
               -> Bool
               -> Bool
               -> T.Text
               -> (F.FrameRec AllCols -> SL.Logger IO (FR.FrameRegressionResult Y True '[X] w AllCols))
               -> SL.Logger (H.HtmlT IO) ()
--               -> SL.Logger (H.HtmlT IO) (T.Text, FR.FrameRegressionResult Y True '[X1] w AllCols, F.FrameRec AllCols)
testRegression yNoise xNoise weighted offset vizId f = do
  let scopeT = "Sy=" <> showText yNoise <> " gaussian noise added to ys & "
               <> "Sx=" <> showText xNoise <> " gaussian noise added to xs"
               <> " (" <> (if weighted then "cone weights" else "unweighted") <> (if offset then ", w/offsets" else "") <> ")"
      vars = if weighted then coneWeighted 100 0.1 else unweighted 100
      wgts = varListToWeights vars
      offsetM = if offset then (Just offsets) else Nothing
  frame <- htmlLift $ SL.liftLog $ (buildRegressable vars offsetM yNoise coeffs xNoise >>= makeFrame vars)
  result <- htmlLift $ f frame
  let header _ _ = scopeT
  SL.liftLog $ hoist generalize $ FR.prettyPrintRegressionResultHtml header result 0.95
  SL.liftLog $ hoist generalize $ H.placeVisualization vizId $ FV.scatterPlot scopeT (Just vizId) result 0.95 frame
  htmlLift $ SL.log SL.Info $ FR.prettyPrintRegressionResult header result 0.95
  return ()

const3 f x y z = f x y

testOLS :: SL.Logger (H.HtmlT IO) ()
testOLS =  SL.wrapPrefix "OLS" $ do
  let regress = FR.ordinaryLeastSquares
  htmlLift $ SL.log SL.Info "Ordinary Least Squares"
  testRegression 0 0 False False "OLS1" regress
  testRegression 0.1 0 False False "OLS2" regress
  testRegression 0.5 0 False False "OLS3" regress
--  testRegression 0.1 0 False True "OLS4" regress
  testRegression 0.3 0.3 False False "OLS5" regress
  testRegression 0.3 0 True False "OLS6" regress
  testRegression 0.3 0.3 True False "OLS7" regress

testWOLS :: SL.Logger (H.HtmlT IO) ()
testWOLS =  SL.wrapPrefix "WOLS" $ do
  let regress = FR.weightedLeastSquares
  htmlLift $ SL.log SL.Info "Weighted Ordinary Least Squares"
  testRegression @Weight 0 0 True False "WOLS1" regress
  testRegression @Weight 0.1 0 True False "WOLS2" regress
  testRegression @Weight 0.1 0 False False "WOLS3" regress
  testRegression @Weight 0.3 0 True False "WOLS4" regress
  testRegression @Weight 0.1 0.1 True False "WOL5" regress
--  testRegression @Weight 0.1 0.1 True True "WOLS6" regress
  testRegression @Weight 0.3 0.3 True False "WOLS7" regress

testTLS :: SL.Logger (H.HtmlT IO) ()
testTLS = SL.wrapPrefix "TLS" $ do
  let regress = FR.totalLeastSquares
  htmlLift $ SL.log SL.Info "Total Least Squares"
  testRegression 0 0 False False "TLS1" regress
  testRegression 0.1 0 False False "TLS2" regress
  testRegression 0.5 0 False False "TLS3" regress
--  testRegression 0.1 0 False True "TLS4" regress
  testRegression 0.3 0.3 False False "TLS5" regress
  testRegression 0.3 0 True False "TLS6" regress
  testRegression 0.3 0.3 True False "TLS7" regress

testWTLS :: SL.Logger (H.HtmlT IO) ()
testWTLS =  SL.wrapPrefix "WTLS" $ do
  let regress = FR.weightedTLS
  htmlLift $ SL.log SL.Info "Weighted Total Least Squares"
  testRegression @Weight 0 0 True False "WTLS1" regress
  testRegression @Weight 0.1 0 True False "WTLS2" regress
  testRegression @Weight 0.1 0 False False "WTLS3" regress
  testRegression @Weight 0.3 0 True False "WTLS4" regress
  testRegression @Weight 0.1 0.1 True False "WTLS5" regress
--  testRegression @Weight 0.1 0.1 True True "WTLS6" regress
  testRegression @Weight 0.3 0.3 True False "WTLS7" regress

