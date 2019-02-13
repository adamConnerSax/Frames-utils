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

module Math.Regression.LeastSquares where

import qualified Math.HMatrixUtils          as HU
import qualified Math.Regression.Regression as RE

import qualified Control.Foldl              as FL
import qualified Control.Foldl.Statistics   as FS
import qualified Data.Foldable              as Foldable
import           Data.Function              (on)
import qualified Data.List                  as List
import qualified Data.Profunctor            as PF
import qualified Data.Text                  as T
import qualified Data.Vector.Storable       as V
import qualified Statistics.Types           as S
import qualified System.PipesLogger         as SL

import           Numeric.LinearAlgebra      (( #> ), (<#), (<.>), (<\>))
import qualified Numeric.LinearAlgebra      as LA
import           Numeric.LinearAlgebra.Data (Matrix, R, Vector)
import qualified Numeric.LinearAlgebra.Data as LA

-- matrix dimensions given in (row x column) form
-- if A is (m x n), its transpose, A' is (n x m)
-- ||A|| is the Frobenius norm of A, the sum of the squares of the elements.
-- if A (m x l) and B (m x k) have the same number of rows, [A B] is the (m x (l+k)) matrix formed by appending the columns of B to the columns of A
-- A: an (m x n) matrix, of m observations of n observables
-- B: an (m x d) matrix, of observations (often d=1 and B is just a length m column-vector)
-- Q: an ((n+d)m x (n+d)m) matrix of covariances, the covariance matrix of [A B].  Given?  Computed?

-- we want an (n x d) matrix X which solves, or "best solves", AX = B.

-- OLS: minimize ||AX - B||
ordinaryLS :: Monad m => Bool -> Matrix R -> Vector R -> SL.Logger m (RE.RegressionResult R)
ordinaryLS withConstant mA vB = do
  let mAwc = if withConstant then addBiasCol (LA.size vB) mA  else mA -- add a constant, e.g., the b in y = mx + b
  HU.checkVectorMatrix "b" "A" vB mAwc  -- vB <> mA is legal, b has same length as A has rows
  let vX = mAwc <\> vB
      vU = vB - (mA #> vX) -- residuals
      mse = (vU <.> vU) / (realToFrac $ LA.size vU)
      (rSq, aRSq) = RE.goodnessOfFit (snd $ LA.size mA) vB vU
      cov = LA.scale mse (LA.inv $ LA.tr mAwc LA.<> mAwc)
  return $ RE.RegressionResult (RE.estimates cov vX) mse rSq aRSq cov

weightedLS :: Monad m => Bool -> Matrix R -> Vector R -> Vector R -> SL.Logger m (RE.RegressionResult R)
weightedLS withConstant mA vB vW = do
  HU.checkEqualVectors "b" "w" vB vW
  let mW = LA.diag vW
      vWB = mW #> vB
      mAwc = if withConstant then addBiasCol (LA.size vB) mA else mA -- add a constant, e.g., the b in y = mx + b
  HU.checkVectorMatrix "b" "A" vB mAwc
  let mWA = mW LA.<> mAwc
      vX = mWA <\> vWB
      vU = vB - (mAwc #> vX)
      vWU = mW #> vU
      (rSq, aRSq) = RE.goodnessOfFit (snd $ LA.size mA) vWB vWU
      mse = (vWU <.> vWU) / LA.sumElements vW
      cov = LA.scale mse (LA.inv $ LA.tr mAwc LA.<> mAwc)
  return $ RE.RegressionResult (RE.estimates cov vX) mse rSq aRSq cov

totalLS :: Monad m =>  Bool -> Matrix R -> Vector R -> SL.Logger m (RE.RegressionResult R)
totalLS withConstant mA vB = do
  let mAwc = if withConstant then addBiasCol (LA.size vB) mA  else mA -- add a constant, e.g., the b in y = mx + b
      p = snd $ LA.size mAwc
  HU.checkVectorMatrix "b" "Awc" vB mAwc
  let mAB = (mAwc LA.||| LA.asColumn vB)
      (sv, mV') = LA.rightSV mAB
--      gSV = sv V.! 0
--      tol = gSV * LA.eps
      sV22 = mV' LA.! p LA.! p
      vV12 = List.head $ LA.toColumns $ LA.subMatrix (0,p) (p,1) mV' --LA.?? (LA.DropLast 1, LA.TakeLast 1)
      vX = LA.scale (-1/sV22) vV12 -- this is the TLS solution.  But in a shifted basis.??
      mV2 = mV' LA.?? (LA.All, LA.TakeLast 1)
      mABt = mAB LA.<> mV2 LA.<> (LA.tr mV2)
      mAt = mABt LA.?? (LA.All, LA.DropLast 1)
      vBfit = (mA - mAt) #> vX
      vU = vB - vBfit
      (rSq, aRSq) = RE.goodnessOfFit (snd $ LA.size mA) vB vU --(vB - mA #> vX)
      mse = (vU <.> vU) / (realToFrac $ LA.size vU)
      cov = LA.scale mse (LA.inv $ LA.tr mAwc LA.<> mAwc)
  return $ RE.RegressionResult (RE.estimates cov vX) mse rSq aRSq cov

weightedTLS :: Monad m =>  Bool -> Matrix R -> Vector R -> Vector R -> SL.Logger m (RE.RegressionResult R)
weightedTLS withConstant mA vB vW = do
  HU.checkEqualVectors "b" "w" vB vW
  let mW = LA.diag vW
      vWB = mW #> vB
      mAwc = if withConstant then addBiasCol (LA.size vB) mA  else mA -- add a constant, e.g., the b in y = mx + b
      p = snd $ LA.size mAwc
  HU.checkVectorMatrix "b" "Awc" vB mAwc
  let mWA = mW LA.<> mAwc
      mWAB = (mWA LA.||| LA.asColumn vWB)
      (sv, mV') = LA.rightSV mWAB
--      gSV = sv V.! 0
--      tol = gSV * LA.eps
      sV22 = mV' LA.! p LA.! p
      vV12 = List.head $ LA.toColumns $ LA.subMatrix (0,p) (p,1) mV' --LA.?? (LA.DropLast 1, LA.TakeLast 1)
      vX = LA.scale (-1/sV22) vV12 -- this is the WTLS solution.  But in a shifted basis.??
      mV2 = mV' LA.?? (LA.All, LA.TakeLast 1)
      mWABt = mWAB LA.<> mV2 LA.<> (LA.tr mV2)
      mWAt = mWABt LA.?? (LA.All, LA.DropLast 1)
      vBfit = (mWA - mWAt) #> vX
      vWU = vWB - vBfit
      (rSq, aRSq) = RE.goodnessOfFit (snd $ LA.size mA) vWB vWU --(vB - mA #> vX)
      mse = (vWU <.> vWU) / LA.sumElements vW
      cov = LA.scale mse (LA.inv $ LA.tr mAwc LA.<> mAwc)
  return $ RE.RegressionResult (RE.estimates cov vX) mse rSq aRSq cov

addBiasCol :: Int -> Matrix R -> Matrix R
addBiasCol rows mA =
  let colList = (List.replicate rows 1)
  in if LA.size mA == (0,0)
     then LA.matrix 1 colList
     else LA.col colList LA.||| mA
