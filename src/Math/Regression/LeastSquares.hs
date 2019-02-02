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


import qualified Control.Foldl              as FL
import qualified Control.Foldl.Statistics   as FS
import qualified Data.Foldable              as Foldable
import           Data.Function              (on)
import qualified Data.List                  as List
import qualified Data.Profunctor            as PF
import qualified Data.Text                  as T
import qualified Data.Vector.Storable       as V
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


-- Plan
-- 1. Write OLS in matrix form
-- 2. Write simple TLS (via SVD)
-- 3. Write a weighted TLL


data RegressionResult a = RegressionResult
                          {
                            parameters       :: Vector a
                          , meanSquaredError :: a
                          , rSquared         :: Double
                          , adjRSquared      :: Double
                          , covariances      :: Matrix a
--                          , residuals :: Vector a
                          } deriving (Show)

-- NB: Some of these may be inefficient. We know A'A is symmetric definite (could have 0 eigenvalue, right?) so there should be faster linear solvers
-- than inversion

goodnessOfFit :: Int -> Vector R -> Vector R -> (R, R)
goodnessOfFit p vB vU =
  let n = LA.size vB
      meanB = LA.sumElements vB / (realToFrac $ LA.size vB)
      ssTot = let x = LA.cmap (\y -> y - meanB) vB in x <.> x
      ssRes = vU <.> vU
      rSq = 1 - (ssRes/ssTot)
      arSq = 1 - (1 - rSq)*(realToFrac $ (n - 1))/(realToFrac $ (n - p - 1))

  in (rSq, arSq)

ordinaryLS :: Monad m => Bool -> Matrix R -> Vector R -> SL.Logger m (RegressionResult R)
ordinaryLS withConstant mA vB = do
  let mAwc = if withConstant then addBiasCol (LA.size vB) mA  else mA -- add a constant, e.g., the b in y = mx + b
  checkVectorMatrix "b" "A" vB mAwc  -- vB <> mA is legal, b has same length as A has rows
  let vX = mAwc <\> vB
      vU = vB - (mA #> vX) -- residuals
      mse = (vU <.> vU) / (realToFrac $ LA.size vU)
      (rSq, aRSq) = goodnessOfFit (snd $ LA.size mA) vB vU
      cov = LA.scale mse (LA.inv $ LA.tr mAwc LA.<> mAwc)
  return $ RegressionResult vX mse rSq aRSq cov

weightedLS :: Monad m => Bool -> Matrix R -> Vector R -> Vector R -> SL.Logger m (RegressionResult R)
weightedLS withConstant mA vB vW = do
  checkEqualVectors "b" "w" vB vW
  let mW = LA.diag vW
      vWB = mW #> vB
      mAwc = if withConstant then addBiasCol (LA.size vB) mA else mA -- add a constant, e.g., the b in y = mx + b
  checkVectorMatrix "b" "A" vB mAwc
  let mWA = mW LA.<> mAwc
      vX = mWA <\> vWB
      vU = vB - (mAwc #> vX)
      vWU = mW #> vU
      (rSq, aRSq) = goodnessOfFit (snd $ LA.size mA) vWB vWU
      mse = (vWU <.> vWU) / LA.sumElements vW
      cov = LA.scale mse (LA.inv $ LA.tr mAwc LA.<> mAwc)
  return $ RegressionResult vX mse rSq aRSq cov

totalLeastSquares :: Monad m =>  Bool -> Matrix R -> Vector R -> SL.Logger m (RegressionResult R)
totalLeastSquares withConstant mA vB = do
  let mAwc = if withConstant then addBiasCol (LA.size vB) mA  else mA -- add a constant, e.g., the b in y = mx + b
      p = snd $ LA.size mAwc
  checkVectorMatrix "b" "Awc" vB mAwc
  let mAB = (mAwc LA.||| LA.asColumn vB)
      (sv, mV) = LA.rightSV mAB
--      gSV = sv V.! 0
--      tol = gSV * LA.eps
      sV22 = mV LA.! p LA.! p
      vV12 = List.head $ LA.toColumns $ mV LA.?? (LA.DropLast 1, LA.TakeLast 1)
      vX = LA.scale (-1/sV22) vV12 -- this is the TLS solution.  But in a shifted basis.??
      mV2 = mV LA.?? (LA.All, LA.TakeLast 1)
      mABt = mAB LA.<> mV2 LA.<> (LA.tr mV2)
      mAt = mABt LA.?? (LA.All, LA.DropLast 1)
      vBfit = (mA + mAt) #> vX
      vU = vB - vBfit
      (rSq, aRSq) = goodnessOfFit (snd $ LA.size mA) vB vU
      mse = (vU <.> vU) / (realToFrac $ LA.size vU)
      cov = LA.scale mse (LA.inv $ LA.tr mAwc LA.<> mAwc)
  return $ RegressionResult vX mse rSq aRSq cov


eickerHeteroscedasticityEstimator :: Monad m => Matrix R -> Vector R -> Vector R -> SL.Logger m (Matrix R)
eickerHeteroscedasticityEstimator mA vB vB' = do
  checkVectorMatrix "b" "A" vB mA
  checkVectorMatrix "b'" "A" vB' mA
  let u = vB - vB'
      diagU = LA.diag $ V.zipWith (*) u u
      mA' = LA.tr mA
      mC = LA.inv (mA' LA.<> mA)
  return $ mC LA.<> (mA' LA.<> diagU LA.<> mA) LA.<> mC


addBiasCol :: Int -> Matrix R -> Matrix R
addBiasCol rows mA =
  let colList = (List.replicate rows 1)
  in if LA.size mA == (0,0)
     then LA.matrix 1 colList
     else LA.col colList LA.||| mA

textSize :: (LA.Container c e, Show (LA.IndexOf c)) => c e -> T.Text
textSize = T.pack . show . LA.size

checkEqualVectors :: Monad m => T.Text -> T.Text -> Vector R -> Vector R -> SL.Logger m ()
checkEqualVectors nA nB vA vB =
  if (LA.size vA == LA.size vB)
  then return ()
  else SL.log SL.Error $ "Unequal vector length. length(" <> nA <> ")=" <> textSize vA <> " and length(" <> nB <> ")=" <> textSize vB

checkMatrixVector :: Monad m => T.Text -> T.Text -> Matrix R -> Vector R -> SL.Logger m ()
checkMatrixVector nA nB mA vB =
  if (snd (LA.size mA) == LA.size vB)
  then return ()
  else SL.log SL.Error $ "Bad matrix * vector lengths. dim(" <> nA <> ")=" <> textSize mA <> " and length(" <> nB <> ")=" <> textSize vB

checkVectorMatrix :: Monad m => T.Text -> T.Text -> Vector R -> Matrix R -> SL.Logger m ()
checkVectorMatrix nA nB vA mB =
  if (LA.size vA == fst (LA.size mB))
  then return ()
  else SL.log SL.Error $ "Bad vector * matrix lengths. length(" <> nA <> ")=" <> textSize vA <> " and dim(" <> nB <> ")=" <> textSize mB
