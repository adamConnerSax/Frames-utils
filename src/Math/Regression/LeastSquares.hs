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


data RegressionResult a = RegressionResult { parameters :: Vector a, residuals :: Vector a } deriving (Show)

-- NB: Some of these may be inefficient. We know A'A is symmetric definite (could have 0 eigenvalue, right?) so there should be faster linear solvers
-- than inversion

ordinaryLS :: Monad m => Bool -> Matrix R -> Vector R -> SL.Logger m (RegressionResult R)
ordinaryLS withConstant mA vB = do
  -- check dimensions
  checkVectorMatrix "b" "A" vB mA  -- vB <> mA is legal, b has same length as A has rows
  let mAwc = if withConstant then (LA.col $ List.replicate (LA.size vB) 1.0) LA.||| mA else mA -- add a constant, e.g., the b in y = mx + b
      vX = mAwc <\> vB
      vU = vB - (mA #> vX) -- residuals
  return $ RegressionResult vX vU

weightedLS :: Monad m => Bool -> Matrix R -> Vector R -> Vector R -> SL.Logger m (RegressionResult R)
weightedLS withConstant mA vB vW = do
  checkEqualVectors "b" "w" vB vW
  checkVectorMatrix "b" "A" vB mA
  let mW = LA.diag vW
      mAwc = if withConstant then (LA.col $ List.replicate (LA.size vB) 1.0) LA.||| mA else mA -- add a constant, e.g., the b in y = mx + b
      mWA = mW LA.<> mAwc
      vX = mWA <\> (mW #> vB)
      vU = vB - (mA #> vX)
  return $ RegressionResult vX vU

eickerHeteroscedasticityEstimator :: Monad m => Matrix R -> Vector R -> Vector R -> SL.Logger m (Matrix R)
eickerHeteroscedasticityEstimator mA vB vB' = do
  checkVectorMatrix "b" "A" vB mA
  checkVectorMatrix "b'" "A" vB' mA
  let u = vB - vB'
      diagU = LA.diag $ V.zipWith (*) u u
      mA' = LA.tr mA
      mC = LA.inv (mA' LA.<> mA)
  return $ mC LA.<> (mA' LA.<> diagU LA.<> mA) LA.<> mC

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
