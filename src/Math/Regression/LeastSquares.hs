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

import qualified System.PipesLogger         as SL

import           Numeric.LinearAlgebra      (( #> ), (<#), (<.>))
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

ordinaryLS :: Monad m => Matrix R -> Vector R -> SL.Logger m (RegressionResult R)
ordinaryLS mA vB = do
  -- check dimensions
  let textSize = T.pack . show . LA.size
  SL.log SL.Diagnostic $ "ordinaryLS called with dim(mA) =" <> textSize mA <> " and dim(mB)=" <> textSize vB
  if (fst (LA.size mA) /= LA.size vB)
    then SL.log SL.Error $ "A is " <> textSize mA <> " but b is length " <> textSize vB
    else return ()
  let mA' = LA.tr mA
      vX = ((LA.inv (mA' LA.<> mA)) LA.<> mA') #> vB
      vR = vB - (mA #> vX)
  return $ RegressionResult vX vR
