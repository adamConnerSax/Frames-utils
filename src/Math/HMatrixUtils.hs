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

module Math.HMatrixUtils where

import qualified Data.Text                     as T

import qualified Numeric.LinearAlgebra         as LA
import           Numeric.LinearAlgebra.Data     ( Matrix
                                                , R
                                                , Vector
                                                )
import qualified Say

textSize :: (LA.Container c e, Show (LA.IndexOf c)) => c e -> T.Text
textSize = T.pack . show . LA.size

checkEqualVectors
  :: MonadIO m
  => T.Text
  -> T.Text
  -> Vector R
  -> Vector R
  -> m ()
checkEqualVectors nA nB vA vB = if LA.size vA == LA.size vB
  then return ()
  else Say.say
       $ "Unequal vector length. length("
       <> nA
       <> ")="
       <> textSize vA
       <> " and length("
       <> nB
       <> ")="
       <> textSize vB

checkMatrixVector
  :: MonadIO m
  => T.Text
  -> T.Text
  -> Matrix R
  -> Vector R
  -> m ()
checkMatrixVector nA nB mA vB = if snd (LA.size mA) == LA.size vB
  then return ()
  else Say.say
       $  "Bad matrix * vector lengths. dim("
       <> nA
       <> ")="
       <> textSize mA
       <> " and length("
       <> nB
       <> ")="
       <> textSize vB

checkVectorMatrix
  :: MonadIO m
  => T.Text
  -> T.Text
  -> Vector R
  -> Matrix R
  -> m ()
checkVectorMatrix nA nB vA mB = if LA.size vA == fst (LA.size mB)
  then return ()
  else Say.say
       $  "Bad vector * matrix lengths. length("
       <> nA
       <> ")="
       <> textSize vA
       <> " and dim("
       <> nB
       <> ")="
       <> textSize mB
