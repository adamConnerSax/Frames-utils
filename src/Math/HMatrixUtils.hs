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

import qualified Knit.Effect.Logger            as KL
import qualified Knit.Report                   as K
import qualified Data.Text                     as T

import qualified Numeric.LinearAlgebra         as LA
import           Numeric.LinearAlgebra.Data     ( Matrix
                                                , R
                                                , Vector
                                                )


textSize :: (LA.Container c e, Show (LA.IndexOf c)) => c e -> T.Text
textSize = T.pack . show . LA.size

checkEqualVectors
  :: K.Member (KL.Logger KL.LogEntry) effs
  => T.Text
  -> T.Text
  -> Vector R
  -> Vector R
  -> K.Sem effs ()
checkEqualVectors nA nB vA vB = if LA.size vA == LA.size vB
  then return ()
  else
    KL.logLE KL.Error
    $  "Unequal vector length. length("
    <> nA
    <> ")="
    <> textSize vA
    <> " and length("
    <> nB
    <> ")="
    <> textSize vB

checkMatrixVector
  :: K.Member (KL.Logger KL.LogEntry) effs
  => T.Text
  -> T.Text
  -> Matrix R
  -> Vector R
  -> K.Sem effs ()
checkMatrixVector nA nB mA vB = if snd (LA.size mA) == LA.size vB
  then return ()
  else
    KL.logLE KL.Error
    $  "Bad matrix * vector lengths. dim("
    <> nA
    <> ")="
    <> textSize mA
    <> " and length("
    <> nB
    <> ")="
    <> textSize vB

checkVectorMatrix
  :: K.Member (KL.Logger KL.LogEntry) effs
  => T.Text
  -> T.Text
  -> Vector R
  -> Matrix R
  -> K.Sem effs ()
checkVectorMatrix nA nB vA mB = if LA.size vA == fst (LA.size mB)
  then return ()
  else
    KL.logLE KL.Error
    $  "Bad vector * matrix lengths. length("
    <> nA
    <> ")="
    <> textSize vA
    <> " and dim("
    <> nB
    <> ")="
    <> textSize mB
