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

import qualified Control.Monad.Freer.Logger as FL
import qualified Data.Text                  as T

import           Numeric.LinearAlgebra      (( #> ), (<#), (<.>), (<\>))
import qualified Numeric.LinearAlgebra      as LA
import           Numeric.LinearAlgebra.Data (Matrix, R, Vector)
import qualified Numeric.LinearAlgebra.Data as LA


textSize :: (LA.Container c e, Show (LA.IndexOf c)) => c e -> T.Text
textSize = T.pack . show . LA.size

checkEqualVectors :: FL.Member (FL.Logger FL.LogEntry) effs => T.Text -> T.Text -> Vector R -> Vector R -> FL.Eff effs ()
checkEqualVectors nA nB vA vB =
  if (LA.size vA == LA.size vB)
  then return ()
  else FL.logLE FL.Error $ "Unequal vector length. length(" <> nA <> ")=" <> textSize vA <> " and length(" <> nB <> ")=" <> textSize vB

checkMatrixVector :: FL.Member (FL.Logger FL.LogEntry) effs => T.Text -> T.Text -> Matrix R -> Vector R -> FL.Eff effs ()
checkMatrixVector nA nB mA vB =
  if (snd (LA.size mA) == LA.size vB)
  then return ()
  else FL.logLE FL.Error $ "Bad matrix * vector lengths. dim(" <> nA <> ")=" <> textSize mA <> " and length(" <> nB <> ")=" <> textSize vB

checkVectorMatrix :: FL.Member (FL.Logger FL.LogEntry) effs => T.Text -> T.Text -> Vector R -> Matrix R -> FL.Eff effs ()
checkVectorMatrix nA nB vA mB =
  if (LA.size vA == fst (LA.size mB))
  then return ()
  else FL.logLE FL.Error $ "Bad vector * matrix lengths. length(" <> nA <> ")=" <> textSize vA <> " and dim(" <> nB <> ")=" <> textSize mB
