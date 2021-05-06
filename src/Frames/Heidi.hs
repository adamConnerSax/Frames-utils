{-# LANGUAGE ConstraintKinds  #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
module Frames.Heidi where

import qualified Frames
import qualified Heidi
import qualified Data.Vinyl as Vinyl
import qualified Data.Vinyl.TypeLevel as Vinyl


--class Heidi.Heidi (Vinyl.Snd t) => HeidiField t where
--  toHeidiVal :: t -> Heidi.Val
--  toHeidiVal = Heidi.toVal . Vinyl.getField

flattenList :: Heidi.Val -> [([Heidi.TC], Heidi.VP)]
flattenList = Heidi.flatten [] (\tcs vp kvs -> (tcs, vp) : kvs)

labelAndFlatten :: String -> Heidi.Val -> [([Heidi.TC], Heidi.VP)]
labelAndFlatten l = fmap (first (Heidi.mkTyN l :)) . flattenList

--instance Heidi.Heidi (Vinyl.Snd t) => HeidiField t
class RecordToHeidi rs where
  recordToKVs :: Vinyl.Rec Vinyl.ElField rs -> [([Heidi.TC], Heidi.VP)]

instance RecordToHeidi '[] where
  recordToKVs _ = []

instance (Heidi.Heidi (Vinyl.Snd t), Vinyl.KnownField t, RecordToHeidi rs) => RecordToHeidi (t ': rs) where
  recordToKVs (x Vinyl.:& rs) = labelAndFlatten (Vinyl.getLabel x) (Heidi.toVal $ Vinyl.getField x) ++ recordToKVs rs

recordToHeidiRow :: RecordToHeidi rs => Vinyl.Rec Vinyl.ElField rs -> Heidi.Row [Heidi.TC] Heidi.VP
recordToHeidiRow = Heidi.rowFromList . recordToKVs
