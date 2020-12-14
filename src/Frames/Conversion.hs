{-# LANGUAGE AllowAmbiguousTypes #-}
--{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs #-}
--{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}

{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.Conversion
  (
    toList
  , toListVia
  , mkConverter
  ) where

import qualified Data.Vinyl           as V
import           Data.Vinyl.Functor   (Identity(..), Const(..))
import qualified Data.Vinyl.Functor as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames               as F


-- | convert a subset of fields, all of type a, to a list
toList :: forall a as rs. (V.RecordToList (V.Unlabeled as)
          , V.RecMapMethod ((~) a) Identity (V.Unlabeled as)
          , F.StripFieldNames as
          , as F.⊆ rs)
  => F.Rec F.ElField rs -> [a] --(F.RecordToList as, ) => F.Record F.ElField as -> [a]
toList = V.recordToList . V.rmapMethod @((~) a) (Const . getIdentity) . V.stripNames . F.rcast @as


type ToTypeRecF a = V.Lift (->) V.ElField (V.Const a)

toListVia :: (V.RecordToList rs, V.RApply rs) => V.Rec (ToTypeRecF a) rs -> F.Record rs -> [a]
toListVia converters = V.recordToList . V.rapply converters

mkConverter :: (V.KnownField t, V.Snd t ~ b) => (b -> a) -> ToTypeRecF a t
mkConverter f = V.Lift $ V.Const . f . V.getField 

{-
toMap :: forall a as rs. (V.RecordToList (V.Unlabeled as)
          , V.RecMapMethod ((~) a) Identity (V.Unlabeled as)
          , F.StripFieldNames as
          , as F.⊆ rs)
  => F.Rec F.ElField rs -> M.Map Text a --(F.RecordToList as, ) => F.Record F.ElField as -> [a]
toMap = M.fromList . V.recordToList . V.rmapMethod @((~) a) (Const . getIdentity) . V.stripNames . F.rcast @as
-}
