{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.Conversion
  (
    toList
  ) where

import qualified Data.Vinyl           as V
import           Data.Vinyl.Functor   (Identity(..), Const(..))
import qualified Frames               as F


-- | convert a subset of fields, all of type a, to a list
toList :: forall a as rs. (V.RecordToList (V.Unlabeled as)
          , V.RecMapMethod ((~) a) Identity (V.Unlabeled as)
          , F.StripFieldNames as
          , as F.⊆ rs)
  => F.Rec F.ElField rs -> [a] --(F.RecordToList as, ) => F.Record F.ElField as -> [a]
toList = V.recordToList . V.rmapMethod @((~) a) (Const . getIdentity) . V.stripNames . F.rcast @as


{-
toMap :: forall a as rs. (V.RecordToList (V.Unlabeled as)
          , V.RecMapMethod ((~) a) Identity (V.Unlabeled as)
          , F.StripFieldNames as
          , as F.⊆ rs)
  => F.Rec F.ElField rs -> M.Map Text a --(F.RecordToList as, ) => F.Record F.ElField as -> [a]
toMap = M.fromList . V.recordToList . V.rmapMethod @((~) a) (Const . getIdentity) . V.stripNames . F.rcast @as
-}
