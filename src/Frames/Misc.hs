{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.Misc
  ( goodDataCount
  , goodDataByKey
  , filterOnField
  , filterOnMaybeField
  , widenAndCalcF
  , mapMergeFrames
  , mapMergeFramesE
  , mapMergeFramesToFrame
  , mapMergeFramesToFrameE
  , CField
  , CFieldOf
  , RealField
  , RealFieldOf
  , TField
  , TFieldOf
  )
where

import qualified Control.MapReduce             as MR
import qualified Control.Foldl                 as FL
import qualified Data.Foldable                 as F
import qualified Data.Map                      as M
import qualified Data.Map.Merge.Strict          as MM
import           Data.Maybe (fromJust)
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.XRec               as V
import           Frames                         ( (:.) )
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.Streamly.InCore        as FSI

-- | Given a set of records with possibly missing fields
-- that is a Rec (Maybe :. ElField), and a subset of columns (the `ks`) to use as a key,
-- count the number of rows attached to each key and the number where are all columns have data.
-- The key columns must exist in al rows or this will error out.
goodDataByKey
  :: forall ks rs
   . (ks F.⊆ rs, Ord (F.Record ks))
  => FL.Fold
       (F.Rec (Maybe F.:. F.ElField) rs)
       (M.Map (F.Record ks) (Int, Int))
goodDataByKey =
  let getKey = F.recMaybe . F.rcast @ks
  in  F.foldMap id <$> MR.mapReduceFold
        MR.noUnpack
        (MR.assign (fromJust . getKey) id)
        (MR.Reduce $ \k -> M.singleton k . FL.fold goodDataCount)

-- | Given a `Rec (Maybe :. ElField)` count total number of rows
-- and number of rows where all columns have data (all are `Just`)
goodDataCount :: FL.Fold (F.Rec (Maybe F.:. F.ElField) rs) (Int, Int)
goodDataCount =
  (,) <$> FL.length <*> FL.prefilter (isJust . F.recMaybe) FL.length

-- | Turn a function from the field type of one field in a record to Bool
-- into a function (F.Record rs -> Bool). Useful for filtering.
filterOnField
  :: forall k rs
   . (V.KnownField k, F.ElemOf rs k)
  => (V.Snd k -> Bool)
  -> F.Record rs
  -> Bool
filterOnField test = test . F.rgetField @k

-- | Turn a function from the field type of one field in a record to Bool
-- into a function from a `(Rec (Maybe :. ElField) -> Bool)`
-- Returns `False` for `Nothing`.
filterOnMaybeField
  :: forall k rs
   . (F.ElemOf rs k, V.IsoHKD F.ElField k)
  => (V.HKD F.ElField k -> Bool)
  -> F.Rec (Maybe :. F.ElField) rs
  -> Bool
filterOnMaybeField test = maybe False test . V.toHKD . F.rget @k


-- | Take a set of rows that can be mapped to a key, combine rows with same key and put into a map
-- then make a calculation from the map, which might fail if a key is missing.
-- This generalizes the pattern required to turn long data wide and make a calculation on the
-- wide data.
-- E.g., Each row is keyed by a color (Red, Green, Blue) and logs intensity.  You want
-- Sum(intensity(Green)) - Sum(intensity(Red)) + 2 * Avg(intensity(Blue))
widenAndCalcF :: Ord k
              => (F.Record as -> (k, a))
              -> FL.Fold a b
              -> (M.Map k b -> Maybe c)
              -> FL.FoldM Maybe (F.Record as) c
widenAndCalcF toKeyed combineF calc =
  let f (k, a) = (k, [a])
      g = calc . fmap (FL.fold combineF) . M.fromListWith (<>) . fmap (f . toKeyed)
  in MR.postMapM g (FL.generalize FL.list)


-- | merge two frames with common keys via map-merge and return a map
-- we wrap the merge/missing in applicative for error handling
mapMergeFrames :: forall ks as bs t d.
                  (Applicative t
                  , Ord (F.Record ks)
                  , as F.⊆ (ks V.++ as)
                  , bs F.⊆ (ks V.++ bs)
                  , ks F.⊆ (ks V.++ as)
                  , ks F.⊆ (ks V.++ bs)
                  )
               => (F.Record ks -> F.Record as -> F.Record bs -> t d)
               -> (F.Record ks -> F.Record bs -> t d)
               -> (F.Record ks -> F.Record as -> t d)
               -> F.FrameRec (ks V.++ as)
               -> F.FrameRec (ks V.++ bs)
               -> t (Map (F.Record ks) d)
mapMergeFrames whenMatched whenMissingA whenMissingB frameA frameB =
  let keyVal :: forall cs . (ks F.⊆ (ks V.++ cs), cs F.⊆ (ks V.++ cs)) => F.Record (ks V.++ cs) -> (F.Record ks, F.Record cs)
      keyVal r = (F.rcast r, F.rcast r)
      asMap :: forall cs .  (ks F.⊆ (ks V.++ cs), cs F.⊆ (ks V.++ cs)) => F.FrameRec (ks V.++ cs) -> Map (F.Record ks) (F.Record cs)
      asMap = FL.fold (FL.premap (keyVal @cs) FL.map)
  in MM.mergeA (MM.traverseMissing whenMissingB) (MM.traverseMissing whenMissingA)
     (MM.zipWithAMatched whenMatched)
     (asMap frameA) (asMap frameB)

-- | merge two frames with common keys via map-merge and return a map
-- wrap the result in an Either for handling keys in one map but not the other
mapMergeFramesE ::  forall ks as bs d.
                  (Ord (F.Record ks)
                  , as F.⊆ (ks V.++ as)
                  , bs F.⊆ (ks V.++ bs)
                  , ks F.⊆ (ks V.++ as)
                  , ks F.⊆ (ks V.++ bs)
                  )
                => (F.Record ks -> F.Record as -> F.Record bs -> d)
                -> (F.Record ks -> Text)
                -> Text
                -> Text
                -> F.FrameRec (ks V.++ as)
                -> F.FrameRec (ks V.++ bs)
                -> Either Text (Map (F.Record ks) d)
mapMergeFramesE whenMatched keyText aLabel bLabel = mapMergeFrames whenMatchedE (whenMissing aLabel) (whenMissing bLabel)
  where
    whenMatchedE x y z = Right $ whenMatched x y z
    whenMissing :: forall cs . Text -> F.Record ks -> F.Record cs -> Either Text d
    whenMissing t k _ = Left $ "Missing key=" <> keyText k <> " in " <> t


-- | merge two frames with common keys via map-merge and return a frame
mapMergeFramesToFrame ::  forall ks as bs t xs.
                          (Applicative t
                          , Ord (F.Record ks)
                          , as F.⊆ (ks V.++ as)
                          , bs F.⊆ (ks V.++ bs)
                          , ks F.⊆ (ks V.++ as)
                          , ks F.⊆ (ks V.++ bs)
                          , FSI.RecVec (ks V.++ xs)
                          )
                      => (F.Record ks -> F.Record as -> F.Record bs -> t (F.Record xs))
                      -> (F.Record ks -> F.Record bs -> t (F.Record xs))
                      -> (F.Record ks -> F.Record as -> t (F.Record xs))
                      -> F.FrameRec (ks V.++ as)
                      -> F.FrameRec (ks V.++ bs)
                      -> t (F.FrameRec (ks V.++ xs))
mapMergeFramesToFrame whenMatched whenMissingA whenMissingB frameA frameB =
  toFrame <$> mapMergeFrames whenMatched whenMissingA whenMissingB frameA frameB
  where
    toFrame = F.toFrame . fmap (\(k, v) -> k F.<+> v) . M.toList


-- | merge two frames with common keys via map-merge and return a map
-- wrap the result in an Either for handling keys in one map but not the other
mapMergeFramesToFrameE ::  forall ks as bs xs.
                           (Ord (F.Record ks)
                           , as F.⊆ (ks V.++ as)
                           , bs F.⊆ (ks V.++ bs)
                           , ks F.⊆ (ks V.++ as)
                           , ks F.⊆ (ks V.++ bs)
                           , FSI.RecVec (ks V.++ xs)
                           )
                       => (F.Record ks -> F.Record as -> F.Record bs -> F.Record xs)
                       -> (F.Record ks -> Text)
                       -> Text
                       -> Text
                       -> F.FrameRec (ks V.++ as)
                       -> F.FrameRec (ks V.++ bs)
                       -> Either Text (F.FrameRec (ks V.++ xs))
mapMergeFramesToFrameE whenMatched keyText aLabel bLabel = mapMergeFramesToFrame whenMatchedE (whenMissing aLabel) (whenMissing bLabel)
  where
    whenMatchedE x y z = Right $ whenMatched x y z
    whenMissing :: Text -> F.Record ks -> F.Record cs -> Either Text q
    whenMissing t k _ = Left $ "Missing key=" <> keyText k <> " in " <> t



-- | Constraint type to make it easier to specify that a field exists and has a specific constraint
-- often used to specify a numerical type of certain fields (Num, Real, RealFrac) so they can be used
-- in various numerical calculations
type CField c x = (V.KnownField x, c (V.Snd x))

-- | Class to easily extend the CField constraint to a set of fields in a record
class (CField c x, F.ElemOf rs x) => CFieldOf c rs x
instance (CField c x, F.ElemOf rs x) => CFieldOf c rs x

-- | Frequently used specific version of CField
type RealField x = CField Real x
class (RealField x, x V.∈ rs) => RealFieldOf rs x
instance (RealField x, x V.∈ rs) => RealFieldOf rs x

-- | Constraint type to specify the specific type of a known field.
-- Used like CField but when the type must be speicific, like a Double or an Int
type TField t x = (V.KnownField x, t ~ V.Snd x)

-- | Class to extend the TField constraint to a set of fields in a record
class (TField t x, F.ElemOf rs x) => TFieldOf t rs x
instance (TField t x, F.ElemOf rs x) => TFieldOf t rs x
