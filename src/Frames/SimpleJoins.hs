 {-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.SimpleJoins
  (
    appendFromKeyed
  , leftJoinM
  , leftJoinE
  , leftJoinWithMissing
  , JoinResult
  , CanLeftJoinM
  , CanLeftJoinWithMissing
  , leftJoinM3
  , leftJoinE3
  , leftJoin3WithMissing
  , JoinResult3
  , CanLeftJoinM3
  , CanLeftJoinWithMissing3
  , MissingKeys(..)
  ) where

import qualified Control.Foldl as FL
import qualified Data.Vinyl           as V
import Data.Vinyl.TypeLevel as V ( type (++) ) --(type (++), Snd)
import qualified Data.Map as M
import           Data.Maybe (fromJust) -- don't love fromJust here
import qualified Frames               as F
import qualified Frames.InCore as FI
import qualified Frames.Melt as F
import qualified Data.Discrimination.Grouping  as G

-- like a join but I'm not sure of all the properties a join should preserve/have...
-- returns an Either so it can tell you what key was missing upon failure
appendFromKeyed
  :: forall jc ac lc rc. (jc F.⊆ lc, ac F.⊆ lc, jc F.⊆ rc, Ord (F.Record jc), FI.RecVec (rc V.++ ac))
  => F.FrameRec lc -> F.FrameRec rc ->  Either (F.Record jc) (F.FrameRec (rc V.++ ac))
appendFromKeyed source appendTo = do
  let m = FL.fold (FL.premap (\r -> (F.rcast @jc r, F.rcast @ac r)) FL.map) source
      g r =
        let kr = F.rcast @jc r
        in maybe (Left kr) (Right . V.rappend r) $ M.lookup kr m
  F.toFrame <$> traverse g (FL.fold FL.list appendTo)
{-# INLINEABLE appendFromKeyed #-}


type CanLeftJoinM ks as bs = (FI.RecVec (as V.++ F.RDeleteAll ks bs)
                             , ks F.⊆ as
                             , ks F.⊆ bs
                             , as F.⊆ (as V.++ F.RDeleteAll ks bs)
                             , F.RDeleteAll ks bs F.⊆ bs
                             , V.RMap as
                             , V.RMap (as V.++ F.RDeleteAll ks bs)
                             , V.RecApplicative (F.RDeleteAll ks bs)
                             , G.Grouping (F.Record ks)
                             , FI.RecVec as
                             , FI.RecVec (F.RDeleteAll ks bs)
                             )

type JoinResult ks as bs = as V.++ F.RDeleteAll ks bs
-- I find this to be a more useful interface for the times when I need all join keys present in rhs
leftJoinM
  :: forall ks as bs. CanLeftJoinM ks as bs
  => F.FrameRec as
  -> F.FrameRec bs
  -> Maybe (F.FrameRec (JoinResult ks as bs))
leftJoinM fa fb = fmap F.toFrame $ sequence $ F.recMaybe <$> F.leftJoin @ks fa fb
{-# INLINEABLE leftJoinM #-}

-- TODO: There seem to be circumstances where the sequence returns Nothing but the list of missing is empty??

type CanLeftJoinWithMissing ks as bs = (CanLeftJoinM ks as bs
                                       , ks F.⊆ JoinResult ks as bs
                                       )

leftJoinWithMissing
  :: forall ks as bs.
  (CanLeftJoinWithMissing ks as bs)
  => F.FrameRec as
  -> F.FrameRec bs
  -> (F.FrameRec (JoinResult ks as bs), [F.Record ks])
leftJoinWithMissing  fa fb =
  let x = F.leftJoin @ks fa fb
      y = fmap F.recMaybe x
  in case sequence y of
    Just z -> (F.toFrame z, [])
    Nothing ->
      let missing = fmap (fromJust . F.recMaybe . F.rcast @ks . fst) . filter (\(_, z) -> isNothing z) $ zip (FL.fold FL.list x) y
          present = F.toFrame $ catMaybes y
      in (present, missing)
{-# INLINEABLE leftJoinWithMissing #-}

leftJoinE
  :: forall ks as bs. CanLeftJoinM ks as bs
  => F.FrameRec as
  -> F.FrameRec bs
  -> Either [F.Record ks] (F.FrameRec (JoinResult ks as bs))
leftJoinE fa fb =
  let x = F.leftJoin @ks fa fb
      y = fmap F.recMaybe x
  in case sequence y of
    Just z -> Right $ F.toFrame z
    Nothing -> Left $ fmap (F.rcast @ks . fst) . filter (\(_, z) -> isNothing z) $ zip (FL.fold FL.list fa) y
{-# INLINEABLE leftJoinE #-}


type CanLeftJoinM3 ks as bs cs = ( FI.RecVec (as V.++ F.RDeleteAll ks bs)
                                 , ks F.⊆ as
                                 , ks F.⊆ bs
                                 , as F.⊆ (as V.++ F.RDeleteAll ks bs)
                                 , F.RDeleteAll ks bs F.⊆ bs
                                 , V.RMap as
                                 , V.RMap (as V.++ F.RDeleteAll ks bs)
                                 , V.RecApplicative (F.RDeleteAll ks bs)
                                 , G.Grouping (F.Record ks)
                                 , FI.RecVec as
                                 , FI.RecVec (F.RDeleteAll ks bs)
                                 , FI.RecVec ((as V.++ F.RDeleteAll ks bs) V.++ F.RDeleteAll ks cs)
                                 , ks F.⊆ (as V.++ F.RDeleteAll ks bs)
                                 , ks F.⊆ cs
                                 , (as V.++ F.RDeleteAll ks bs) F.⊆ (as V.++ F.RDeleteAll ks bs V.++ F.RDeleteAll ks cs)
                                 , F.RDeleteAll ks cs F.⊆ cs
                                 , V.RMap (as V.++ F.RDeleteAll ks bs V.++ F.RDeleteAll ks cs)
                                 , V.RecApplicative (F.RDeleteAll ks cs)
                                 , FI.RecVec (F.RDeleteAll ks cs)
                                 )

type JoinResult3 ks as bs cs = (as ++ F.RDeleteAll ks bs) ++ F.RDeleteAll ks cs

type CanLeftJoinWithMissing3 ks as bs cs = (CanLeftJoinM3 ks as bs cs
                                           , ks F.⊆ JoinResult3 ks as bs cs
                                           )

-- I've found this useful
leftJoinM3
  :: forall ks as bs cs. CanLeftJoinM3 ks as bs cs
  => F.FrameRec as
  -> F.FrameRec bs
  -> F.FrameRec cs
  -> Maybe (F.FrameRec (JoinResult3 ks as bs cs))
leftJoinM3 fa fb fc = do
  fab <-  leftJoinM @ks fa fb
  leftJoinM @ks fab fc
{-# INLINEABLE leftJoinM3 #-}

data MissingKeys ks = FirstJoin [F.Record ks] | SecondJoin [F.Record ks]

deriving instance Show (F.Record ks) => Show (MissingKeys ks)

fromEithers :: Either [F.Record ks] (Either [F.Record ks] a) -> Either (MissingKeys ks) a
fromEithers e = case e of
  Left keys -> Left $ FirstJoin keys
  Right e' -> either (Left . SecondJoin) Right e'

leftJoin3WithMissing
  :: forall ks as bs cs.
  (CanLeftJoinWithMissing3 ks as bs cs)
  => F.FrameRec as
  -> F.FrameRec bs
  -> F.FrameRec cs
  -> (F.FrameRec (JoinResult3 ks as bs cs), [F.Record ks], [F.Record ks])
leftJoin3WithMissing fa fb fc =
  let (j1, m1) = leftJoinWithMissing @ks fa fb
      (j2, m2) = leftJoinWithMissing @ks j1 fc
  in (j2, m1, m2)
{-# INLINEABLE leftJoin3WithMissing #-}

leftJoinE3
  :: forall ks as bs cs. CanLeftJoinM3 ks as bs cs
  => F.FrameRec as
  -> F.FrameRec bs
  -> F.FrameRec cs
  -> Either (MissingKeys ks) (F.FrameRec (JoinResult3 ks as bs cs))
leftJoinE3 fa fb fc = do
  let fjE = leftJoinE @ks fa fb
  fromEithers $ fmap (\fj -> leftJoinE @ks fj fc) fjE
{-# INLINEABLE leftJoinE3 #-}
