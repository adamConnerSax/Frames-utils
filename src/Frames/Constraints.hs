{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.Constraints
  ( EachIs
  , LeftJoinC
  )
where

import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI

import           Data.Discrimination            ( Grouping)

-- | Type family to create constraints that apply a constraint taking a type-list
-- and a single type and apply it to all types in another list
type family EachIs (c :: [k] -> k -> Constraint) (rs :: [k]) (ts :: [k]) :: Constraint where
  EachIs _ _ '[] = ()
  EachIs c rs (t ': ts) = (c rs t, EachIs c rs ts)


-- leftJoin @fs (F.FrameRec @rs) (F.FrameRec @rs2) :: F.FrameRec @rs2'
type LeftJoinC fs rs rs2 rs2' = (fs F.⊆ rs
                                , fs F.⊆ rs2
                                , rs F.⊆ (rs V.++ rs2')
                                , rs2' F.⊆ rs2
                                , rs2' ~ F.RDeleteAll fs rs2
                                , V.RMap rs
                                , V.RMap (rs V.++ rs2')
                                , V.RecApplicative rs2'
                                , Grouping (F.Record fs)
                                , FI.RecVec rs
                                , FI.RecVec rs2'
                                , FI.RecVec (rs V.++ rs2')
                                )
