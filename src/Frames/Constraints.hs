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
  , ElemsOf
  , RecordShow
  )
where

import qualified Data.Vinyl                    as V
--import qualified Data.Vinyl.TypeLevel          as V
--import qualified Frames                        as F
import qualified Frames.Melt                   as F
--import qualified Frames.InCore                 as FI

--import           Data.Discrimination            ( Grouping)

-- | Type family to create constraints that apply a constraint taking a type-list
-- and a single type and apply it to all types in another list
type family EachIs (c :: [k] -> k -> Constraint) (rs :: [k]) (ts :: [k]) :: Constraint where
  EachIs _ _ '[] = ()
  EachIs c rs (t ': ts) = (c rs t, EachIs c rs ts)


type family ElemsOf (rs :: [k]) (es :: [k]) :: Constraint where
  ElemsOf _ '[] = ()
  ElemsOf rs (t ': ts) = (F.ElemOf rs t, ElemsOf rs ts)


type RecordShow rs = (V.RMap rs, V.ReifyConstraint Show V.ElField rs, V.RecordToList rs)
