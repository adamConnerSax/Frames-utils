{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DerivingVia         #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.Serialize
  (
    -- * Types
    SElField(..)
  , RecSerialize
  , RecBinary
    -- * Record coercions
  , toS
  , fromS
  )
where

import           Data.Coerce (coerce)
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V                 

import           Data.Binary                   as B
import           Data.Serialize                as S

import           GHC.Generics (Generic,Rep)
import           GHC.TypeLits (KnownSymbol)

newtype SElField t = SElField { unSElField :: V.ElField t } 
deriving via (V.ElField '(s,a)) instance (KnownSymbol s, Show a) => Show (SElField '(s,a)) 
deriving via (V.ElField '(s,a)) instance (KnownSymbol s) => Generic (SElField '(s,a)) 
deriving via (V.ElField '(s,a)) instance Eq a => Eq (SElField '(s,a))
deriving via (V.ElField '(s,a)) instance Ord a => Ord (SElField '(s,a))


toS :: V.RMap rs => V.Rec V.ElField rs -> V.Rec SElField rs
toS = V.rmap coerce

fromS :: V.RMap rs => V.Rec SElField rs -> V.Rec V.ElField rs
fromS = V.rmap coerce

-- those generic instances allow us to derive instances for the serialization libs
-- instance (S.Serialize (V.Snd t), V.KnownField t) => S.Serialize (V.ElField t)
instance (S.Serialize (V.Snd t), V.KnownField t) => S.Serialize (SElField t)
instance (B.Binary (V.Snd t), V.KnownField t) => B.Binary (SElField t)

type RecSerialize rs = (GSerializePut (Rep (V.Rec SElField rs))
                       , GSerializeGet (Rep (V.Rec SElField rs)) 
                       , Generic (V.Rec SElField rs))

instance RecSerialize rs => S.Serialize (V.Rec SElField rs)

type RecBinary rs = (GBinaryPut (Rep (V.Rec SElField rs))
                       , GBinaryGet (Rep (V.Rec SElField rs)) 
                       , Generic (V.Rec SElField rs))

instance RecBinary rs => B.Binary (V.Rec SElField rs)
