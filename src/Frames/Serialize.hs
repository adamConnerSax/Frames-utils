{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DerivingVia         #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
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
  , SFrame (..)
  )
where

--import qualified Control.Foldl as FL
import qualified Control.Monad.ST as ST
import           Data.Coerce (coerce)
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V                 

import           Data.Binary                   as B
import           Data.Binary.Put               as B
import           Data.Binary.Get               as B
import Data.Functor.Identity (Identity (..))
import           Data.Serialize                as S
import qualified Frames as F
import qualified Frames.InCore as FI
import qualified Frames.Streamly.InCore as FS

import           GHC.Generics (Generic,Rep)
import           GHC.TypeLits (KnownSymbol)

import qualified Streamly
import qualified Streamly.Prelude as Streamly
--import qualified Streamly.Fold
import qualified Streamly.Internal.Data.Fold as Streamly.Fold
--import qualified Streamly.Internal.Data.Fold.Types as Streamly.Fold



newtype SElField t = SElField { unSElField :: V.ElField t } 
deriving via (V.ElField '(s,a)) instance (KnownSymbol s, Show a) => Show (SElField '(s,a)) 
deriving via (V.ElField '(s,a)) instance (KnownSymbol s) => Generic (SElField '(s,a)) 
deriving via (V.ElField '(s,a)) instance Eq a => Eq (SElField '(s,a))
deriving via (V.ElField '(s,a)) instance Ord a => Ord (SElField '(s,a))


toS :: V.RMap rs => V.Rec V.ElField rs -> V.Rec SElField rs
toS = V.rmap coerce
{-# INLINE toS #-}

fromS :: V.RMap rs => V.Rec SElField rs -> V.Rec V.ElField rs
fromS = V.rmap coerce
{-# INLINE fromS #-}

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


type SFrameRec rs = SFrame (F.Record rs)

newtype SFrame a = SFrame { unSFrame :: F.Frame a }

-- Cereal
instance (V.RMap rs, FI.RecVec rs, RecSerialize rs) => S.Serialize (SFrame (F.Record rs)) where
  put = streamlyPutC . Streamly.map toS . Streamly.fromFoldable . unSFrame
  get = sframeGetC

streamlyPutC :: S.Serialize a => S.Putter (Streamly.SerialT Identity a)
streamlyPutC s = do
  let lengthF = Streamly.Fold.length
      putF = Streamly.Fold.Fold (\b a -> return $ b <> S.put a) mempty return
      (l, streamPut) = runIdentity $ Streamly.fold (Streamly.Fold.tee lengthF putF) s
  S.putWord64be $ fromIntegral l
  streamPut

sframeGetC :: forall rs. (FI.RecVec rs, V.RMap rs, RecSerialize rs) => S.Get (SFrameRec rs)
sframeGetC = go Streamly.nil =<< S.getWord64be where
  go :: (forall s.Streamly.SerialT (ST.ST s) (F.Rec SElField rs)) -> Word64 -> S.Get (SFrameRec rs)
  go s nLeft =
    if nLeft == 0
    then return $ SFrame $ ST.runST $ FS.inCoreAoS $ Streamly.map fromS s
    else do
      a <- S.get
      go (Streamly.cons a s) (nLeft - 1)

-- Binary
instance (V.RMap rs, FI.RecVec rs, RecBinary rs) => B.Binary (SFrame (F.Record rs)) where
  put = streamlyPutB . Streamly.map toS . Streamly.fromFoldable . unSFrame
  get = sframeGetB

streamlyPutB :: B.Binary a => Streamly.SerialT Identity a -> B.Put
streamlyPutB s = do
  let lengthF = Streamly.Fold.length
      putF = Streamly.Fold.Fold (\b a -> return $ b <> B.put a) mempty return
      (l, streamPut) = runIdentity $ Streamly.fold (Streamly.Fold.tee lengthF putF) s
  B.putWord64be $ fromIntegral l
  streamPut

sframeGetB :: forall rs. (FI.RecVec rs, V.RMap rs, RecBinary rs) => B.Get (SFrameRec rs)
sframeGetB = go Streamly.nil =<< B.getWord64be where
  go :: (forall s.Streamly.SerialT (ST.ST s) (F.Rec SElField rs)) -> Word64 -> B.Get (SFrameRec rs)
  go s nLeft =
    if nLeft == 0
    then return $ SFrame $ ST.runST $ FS.inCoreAoS $ Streamly.map fromS s
    else do
      a <- B.get
      go (Streamly.cons a s) (nLeft - 1)

