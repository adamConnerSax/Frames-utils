{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE UndecidableInstances #-}
module Frames.ParseableTypes
  (
    FrameDay(..)
  , FrameLocalTime(..)
  , ColumnsWithDayAndLocalTime
  )
where

import           Control.Monad         (msum)
import qualified Data.Readable         as R
import qualified Data.Text             as T
import qualified Data.Time.Calendar    as Time
import qualified Data.Time.Format      as Time
import qualified Data.Time.LocalTime   as Time
import           Data.Typeable         (Typeable)
import qualified Data.Vector           as V
import qualified Frames                as F
import qualified Frames.ColumnTypeable as F
import qualified Frames.InCore         as F

newtype FrameDay = FrameDay { unFrameDay :: Time.Day } deriving (Show, Eq, Ord, Typeable)

type instance F.VectorFor FrameDay = V.Vector

instance R.Readable FrameDay where
  fromText t = fmap FrameDay $ do
    msum
      [
        Time.parseTimeM False Time.defaultTimeLocale (Time.iso8601DateFormat Nothing) (T.unpack t)
      , Time.parseTimeM False Time.defaultTimeLocale "%D" (T.unpack t)
      , Time.parseTimeM False Time.defaultTimeLocale "%F" (T.unpack t)
      ]

instance F.Parseable FrameDay where
--  parse = fmap F.Definitely . R.fromText

newtype FrameLocalTime = FrameLocalTime { unFrameLocalTime :: Time.LocalTime } deriving (Show, Eq, Ord, Typeable)

type instance F.VectorFor FrameLocalTime = V.Vector

instance R.Readable FrameLocalTime where
 fromText t = fmap FrameLocalTime $
    msum
      [
        Time.parseTimeM False Time.defaultTimeLocale "%c" (T.unpack t)
      ]

instance F.Parseable FrameLocalTime where
--  parse = fmap F.Definitely . R.fromText

type ColumnsWithDayAndLocalTime = FrameDay ': F.CommonColumns
