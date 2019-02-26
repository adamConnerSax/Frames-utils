{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE UndecidableInstances #-}
module Frames.ParseableTypes
  (
    ColumnsWithDayAndLocalTime
  )
where

import           Control.Monad         (MonadPlus, msum)
import           Control.Monad.Fail    (MonadFail)
import qualified Data.Readable         as R
import qualified Data.Text             as T
import qualified Data.Time.Calendar    as Time
import qualified Data.Time.Format      as Time
import qualified Data.Time.LocalTime   as Time
import qualified Data.Vector           as V
import qualified Frames                as F
import qualified Frames.ColumnTypeable as F
import qualified Frames.InCore         as F
type instance F.VectorFor Time.Day = V.Vector

instance F.Parseable Time.Day where
  parse t = fmap F.Definitely $
    msum
      [
        Time.parseTimeM False Time.defaultTimeLocale (Time.iso8601DateFormat Nothing) (T.unpack t)
      , Time.parseTimeM False Time.defaultTimeLocale "%D" (T.unpack t)
      , Time.parseTimeM False Time.defaultTimeLocale "%F" (T.unpack t)
      ]

type instance F.VectorFor Time.LocalTime = V.Vector

instance F.Parseable Time.LocalTime where
  parse t = fmap F.Definitely $
    msum
      [
        Time.parseTimeM False Time.defaultTimeLocale "%c" (T.unpack t)
      ]


type ColumnsWithDayAndLocalTime = Time.Day ': Time.LocalTime ': F.CommonColumns
