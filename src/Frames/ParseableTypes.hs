{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE UndecidableInstances #-}
module Frames.ParseableTypes
  ( FrameDay(..)
  , FrameLocalTime(..)
  , ColumnsWithDayAndLocalTime
  )
where

import           Control.Monad                  ( msum)
import qualified Data.Readable                 as R
import qualified Data.Serialize                as S
import           Data.Time.Calendar.Serialize()
import           Data.Time.LocalTime.Serialize()
import qualified Data.Text                     as T
import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Format              as Time
import qualified Data.Time.LocalTime           as Time
import qualified Data.Vector                   as V
import qualified Frames                        as F
import qualified Frames.Streamly.ColumnTypeable         as FS
import qualified Frames.Streamly.InCore                 as FS
import qualified Frames.ShowCSV as F

newtype FrameDay = FrameDay { unFrameDay :: Time.Day } deriving (Show, Eq, Ord, Typeable, Generic)

type instance FS.VectorFor FrameDay = V.Vector
instance F.ShowCSV FrameDay where
  showCSV = T.pack . show . unFrameDay

instance S.Serialize FrameDay where
  put = S.put . Time.toModifiedJulianDay . unFrameDay
  get = FrameDay . Time.ModifiedJulianDay <$> S.get

-- using parseTime instead of parseTimeM is deprecated.  But parseTimeM requires MonadFail
-- and Readable and F.Parseable require MonadPlus and I can't see how to bridge it
-- except maybe reflection (??) to add the MonadFail instance on the fly via
-- fail _ = mzero

instance R.Readable FrameDay where
  fromText t = fmap FrameDay $ do
    let parsedM = msum
          [
            Time.parseTimeM True Time.defaultTimeLocale (Time.iso8601DateFormat Nothing) (T.unpack t)
          , Time.parseTimeM True Time.defaultTimeLocale "%0m/%d/%y" (T.unpack t)
          , Time.parseTimeM True Time.defaultTimeLocale "%0m/%d/%Y" (T.unpack t)
          , Time.parseTimeM True Time.defaultTimeLocale "%D" (T.unpack t)
          , Time.parseTimeM True Time.defaultTimeLocale "%F" (T.unpack t)
          ]
    maybe mzero return parsedM --fail (T.unpack $ "Parse Error reading \"" <> t <> "\" as Day")

instance FS.Parseable FrameDay where
  parse = fmap FS.Definitely . R.fromText

newtype FrameLocalTime = FrameLocalTime { unFrameLocalTime :: Time.LocalTime } deriving (Show, Eq, Ord, Typeable, Generic)

type instance FS.VectorFor FrameLocalTime = V.Vector
instance S.Serialize FrameLocalTime
instance F.ShowCSV FrameLocalTime where
  showCSV = T.pack . show . unFrameLocalTime

instance R.Readable FrameLocalTime where
 fromText t = fmap FrameLocalTime $ do
   let parsedM = msum
         [
           Time.parseTimeM False Time.defaultTimeLocale "%c" (T.unpack t)
         ]
   maybe mzero return parsedM --fail (T.unpack $ "Parse Error reading \"" <> t <> "\" as LocalTime")

instance FS.Parseable FrameLocalTime where
  parse = fmap FS.Definitely . R.fromText

type ColumnsWithDayAndLocalTime = FrameDay ': (FrameLocalTime ': F.CommonColumns)
