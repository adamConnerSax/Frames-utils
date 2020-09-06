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

import           Control.Monad                  ( msum
                                                , mzero
                                                )
import qualified Data.Readable                 as R
import qualified Data.Serialize                as S
import           Data.Time.Calendar.Serialize()
import           Data.Time.LocalTime.Serialize()
import qualified Data.Text                     as T
import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Format              as Time
import qualified Data.Time.LocalTime           as Time
import           Data.Typeable                  ( Typeable )
import qualified Data.Vector                   as V
import qualified Frames                        as F
import qualified Frames.ColumnTypeable         as F
import qualified Frames.InCore                 as F
import qualified Frames.ShowCSV as F

import           GHC.Generics                   ( Generic )

newtype FrameDay = FrameDay { unFrameDay :: Time.Day } deriving (Show, Eq, Ord, Typeable, Generic)

type instance F.VectorFor FrameDay = V.Vector
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
    case parsedM of
      Just x -> return x
      Nothing -> mzero --fail (T.unpack $ "Parse Error reading \"" <> t <> "\" as Day")

instance F.Parseable FrameDay where
  parse = fmap F.Definitely . R.fromText

newtype FrameLocalTime = FrameLocalTime { unFrameLocalTime :: Time.LocalTime } deriving (Show, Eq, Ord, Typeable, Generic)

type instance F.VectorFor FrameLocalTime = V.Vector
instance S.Serialize FrameLocalTime
instance F.ShowCSV FrameLocalTime where
  showCSV = T.pack . show . unFrameLocalTime

instance R.Readable FrameLocalTime where
 fromText t = fmap FrameLocalTime $ do
   let parsedM = msum
         [
           Time.parseTimeM False Time.defaultTimeLocale "%c" (T.unpack t)
         ]
   case parsedM of
     Just x -> return x
     Nothing -> mzero --fail (T.unpack $ "Parse Error reading \"" <> t <> "\" as LocalTime")

instance F.Parseable FrameLocalTime where
  parse = fmap F.Definitely . R.fromText

type ColumnsWithDayAndLocalTime = FrameDay ': (FrameLocalTime ': F.CommonColumns)
