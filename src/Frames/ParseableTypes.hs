{-# LANGUAGE CPP                  #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
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
#if __GLASGOW_HASKELL__ < 808
--import           Control.Monad.Fail (MonadFail(fail))
#endif  


newtype FrameDay = FrameDay { unFrameDay :: Time.Day } deriving (Show, Eq, Ord, Typeable)

type instance F.VectorFor FrameDay = V.Vector

instance R.Readable FrameDay where
  fromText t = fmap FrameDay $ do
    let parsedM = msum
          [
            Time.parseTime Time.defaultTimeLocale (Time.iso8601DateFormat Nothing) (T.unpack t)
          , Time.parseTime Time.defaultTimeLocale "%D" (T.unpack t)
          , Time.parseTime Time.defaultTimeLocale "%F" (T.unpack t)
          ]
    case parsedM of
      Just x -> return x
      Nothing -> fail (T.unpack $ "Parse Error reading \"" <> t <> "\" as Day") 

instance F.Parseable FrameDay where
--  parse = fmap F.Definitely . R.fromText

newtype FrameLocalTime = FrameLocalTime { unFrameLocalTime :: Time.LocalTime } deriving (Show, Eq, Ord, Typeable)

type instance F.VectorFor FrameLocalTime = V.Vector

instance R.Readable FrameLocalTime where
 fromText t = fmap FrameLocalTime $ do
   let parsedM = msum
         [
           Time.parseTimeM False Time.defaultTimeLocale "%c" (T.unpack t)
         ]
   case parsedM of
     Just x -> return x
     Nothing -> fail (T.unpack $ "Parse Error reading \"" <> t <> "\" as LocalTime") 

instance F.Parseable FrameLocalTime where
--  parse = fmap F.Definitely . R.fromText

type ColumnsWithDayAndLocalTime = FrameDay ': F.CommonColumns
