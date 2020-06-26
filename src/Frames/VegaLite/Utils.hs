{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE AllowAmbiguousTypes   #-}
module Frames.VegaLite.Utils
  (
    ToVLDataValue(..) -- derive this for any col type you wish to render as a datum for a VegaLite visualization
  , recordToVLDataRow
  , recordsToVLData
  , colName
  , colNames
  , pName
  , mName
  , fName
  , tName
  , hName
  , oName
  , dName
  ) where

import           Data.Fixed             (div', divMod')
import           Data.Text              (Text)
import qualified Data.Text              as T
import qualified Data.Time              as DT
import qualified Data.Vinyl             as V
import qualified Data.Vinyl.Functor     as V
import qualified Frames                 as F
import qualified Graphics.Vega.VegaLite as GV
import qualified Control.Foldl          as FL
import qualified Data.List              as List
import           Data.Proxy             (Proxy(..))

recordToVLDataRow' :: (V.RMap rs, V.ReifyConstraint ToVLDataValue F.ElField rs, V.RecordToList rs) => F.Record rs -> [(Text, GV.DataValue)]
recordToVLDataRow' xs = V.recordToList . V.rmap (\(V.Compose (V.Dict x)) -> V.Const $ toVLDataValue x) $ V.reifyConstraint @ToVLDataValue xs

recordToVLDataRow :: (V.RMap rs, V.ReifyConstraint ToVLDataValue F.ElField rs, V.RecordToList rs) => F.Record rs -> [GV.DataRow]
recordToVLDataRow r = GV.dataRow (recordToVLDataRow' r) []

recordsToVLData :: ({-as V.⊆ rs,-} V.RMap as, V.ReifyConstraint ToVLDataValue F.ElField as, V.RecordToList as, Foldable f)
                    => (F.Record rs -> F.Record as) -> f (F.Record rs) -> GV.Data
recordsToVLData transform xs = GV.dataFromRows [] $ List.concat $ fmap (recordToVLDataRow . transform) $ FL.fold FL.list xs

colName :: forall x. (F.ColumnHeaders '[x]) => Text
colName = List.head $ colNames @'[x] --T.pack $ List.head $ F.columnHeaders (Proxy :: Proxy (F.Record '[x]))

colNames :: forall rs. (F.ColumnHeaders rs) => [Text]
colNames = T.pack <$> F.columnHeaders (Proxy :: Proxy (F.Record rs))

pName :: forall x. (F.ColumnHeaders '[x]) => GV.PositionChannel
pName = GV.PName (colName @x)

mName :: forall x. (F.ColumnHeaders '[x]) => GV.MarkChannel
mName = GV.MName (colName @x)

fName :: forall x. (F.ColumnHeaders '[x]) => GV.FacetChannel
fName = GV.FName (colName @x)

tName :: forall x. (F.ColumnHeaders '[x]) => GV.TextChannel
tName = GV.TName (colName @x)

hName :: forall x. (F.ColumnHeaders '[x]) => GV.HyperlinkChannel
hName = GV.HName (colName @x)

oName :: forall x. (F.ColumnHeaders '[x]) => GV.OrderChannel
oName = GV.OName (colName @x)

dName :: forall x. (F.ColumnHeaders '[x]) => GV.DetailChannel
dName = GV.DName (colName @x)

class ToVLDataValue x where
  toVLDataValue :: x -> (Text, GV.DataValue)

instance ToVLDataValue (F.ElField '(s, Int)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Number $ realToFrac $ V.getField x)

instance ToVLDataValue (F.ElField '(s, Integer)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Number $ realToFrac $ V.getField x)

instance ToVLDataValue (F.ElField '(s, Double)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Number $ V.getField x)

instance ToVLDataValue (F.ElField '(s, Float)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Number $ realToFrac $ V.getField x)

instance ToVLDataValue (F.ElField '(s, String)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ V.getField x)

instance ToVLDataValue (F.ElField '(s, Text)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ V.getField x)

instance ToVLDataValue (F.ElField '(s, Bool)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Boolean $ V.getField x)

{-
instance ToVLDataValue (F.ElField '(s, Int32)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Number $ realToFrac $ V.getField x)

instance ToVLDataValue (F.ElField '(s, Int64)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Number $ realToFrac $ V.getField x)
-}

instance ToVLDataValue (F.ElField '(s, DT.Day)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.DateTime $ toVLDateTime $ V.getField x)

instance ToVLDataValue (F.ElField '(s, DT.TimeOfDay)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.DateTime $ toVLDateTime $ V.getField x)

instance ToVLDataValue (F.ElField '(s, DT.LocalTime)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.DateTime $ toVLDateTime $ V.getField x)


vegaLiteMonth :: Int -> GV.MonthName
vegaLiteMonth 1  = GV.Jan
vegaLiteMonth 2  = GV.Feb
vegaLiteMonth 3  = GV.Mar
vegaLiteMonth 4  = GV.Apr
vegaLiteMonth 5  = GV.May
vegaLiteMonth 6  = GV.Jun
vegaLiteMonth 7  = GV.Jul
vegaLiteMonth 8  = GV.Aug
vegaLiteMonth 9  = GV.Sep
vegaLiteMonth 10 = GV.Oct
vegaLiteMonth 11 = GV.Nov
vegaLiteMonth 12 = GV.Dec
vegaLiteMonth _  = undefined

{-
--this is what we should do once we can use time >= 1.9
vegaLiteDay :: DT.DayOfWeek -> GV.DayName
vegaLiteDay DT.Monday = GV.Mon
vegaLiteDay DT.Tuesday = GV.Tue
vegaLiteDay DT.Wednesday = GV.Wed
vegaLiteDay DT.Thursday = GV.Thu
vegaLiteDay DT.Friday = GV.Fri
vegaLiteDay DT.Saturday = GV.Sat
vegaLiteDay DT.Sunday = GV.Mon
-}

vegaLiteDay :: Int -> GV.DayName
vegaLiteDay 1 = GV.Mon
vegaLiteDay 2 = GV.Tue
vegaLiteDay 3 = GV.Wed
vegaLiteDay 4 = GV.Thu
vegaLiteDay 5 = GV.Fri
vegaLiteDay 6 = GV.Sat
vegaLiteDay 7 = GV.Mon
vegaLiteDay _ = undefined

vegaLiteDate :: DT.Day -> [GV.DateTime]
vegaLiteDate x = let (y,m,d) = DT.toGregorian x in [GV.DTYear (fromIntegral y), GV.DTMonth (vegaLiteMonth m), GV.DTDay (vegaLiteDay d)]

vegaLiteTime :: DT.TimeOfDay -> [GV.DateTime]
vegaLiteTime x =
  let (sec, remainder) = (DT.todSec x) `divMod'` 1
      ms = (1000 * remainder) `div'` 1
  in [GV.DTHours (DT.todHour x), GV.DTMinutes (DT.todMin x), GV.DTSeconds sec, GV.DTMilliseconds ms]

class ToVLDateTime x where
  toVLDateTime :: x -> [GV.DateTime]

instance ToVLDateTime DT.Day where
  toVLDateTime = vegaLiteDate

instance ToVLDateTime DT.TimeOfDay where
  toVLDateTime = vegaLiteTime

instance ToVLDateTime DT.LocalTime where
  toVLDateTime (DT.LocalTime day timeOfDay) = vegaLiteDate day ++ vegaLiteTime timeOfDay

