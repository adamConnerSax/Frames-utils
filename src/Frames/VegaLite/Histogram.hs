{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
module Frames.VegaLite.Histogram
  ( singleHistogram
  , multiHistogram
  , MultiHistogramStyle(..)
  )
where

import qualified Frames.Misc                  as FU
import qualified Frames.VegaLite.Utils         as FV

import qualified Control.Foldl                 as FL
import qualified Data.List                     as List
import qualified Data.Map                      as M
import qualified Data.Text                     as T
import qualified Data.Vector                   as VB
import qualified Data.Vector.Unboxed           as VU
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Graphics.Vega.VegaLite        as GV

import qualified Data.Histogram                as H
import qualified Data.Histogram.Fill           as H

#if MIN_VERSION_hvega(0,4,0)
gvTitle :: Text -> GV.PropertySpec
gvTitle x = GV.title x []
#else
gvTitle :: Text -> (GV.VLProperty, GV.VLSpec)
gvTitle = GV.title
#endif

-- | Histograms
-- | Single, stacked, side-by-side, and faceted

data MultiHistogramStyle = StackedBar | AdjacentBar

singleHistogram
  :: forall x rs f
   . (FU.RealFieldOf rs x, Foldable f)
  => Text
  -> Maybe T.Text
  -> Int
  -> Maybe (V.Snd x)
  -> Maybe (V.Snd x)
  -> Bool
  -> f (F.Record rs)
  -> GV.VegaLite
singleHistogram title yLabelM nBins minM maxM addOutOfRange rows
  = let
      yLabel          = fromMaybe "count" yLabelM
      xLabel          = FV.colName @x
      width :: Double = 800 -- I need to make this settable, obv
      bandSize        = (width / realToFrac nBins) - 10.0
      vecX            = FL.fold
        (FL.premap (realToFrac . F.rgetField @x) (FL.vector @VU.Vector))
        rows
      minVal = maybe (VU.minimum vecX) realToFrac minM
      maxVal = maybe (VU.maximum vecX) realToFrac maxM
      bins   = H.binD minVal nBins maxVal
      hVec   = makeHistogram addOutOfRange bins vecX
      toVLRow (bv, ct) =
        GV.dataRow [(xLabel, GV.Number bv), ("count", GV.Number ct)] []
      dat =
        GV.dataFromRows [] $ List.concat $ VB.toList $ toVLRow <$> VB.convert
          hVec
      encX = GV.position GV.X [FV.pName @x, GV.PmType GV.Quantitative]
      encY = GV.position
        GV.Y
        [ GV.PName "count"
        , GV.PmType GV.Quantitative
        , GV.PAxis [GV.AxTitle yLabel]
        ]
      hBar = GV.mark GV.Bar [GV.MBinSpacing 1, GV.MSize bandSize]
      hEnc = encX . encY
      configuration =
        GV.configure
          . GV.configuration (GV.ViewStyle [GV.ViewContinuousWidth width, GV.ViewContinuousHeight 800])
          . GV.configuration (GV.PaddingStyle $ GV.PSize 50)
      vl = GV.toVegaLite
        [gvTitle title, dat, (GV.encoding . hEnc) [], hBar, configuration []]
    in
      vl


multiHistogram
  :: forall x c rs f
   . ( FU.RealFieldOf rs x
     , V.KnownField c
     , F.ElemOf rs c
     , FV.ToVLDataValue (F.ElField c)
     , V.KnownField c
     , Ord (V.Snd c)
     , Foldable f
     )
  => Text
  -> Maybe T.Text
  -> Int
  -> Maybe (V.Snd x)
  -> Maybe (V.Snd x)
  -> Bool
  -> MultiHistogramStyle
  -> f (F.Record rs)
  -> GV.VegaLite
multiHistogram title yLabelM nBins minM maxM addOutOfRange mhStyle rows
  = let
      yLabel       = fromMaybe "count" yLabelM
      xLabel       = FV.colName @x
      width :: Int = 800 -- I need to make this settable, obv
      allXF = FL.premap (realToFrac . F.rgetField @x) (FL.vector @VB.Vector)
      mapByCF =
        let ff m r = M.insertWith (++)
                                  (F.rgetField @c r)
                                  (pure $ realToFrac $ F.rgetField @x r)
                                  m -- FIX ++.  Ugh.
        in  FL.Fold ff M.empty (fmap VU.fromList)
      (vecAllX, mapByC) = FL.fold ((,) <$> allXF <*> mapByCF) rows
      minVal            = maybe (VB.minimum vecAllX) realToFrac minM
      maxVal            = maybe (VB.maximum vecAllX) realToFrac maxM
      bins              = H.binD minVal nBins maxVal
      makeRow k (bv, ct) = GV.dataRow
        [ (xLabel , GV.Number bv)
        , ("count", GV.Number ct)
        , FV.toVLDataValue (V.Field @c k)
        ]
        []
      makeRowsForOne (c, v) =
        let binned = makeHistogram addOutOfRange bins v
        in  List.concat $ VB.toList $ makeRow c <$> VB.convert binned
      dat = GV.dataFromRows [] $ List.concat $ makeRowsForOne <$> M.toList
        mapByC
      encY = GV.position
        GV.Y
        [ GV.PName "count"
        , GV.PmType GV.Quantitative
        , GV.PAxis [GV.AxTitle yLabel]
        ]
      encC         = GV.color [FV.mName @c, GV.MmType GV.Nominal]
      (hEnc, hBar) = case mhStyle of
        StackedBar ->
          let encX = GV.position GV.X [FV.pName @x, GV.PmType GV.Quantitative]
              bandSize = (realToFrac width / realToFrac nBins) - 2
              hBar' = GV.mark GV.Bar [GV.MBinSpacing 1, GV.MSize bandSize]
          in  (encX . encY . encC, hBar')
        AdjacentBar ->
          let encX = GV.position
                GV.X
                [FV.pName @c, GV.PmType GV.Nominal, GV.PAxis [GV.AxTitle ""]]
              encF  = GV.column [FV.fName @x, GV.FmType GV.Quantitative]
              hBar' = GV.mark GV.Bar [GV.MBinSpacing 1]
          in  (encX . encY . encC . encF, hBar')
      configuration =
        GV.configure
          . GV.configuration (GV.ViewStyle [GV.ViewContinuousWidth 800, GV.ViewContinuousHeight 800])
          . GV.configuration (GV.PaddingStyle $ GV.PSize 50)
      vl = GV.toVegaLite
        [gvTitle title, dat, (GV.encoding . hEnc) [], hBar, configuration []]
    in
      vl


makeHistogram
  :: Bool -> H.BinD -> VU.Vector Double -> VU.Vector (H.BinValue H.BinD, Double)
makeHistogram addOutOfRange bins vecX =
  let histo =
        H.fillBuilder (H.mkSimple bins)
        (VB.convert vecX :: VB.Vector Double)
      hVec              = H.asVector histo
      minIndex          = 0
      maxIndex          = VU.length hVec - 1
      (minBV, minCount) = hVec VU.! minIndex
      (maxBV, maxCount) = hVec VU.! maxIndex
      newMin            = (minBV, minCount + fromMaybe 0 (H.underflows histo))
      newMax            = (maxBV, maxCount + fromMaybe 0 (H.overflows histo))
  in  if addOutOfRange
        then hVec VU.// [(minIndex, newMin), (maxIndex, newMax)]
        else hVec
