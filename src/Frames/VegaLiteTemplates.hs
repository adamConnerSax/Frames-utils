{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE AllowAmbiguousTypes   #-}
module Frames.VegaLiteTemplates
  (
    clustersWithClickIntoVL
  ) where

import qualified Frames.VegaLite as FV

import           Data.Text              (Text)
import qualified Data.Text              as T
import qualified Data.Vinyl             as V
import qualified Data.Vinyl.Core        as V
import qualified Data.Vinyl.Functor     as V
import qualified Data.Vinyl.TypeLevel   as V
import qualified Data.Vinyl.Lens        as V
import qualified Frames                 as F
import qualified Frames.Melt            as F
import qualified Graphics.Vega.VegaLite as GV
import qualified Control.Foldl          as FL
import qualified Data.List              as List
import           Data.Proxy             (Proxy(..))

clustersWithClickIntoVL :: forall x y ic cid ml as. ( F.ColumnHeaders '[x]
                                                    , F.ColumnHeaders '[y]
                                                    , F.ColumnHeaders '[ml]
                                                    , F.ColumnHeaders '[ic]
                                                    , F.ColumnHeaders ('[cid] V.++ as)                                                                                             
                                                    )
                        => Text -> Text -> Text -> GV.BuildLabelledSpecs -> GV.BuildLabelledSpecs -> GV.BuildLabelledSpecs -> GV.BuildLabelledSpecs -> GV.Data -> GV.VegaLite
clustersWithClickIntoVL xAxisTitle yAxisTitle title pointEncoding calcFields extraSelection extraFilter dat =
  let posEncodingT = GV.position GV.X
                     [
                       FV.pName @x
                     , GV.PmType GV.Quantitative
                     , GV.PAxis [GV.AxTitle xAxisTitle]
                     ]
                     . GV.position GV.Y
                     [FV.pName @y
                     , GV.PmType GV.Quantitative
                     , GV.PAxis [GV.AxTitle yAxisTitle]
                     ]
      posEncodingB = GV.position GV.X
                     [
                       FV.pName @x
                     , GV.PmType GV.Quantitative
                     , GV.PAxis [GV.AxTitle xAxisTitle]
                     , GV.PScale [GV.SZero False]
                     ]
                     . GV.position GV.Y
                     [
                       FV.pName @y
                     , GV.PmType GV.Quantitative
                     , GV.PAxis [GV.AxTitle yAxisTitle]
                     , GV.PScale [GV.SZero False]
                     ]
      selectScalesT = GV.select "scalesT" GV.Interval [GV.BindScales]
      selectScalesB = GV.select "scalesB" GV.Interval [GV.BindScales]
      labelEncoding = GV.text [FV.tName @ml, GV.TmType GV.Nominal]
      pointSpec = GV.fromVL $ GV.toVegaLite [(GV.encoding . posEncodingB . pointEncoding) [], GV.mark GV.Point [], (GV.selection . selectScalesB) []]
      labelSpec = GV.fromVL $ GV.toVegaLite [(GV.encoding . posEncodingB . labelEncoding) [], GV.mark GV.Text []]      
      labeledPoints = GV.layer [pointSpec, labelSpec] -- layer labels on the points for detail plot
      onlyCentroid = GV.transform . GV.filter (GV.FEqual (FV.colName @ic) (GV.Boolean True)) . extraFilter
      selectCluster = GV.select "detail" GV.Single [GV.On "dblclick",GV.Fields $ FV.colNames @('[cid] V.++ as)]
      onlySelectedCluster = GV.transform . GV.filter (GV.FSelection "detail")
      topSpec = GV.fromVL $ GV.toVegaLite $
                [
                  GV.title (title <> " (Clustered)")
                , (GV.encoding . posEncodingT . pointEncoding) []
                , GV.mark GV.Point []
                , onlyCentroid []
                , (GV.selection . selectCluster . selectScalesT . extraSelection) []
                ]
      bottomSpec = GV.fromVL $ GV.toVegaLite $
                   [
                     GV.title (title <> " (cluster detail)")
                   , labeledPoints
                   , onlySelectedCluster []
                   ]
      configuration = GV.configure . GV.configuration (GV.View [GV.ViewWidth 800, GV.ViewHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
      vl = GV.toVegaLite $
        [ GV.description "Vega-lite"
        , GV.background "white"
        , GV.vConcat [topSpec, bottomSpec]
        , configuration []
        , (GV.transform . calcFields) []
        , dat]
  in vl

{-
-- leaving this here for reference
crimeRateVsMoneyBondRateVL' mergedOffenseAgainst dataRecords =
  let dat = FV.recordsToVLData (transformF @MoneyBondRate (*100) . transformF @CrimeRate (*100)) dataRecords
      posEncodingT = GV.position GV.X [FV.pName @MoneyBondRate, GV.PmType GV.Quantitative,
                                       GV.PAxis [GV.AxTitle "Money Bond Rate (%, All Crime Types)"]
                                      ]
                    . GV.position GV.Y [FV.pName @CrimeRate,
                                       GV.PmType GV.Quantitative,
                                       GV.PAxis [GV.AxTitle $ if mergedOffenseAgainst then "Crime Rate (%, All Crime Types)" else "Crime Rate (%)"]
                                      ]
      posEncodingB = GV.position GV.X [FV.pName @MoneyBondRate, GV.PmType GV.Quantitative
                                      , GV.PAxis [GV.AxTitle "Money Bond Rate (%, All Crime Types)"]
                                      , GV.PScale [GV.SZero False]
                                      ]
                    . GV.position GV.Y [FV.pName @CrimeRate,
                                       GV.PmType GV.Quantitative
                                       , GV.PAxis [GV.AxTitle $ if mergedOffenseAgainst then "Crime Rate (%, All Crime Types)" else "Crime Rate (%)"]
                                       , GV.PScale [GV.SZero False]
                                      ]                     
      pointEncoding = GV.color [FV.mName @Year, GV.MmType GV.Nominal]
                      . GV.size [FV.mName @EstPopulation, GV.MmType GV.Quantitative, GV.MLegend [GV.LTitle "population"]]
                      . if mergedOffenseAgainst then id else GV.row [FV.fName @CrimeAgainst, GV.FmType GV.Nominal,GV.FHeader [GV.HTitle "Crime Against"]]
                                                                                                                              
      labelEncoding = GV.text [FV.tName @KM.MarkLabel, GV.TmType GV.Nominal]
      ptSpec = GV.fromVL $ GV.toVegaLite [(GV.encoding . posEncodingB . pointEncoding) [], GV.mark GV.Point []]
      labelSpec = GV.fromVL $ GV.toVegaLite [(GV.encoding . posEncodingB . labelEncoding) [], GV.mark GV.Text []]      
      layers = GV.layer [ptSpec, labelSpec]
      transformCentroid = GV.transform . GV.filter (GV.FEqual (FV.colName @KM.IsCentroid) (GV.Boolean True))
      sizing = [GV.height 400, GV.width 600]
      selectDetail = GV.selection . GV.select "county_detail" GV.Interval []
      transformCounty = GV.transform . GV.filter (GV.FEqual (FV.colName @KM.IsCentroid) (GV.Boolean False)) . GV.filter (GV.FSelection "county_detail")
      mergedOffenseCol = if mergedOffenseAgainst then [] else [FV.colName @CrimeAgainst]
      selectCluster = GV.selection . GV.select "cluster_detail" GV.Single [GV.On "dblclick",GV.Fields ([FV.colName @KM.ClusterId, FV.colName @Year] <> mergedOffenseCol)]
      transformCluster = GV.transform . GV.filter (GV.FSelection "cluster_detail")
      topSpec = GV.fromVL $ GV.toVegaLite $
                [
                  GV.title ("Crime rate vs. Money Bond rate (Clustered)")
                , (GV.encoding . posEncodingT . pointEncoding) []
                , GV.mark GV.Point []
                , transformCentroid []
                , selectCluster []
                ] -- <> sizing 
      bottomSpec = GV.fromVL $ GV.toVegaLite $
                   [
                     GV.title ("Crime rate vs. Money Bond rate (Counties)")
                   , layers
                   , transformCluster []
                   ] -- <> sizing
      configuration = GV.configure . GV.configuration (GV.View [GV.ViewWidth 800, GV.ViewHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
      vl = GV.toVegaLite $
        [ GV.description "Vega-lite"
        , GV.background "white"
        , GV.vConcat [topSpec, bottomSpec]
        , configuration []
        , dat]
  in vl
-}
