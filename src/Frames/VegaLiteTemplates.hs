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
  , scatterWithFit
  , scatterWithFit'
  , FitToPlot (..)
  ) where

import qualified Frames.VegaLite as FV
import qualified Frames.Transform as FT
import           Math.Regression.Regression (RegressionResult (..))

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


type YError = "yError" F.:-> Double
type YFit = "yFit" F.:-> Double
type YFitError = "yFitError" F.:->Double

type Error rs = (F.Record rs -> Double)
data FitToPlot rs = FitToPlot { fitLabel :: T.Text, fitFunction :: F.Record rs -> (Double, Double) }

scatterWithFit :: forall x y f rs. ( F.ColumnHeaders '[x]
                                   , F.ColumnHeaders '[y]
                                   , FV.ToVLDataValue (F.ElField x)
                                   , FV.ToVLDataValue (F.ElField y)
                                   , F.ElemOf rs x
                                   , F.ElemOf rs y
                                   , [x,y,YError,YFit,YFitError]  F.⊆ (rs V.++ '[YError,YFit,YFitError]) 
                                   , Foldable f)
               => Error rs -> FitToPlot rs -> Text -> f (F.Record rs) -> GV.VegaLite
scatterWithFit err fit title frame =
  let mut :: F.Record rs -> F.Record '[YError, YFit, YFitError]
      mut r = let (f,fe) = fitFunction fit r in err r F.&: f F.&: fe F.&: V.RNil
      vegaDat = FV.recordsToVLData (F.rcast @[x,y,YError,YFit,YFitError] . FT.mutate mut) frame
  in scatterWithFit' @x @y @YError @YFit @YFitError title (fitLabel fit) vegaDat


-- TODO: Add xErrors as well, in scatter and in fit
scatterWithFit' :: forall x y ye fy fye. ( F.ColumnHeaders '[x]
                                         , F.ColumnHeaders '[y]
                                         , F.ColumnHeaders '[ye]
                                         , F.ColumnHeaders '[fy]
                                         , F.ColumnHeaders '[fye])
  => Text -> Text -> GV.Data -> GV.VegaLite
scatterWithFit' title fitLabel dat =
-- create 4 new cols so we can use rules/areas for errors
  let yLoCalc yName yErrName = "datum." <> yName <> " - (datum." <> yErrName <> "/2)"
      yHiCalc yName yErrName = "datum." <> yName <> " + (datum." <> yErrName <> "/2)"
      calcHiLos = GV.calculateAs (yLoCalc (FV.colName @y) (FV.colName @ye)) "yLo"
                  . GV.calculateAs (yHiCalc (FV.colName @y) (FV.colName @ye)) "yHi"
                  . GV.calculateAs (yLoCalc (FV.colName @fy) (FV.colName @fye)) "fyLo"
                  . GV.calculateAs (yHiCalc (FV.colName @fy) (FV.colName @fye)) "fyHi"

      xEnc = GV.position GV.X [FV.pName @x, GV.PmType GV.Quantitative]
      yEnc = GV.position GV.Y [FV.pName @y, GV.PmType GV.Quantitative]
      yLEnc = GV.position GV.Y [GV.PName "yLo", GV.PmType GV.Quantitative]
      yHEnc = GV.position GV.Y2 [GV.PName "yHi", GV.PmType GV.Quantitative]
--      yErrorEnc = GV.position GV.YError [FV.pName @ye, GV.PmType GV.Quantitative]
      yFitEnc t = GV.position GV.Y [FV.pName @fy, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle t]]
      yFitLEnc = GV.position GV.Y [GV.PName "fyLo", GV.PmType GV.Quantitative]
      yFitHEnc = GV.position GV.Y2 [GV.PName "fyHi", GV.PmType GV.Quantitative]
  --    yFitErrorEnc = GV.position GV.YError [FV.pName @fye, GV.PmType GV.Quantitative]
      scatterEnc = xEnc . yEnc
      scatterBarEnc = xEnc . yLEnc . yHEnc 
      fitEnc t = xEnc . yFitEnc t
      fitBandEnc = xEnc . yFitLEnc . yFitHEnc 
                  
      selectScalesS = GV.select "scalesS" GV.Interval [GV.BindScales]
      selectScalesSE = GV.select "scalesSE" GV.Interval [GV.BindScales]
      selectScalesF = GV.select "scalesF" GV.Interval [GV.BindScales]
      selectScalesFE = GV.select "scalesFE" GV.Interval [GV.BindScales]
      scatterSpec = GV.fromVL $ GV.toVegaLite
        [
          (GV.encoding . scatterEnc) []
        , GV.mark GV.Point []
        , (GV.selection . selectScalesS) []
        ]
      scatterBarSpec = GV.fromVL $ GV.toVegaLite
        [
          (GV.encoding . scatterBarEnc) []
        , GV.mark GV.Rule []
        , (GV.selection . selectScalesSE) []
        ]
      fitLineSpec = GV.fromVL $ GV.toVegaLite
        [
          (GV.encoding . fitEnc fitLabel) []
        , GV.mark GV.Line []
        , (GV.selection . selectScalesF) []
        ]        
      fitBandSpec = GV.fromVL $ GV.toVegaLite
        [
          (GV.encoding . fitBandEnc) []
        , GV.mark GV.Area [GV.MOpacity 0.5]
        , (GV.selection . selectScalesFE) []
        ]
      layers = GV.layer [ scatterSpec, scatterBarSpec, fitLineSpec, fitBandSpec]
      configuration = GV.configure
        . GV.configuration (GV.View [GV.ViewWidth 800, GV.ViewHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
      vl =
        GV.toVegaLite
        [
          GV.title title 
        , (GV.transform . calcHiLos) []
        , layers
        , dat
        , configuration []]
  in vl

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
      configuration = GV.configure
        . GV.configuration (GV.View [GV.ViewWidth 800, GV.ViewHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
--        . GV.configuration (GV.Scale [GV.SCMinSize 100])
      vl = GV.toVegaLite $
        [ GV.description "Vega-lite"
        , GV.background "white"
        , GV.vConcat [topSpec, bottomSpec]
        , configuration []
        , (GV.transform . calcFields) []
        , dat]
  in vl


