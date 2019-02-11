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
  , FitToPlot (..)
  , regressionCoefficientPlot
  , regressionCoefficientPlotMany
  ) where

import qualified Frames.VegaLite as FV
import qualified Frames.Transform as FT
import qualified Math.Regression.Regression as RE 

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
import Text.Printf (printf)


-- | Plot regression coefficents with error bars
-- | Flex version handles a foldable of results so we can, e.g., 
-- | 1. Compare across time or other variable in the data
-- | 2. Compare different fitting methods for the same data

-- TODO: Fix replicated y-axis ticks.  There since we need the difference between them (trailing "'"s) to provide y-offset
-- to the different results.  Otherwise they would overlap in y.  Maybe fix by switching to layers and only having Y-axis labels
-- on 0th?

-- TODO: Make this typed?  Using a regression result with typed parameters?  Power-to-weight? 

regressionCoefficientPlot :: T.Text -> [T.Text] -> RE.RegressionResult Double -> Double -> GV.VegaLite
regressionCoefficientPlot title names r ci = regressionCoefficientPlotFlex False id title names (V.Identity ("",r)) ci

regressionCoefficientPlotMany :: forall k f. ( Show k
                                              , Foldable f)
                              => (k -> T.Text) -> T.Text -> [T.Text] -> f (k, RE.RegressionResult Double) -> Double -> GV.VegaLite
regressionCoefficientPlotMany = regressionCoefficientPlotFlex True 

regressionCoefficientPlotFlex :: Foldable f
                              => Bool -> (k -> Text) -> T.Text -> [T.Text] -> f (k, RE.RegressionResult Double) -> Double -> GV.VegaLite
regressionCoefficientPlotFlex haveLegend printKey title names results ci =
  let toRow m (RE.NamedEstimate n e eci) = [("Parameter",GV.Str (n <> T.replicate m "'")), ("Estimate",GV.Number e), ("Confidence",GV.Number eci)]
      addKey k l = ("Key", GV.Str $ printKey k) : l
      dataRowFold = FL.Fold (\(l,n) (k,regRes) -> (l ++ fmap (flip GV.dataRow [] . addKey k . toRow n) (RE.namedEstimates names regRes ci), n+1)) ([],0) (GV.dataFromRows [] . List.concat . fst)
      dat = FL.fold dataRowFold results
      xLabel = "Estimate (with " <> (T.pack $ printf "%2.0f" (100 * ci)) <> "% confidence error bars)"
      estimateXEnc = GV.position GV.X [GV.PName "Estimate", GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle xLabel]]
      estimateYEnc = GV.position GV.Y [GV.PName "Parameter", GV.PmType GV.Ordinal]
      handleLegend l = if haveLegend then l else (GV.MLegend []) : l
      estimateColorEnc = GV.color $ handleLegend $ [GV.MName "Key", GV.MmType GV.Nominal]
      estimateEnc = estimateXEnc . estimateYEnc . estimateColorEnc
      estLoCalc = "datum.Estimate - datum.Confidence/2"
      estHiCalc = "datum.Estimate + datum.Confidence/2"
      calcEstConf = GV.calculateAs estLoCalc "estLo" . GV.calculateAs estHiCalc "estHi"
      estConfLoEnc = GV.position GV.X [GV.PName "estLo", GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle ""]]
      estConfHiEnc = GV.position GV.X2 [GV.PName "estHi", GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle ""]]
      estConfEnc = estConfLoEnc . estConfHiEnc . estimateYEnc . estimateColorEnc
      estSpec = GV.asSpec [(GV.encoding . estimateEnc) [], GV.mark GV.Point []]
      confSpec = GV.asSpec [(GV.encoding . estConfEnc) [], GV.mark GV.Rule []]
      configuration = GV.configure
        . GV.configuration (GV.View [GV.ViewWidth 800, GV.ViewHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
      vl = GV.toVegaLite
        [
          GV.title title
        , (GV.transform . calcEstConf) []
        , GV.layer [estSpec, confSpec]
        , dat
        , configuration []
        ]
  in vl 

--
type YError = "yError" F.:-> Double
type YFit = "yFit" F.:-> Double
type YFitError = "yFitError" F.:->Double

type Error rs = (F.Record rs -> Double)
data FitToPlot rs = FitToPlot { fitLabel :: T.Text, fitFunction :: F.Record rs -> (Double, Double) }

-- | 2D Scatter of Data with calculated error and fit function.  
-- | Use TypeApplications to specify x and y columns for scatter and then provide calculated error and fit.
-- | Since both calculations use the record itself as the domain, you can put the error and fit into the record and just use
-- | field selection.  But this allows more flexibility and doesn't require adding things to the input frame record just for the plot.

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
      scatterSpec = GV.asSpec
        [
          (GV.encoding . scatterEnc) []
        , GV.mark GV.Point []
        , (GV.selection . selectScalesS) []
        ]
      scatterBarSpec = GV.asSpec
        [
          (GV.encoding . scatterBarEnc) []
        , GV.mark GV.Rule []
        , (GV.selection . selectScalesSE) []
        ]
      fitLineSpec = GV.asSpec
        [
          (GV.encoding . fitEnc fitLabel) []
        , GV.mark GV.Line []
        , (GV.selection . selectScalesF) []
        ]        
      fitBandSpec = GV.asSpec
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

-- | Plot a view of clustered data
-- | Use TypeApplications to specify:
-- | 'x': Number. x position of the cluster or underlying data point
-- | 'y': Number. y position of the cluster or underlying data point
-- | 'ic': Boolean. True if the point is a cluster, False if the point is underlying data
-- | 'cid': Int. specifying cluster membership for underlying data points.  Set to 0 for clusters.
-- | 'ml': Text.  Label for the underlying data point on the single cluster view. 
-- | 'as': Remaining key fields for cluster, so that multiple clustered sets may be overlaid.
  
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
      pointSpec = GV.asSpec [(GV.encoding . posEncodingB . pointEncoding) [], GV.mark GV.Point [], (GV.selection . selectScalesB) []]
      labelSpec = GV.asSpec [(GV.encoding . posEncodingB . labelEncoding) [], GV.mark GV.Text []]      
      labeledPoints = GV.layer [pointSpec, labelSpec] -- layer labels on the points for detail plot
      onlyCentroid = GV.transform . GV.filter (GV.FEqual (FV.colName @ic) (GV.Boolean True)) . extraFilter
      selectCluster = GV.select "detail" GV.Single [GV.On "dblclick",GV.Fields $ FV.colNames @('[cid] V.++ as)]
      onlySelectedCluster = GV.transform . GV.filter (GV.FSelection "detail")
      topSpec = GV.asSpec
                [
                  GV.title (title <> " (Clustered)")
                , (GV.encoding . posEncodingT . pointEncoding) []
                , GV.mark GV.Point []
                , onlyCentroid []
                , (GV.selection . selectCluster . selectScalesT . extraSelection) []
                ]
      bottomSpec = GV.asSpec
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


