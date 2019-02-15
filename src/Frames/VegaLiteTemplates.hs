{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE ConstraintKinds #-}
module Frames.VegaLiteTemplates
  (
    clustersWithClickIntoVL
  , Frame2DRegressionScatterFit (..)
  , frameScatterWithFit
  , keyedLayeredFrameScatterWithFit
  , scatterWithFit
  , FitToPlot (..)
  , regressionCoefficientPlot
  , regressionCoefficientPlotMany
  ) where

import qualified Frames.VegaLite as FV
import qualified Frames.Transform as FT
import qualified Frames.Regression as FR
import qualified Math.Regression.Regression as RE 

import           Data.Maybe (fromMaybe)
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
import GHC.TypeLits (Symbol)
import Data.Kind (Constraint, Type)

import qualified Statistics.Types               as S


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

regressionCoefficientPlotMany :: Foldable f
                              => (k -> T.Text) -> T.Text -> [T.Text] -> f (k, RE.RegressionResult Double) -> Double -> GV.VegaLite
regressionCoefficientPlotMany = regressionCoefficientPlotFlex True 

regressionCoefficientPlotFlex :: Foldable f
                              => Bool -> (k -> Text) -> T.Text -> [T.Text] -> f (k, RE.RegressionResult Double) -> Double -> GV.VegaLite
regressionCoefficientPlotFlex haveLegend printKey title names results ci =
  let toRow m (RE.NamedEstimate n e eci epi) = [("Parameter",GV.Str (n <> T.replicate m "'")), ("Estimate",GV.Number e), ("Confidence",GV.Number eci)]
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
{-
type family WeightElemOf (rs :: [(Symbol, Type)]) (w :: (Symbol, Type)) :: Constraint where
  WeightElemOf _ FR.Unweighted = ()
  WeightElemOf rs w  = F.ElemOf rs w
-}

frameRegressionError :: forall y wc as w rs. FR.FrameRegressionResult y wc as w rs -> Error rs
frameRegressionError (FR.FrameUnweightedRegressionResult _) = const 0
frameRegressionError (FR.FrameWeightedRegressionResult wf _) = (\r -> 1/(wf $ F.rgetField @w r))


type ScatterFitConstraints x y = ( F.ColumnHeaders '[x]
                                 , F.ColumnHeaders '[y]
                                 , FV.ToVLDataValue (F.ElField x)
                                 , FV.ToVLDataValue (F.ElField y)
                                 , V.KnownField y
                                 , Real (V.Snd y)
                                 )

type ScatterFitC1 rs x y = ( F.ElemOf (rs V.++ [YError,YFit,YFitError]) x
                           , F.ElemOf (rs V.++ [YError,YFit,YFitError]) y
                           , F.ElemOf (rs V.++ [YError,YFit,YFitError]) YError
                           , F.ElemOf (rs V.++ [YError,YFit,YFitError]) YFit
                           , F.ElemOf (rs V.++ [YError,YFit,YFitError]) YFitError)
                           
class Frame2DRegressionScatterFit rs a where
  regressionResultToError :: a -> Error rs
  regressionResultToFit :: Maybe T.Text -> a -> Double -> FitToPlot rs
--  scatterPlot :: (Foldable f, Functor f) => T.Text -> Maybe T.Text -> a -> Double -> f (F.Record rs) -> GV.VegaLite
  scatterPlotSpec :: (Foldable f, Functor f) => Maybe T.Text -> a -> Double -> f (F.Record rs) -> GV.VLSpec

frameScatterWithFit :: (Frame2DRegressionScatterFit rs a, Foldable f, Functor f)
                    => T.Text -> Maybe T.Text -> a -> Double -> f (F.Record rs) -> GV.VegaLite
frameScatterWithFit title fitNameM frr ci frame =
  let configuration = GV.configure
        . GV.configuration (GV.View [GV.ViewWidth 800, GV.ViewHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
      swfSpec = GV.layer [scatterPlotSpec fitNameM frr ci frame]
  in GV.toVegaLite [configuration [], swfSpec, GV.title title]

keyedLayeredFrameScatterWithFit :: (Frame2DRegressionScatterFit rs a, Foldable f, Functor f, Foldable g, Functor g) 
  => T.Text -> (k -> T.Text) -> g (k, a) -> Double -> f (F.Record rs) -> GV.VegaLite
keyedLayeredFrameScatterWithFit title keyText keyedFits ci dat =
  let toSpec (k, a) = scatterPlotSpec (Just $ keyText k) a ci dat
      specs = FL.fold FL.list (fmap toSpec keyedFits)
      configuration = GV.configure
        . GV.configuration (GV.View [GV.ViewWidth 800, GV.ViewHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
  in GV.toVegaLite [configuration [], GV.layer specs, GV.title title]

instance ( V.KnownField x
         , Real (V.Snd x)
         , ScatterFitConstraints x y
         , ScatterFitC1 rs x y
         , F.ElemOf (rs V.++ [YError,YFit,YFitError]) x
         , F.ElemOf (rs V.++ [YError,YFit,YFitError]) y
         , F.ElemOf rs x
         , F.ElemOf rs y
         , V.KnownField w
         , rs F.⊆ rs
         ) => Frame2DRegressionScatterFit rs (FR.FrameRegressionResult y 'True '[x] w rs) where
  regressionResultToError = frameRegressionError
  regressionResultToFit fitNameM frr ci =
    let label = fromMaybe "fit" fitNameM
        dof = RE.degreesOfFreedom (FR.regressionResult frr)
        predF = RE.predictFromEstimateAtConfidence dof frr (S.mkCL ci)        
    in FitToPlot label predF

--  scatterPlot title fitNameM frr ci frame = 
--    scatterWithFit @x @y @rs @rs (regressionResultToError frr) (regressionResultToFit fitNameM frr ci) Nothing title frame
    
  scatterPlotSpec fitNameM frr ci frame = 
      scatterWithFitSpec @x @y @rs @rs (regressionResultToError frr) (regressionResultToFit fitNameM frr ci) Nothing frame


-- in this case we have y = a(x1) + b(x2) so we plot (y/x1) vs (a + b(x1/x2))
type YOverX1 = "y_over_x1" F.:-> Double
type X2OverX1 = "x2_over_x1" F.:-> Double

instance ( F.ElemOf rs x1
         , F.ElemOf rs x2
         , F.ElemOf rs y
         , V.KnownField y         
         , V.KnownField x1
         , V.KnownField x2
         , V.KnownField w
         , Real (V.Snd x1)
         , Real (V.Snd x2)
         , Real (V.Snd y)
         , F.ElemOf [x1,x2] x1
         , F.ElemOf [x1,x2] x2
         , F.ElemOf (rs V.++ [YOverX1, X2OverX1]) x1
         , F.ElemOf (rs V.++ [YOverX1, X2OverX1]) x2
         , F.ElemOf (rs V.++ [YOverX1, X2OverX1]) y
         , F.ElemOf (rs V.++ [YOverX1, X2OverX1]) X2OverX1
         , F.ElemOf (rs V.++ [YOverX1, X2OverX1]) YOverX1
         , rs F.⊆ (rs V.++  [YOverX1, X2OverX1])
         , ScatterFitC1 (rs V.++ [YOverX1, X2OverX1]) YOverX1 X2OverX1
         ) => Frame2DRegressionScatterFit rs (FR.FrameRegressionResult y 'False '[x1,x2] w rs) where
  regressionResultToError frr =
    let x1 = realToFrac . F.rgetField @x1
    in (\r -> frameRegressionError frr r/x1 r)
  
  regressionResultToFit fitNameM frr ci =
    let label = fromMaybe "fit" fitNameM
        dof = RE.degreesOfFreedom (FR.regressionResult frr)
        predF r =
          let x1 = realToFrac $ F.rgetField @x1 r
              (y,dy) = RE.predictFromEstimateAtConfidence dof frr (S.mkCL ci) r
          in (y/x1, dy/x1)
    in FitToPlot label predF    

  scatterPlotSpec fitNameM frr ci frame =
    let mut :: F.Record rs -> F.Record [YOverX1, X2OverX1]
        mut r =
          let y = F.rgetField @y r
              x1 = F.rgetField @x1 r
              x2 = F.rgetField @x2 r
          in (realToFrac y/realToFrac x1) F.&: (realToFrac x2/realToFrac x1) F.&: V.RNil
        mutData = fmap (FT.mutate mut) frame
        yName = FV.colName @y
        x1Name = FV.colName @x1
        x2Name = FV.colName @x2
        xLabel = x2Name <> "/" <> x1Name
        yLabel = yName <> "/" <> x1Name
    in scatterWithFitSpec @X2OverX1 @YOverX1 @(rs V.++ [YOverX1,X2OverX1]) @rs (regressionResultToError @rs frr) (regressionResultToFit @rs fitNameM frr ci) (Just (xLabel,yLabel)) mutData

{-    
frame2DRegressionScatter :: forall x y rs a f. (Foldable f, Frame2DRegressionScatterFit rs x y a)
  => T.Text -> Maybe T.Text -> a -> Double -> f (F.Record rs) -> GV.VegaLite    
frame2DRegressionScatter title fitNameM frr ci frame =
-}

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

scatterWithFit :: forall x y rs as f. ( ScatterFitConstraints x y
                                      , as F.⊆ rs
                                      , ScatterFitC1 rs x y
                                      , Foldable f)
               => Error as -> FitToPlot as -> Maybe (T.Text, T.Text) -> Text -> f (F.Record rs) -> GV.VegaLite
scatterWithFit err fit axisLabelsM title frame =
  let configuration = GV.configure
        . GV.configuration (GV.View [GV.ViewWidth 800, GV.ViewHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
      swfSpec = GV.specification $ scatterWithFitSpec @x @y @rs @as @f err fit axisLabelsM frame
  in GV.toVegaLite [configuration [], swfSpec, GV.title title]
  
scatterWithFitSpec :: forall x y rs as f. ( ScatterFitConstraints x y
                                          , as F.⊆ rs
                                          , ScatterFitC1 rs x y
                                          , Foldable f)
               => Error as -> FitToPlot as -> Maybe (T.Text, T.Text) -> f (F.Record rs) -> GV.VLSpec
scatterWithFitSpec err fit axisLabelsM frame =  
  let mut :: F.Record rs -> F.Record '[YError, YFit, YFitError]
      mut r =
        let a = F.rcast r
            (f,fe) = fitFunction fit a
        in err a F.&: f F.&: fe F.&: V.RNil
      vegaDat = FV.recordsToVLData (F.rcast @[x,y,YError,YFit,YFitError] . FT.mutate mut) frame
  in scatterWithFitSpec' @x @y @YError @YFit @YFitError axisLabelsM (fitLabel fit) vegaDat


scatterWithFit' :: forall x y ye fy fye. ( F.ColumnHeaders '[x]
                                         , F.ColumnHeaders '[y]
                                         , F.ColumnHeaders '[ye]
                                         , F.ColumnHeaders '[fy]
                                         , F.ColumnHeaders '[fye])
  => Maybe (T.Text, T.Text) -> Text -> Text -> GV.Data -> GV.VegaLite
scatterWithFit' axisLabelsM title fitLabel dat =
  let configuration = GV.configure
        . GV.configuration (GV.View [GV.ViewWidth 800, GV.ViewHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
      swfSpec = GV.specification $ scatterWithFitSpec' @x @y @ye @fy @fye axisLabelsM fitLabel dat
  in GV.toVegaLite [configuration [], swfSpec, GV.title title]

-- TODO: Add xErrors as well, in scatter and in fit
scatterWithFitSpec' :: forall x y ye fy fye. ( F.ColumnHeaders '[x]
                                             , F.ColumnHeaders '[y]
                                             , F.ColumnHeaders '[ye]
                                             , F.ColumnHeaders '[fy]
                                             , F.ColumnHeaders '[fye])
  => Maybe (T.Text, T.Text) -> Text -> GV.Data -> GV.VLSpec
scatterWithFitSpec' axisLabelsM fitLabel dat =
-- create 4 new cols so we can use rules/areas for errors
  let yLoCalc yName yErrName = "datum." <> yName <> " - (datum." <> yErrName <> ")"
      yHiCalc yName yErrName = "datum." <> yName <> " + (datum." <> yErrName <> ")"
      calcs = GV.calculateAs (yLoCalc (FV.colName @y) (FV.colName @ye)) "yLo"
              . GV.calculateAs (yHiCalc (FV.colName @y) (FV.colName @ye)) "yHi"
              . GV.calculateAs (yLoCalc (FV.colName @fy) (FV.colName @fye)) "fyLo"
              . GV.calculateAs (yHiCalc (FV.colName @fy) (FV.colName @fye)) "fyHi"
              . GV.calculateAs ("\"" <> fitLabel <> "\"")  "fitLabel"
      xLabel = fromMaybe (FV.colName @x) (fst <$> axisLabelsM)
      yLabel = fromMaybe (FV.colName @y) (snd <$> axisLabelsM)
      xEnc = GV.position GV.X [FV.pName @x, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle xLabel]]
      yEnc = GV.position GV.Y [FV.pName @y, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle yLabel]]
      yLEnc = GV.position GV.Y [GV.PName "yLo", GV.PmType GV.Quantitative]
      yHEnc = GV.position GV.Y2 [GV.PName "yHi", GV.PmType GV.Quantitative]
--      yErrorEnc = GV.position GV.YError [FV.pName @ye, GV.PmType GV.Quantitative]
      yFitEnc t = GV.position GV.Y [FV.pName @fy, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle t]]
      yFitLEnc = GV.position GV.Y [GV.PName "fyLo", GV.PmType GV.Quantitative]
      yFitHEnc = GV.position GV.Y2 [GV.PName "fyHi", GV.PmType GV.Quantitative]
      colorEnc = GV.color [GV.MName "fitLabel", GV.MmType GV.Nominal]
  --    yFitErrorEnc = GV.position GV.YError [FV.pName @fye, GV.PmType GV.Quantitative]
      scatterEnc = xEnc . yEnc 
      scatterBarEnc = xEnc . yLEnc . yHEnc
      fitEnc t = xEnc . yFitEnc t . colorEnc
      fitBandEnc = xEnc . yFitLEnc . yFitHEnc . colorEnc 
                  
      selectScalesS = GV.select ("scalesS" <> fitLabel) GV.Interval [GV.BindScales]
      selectScalesSE = GV.select("scalesSE" <> fitLabel) GV.Interval [GV.BindScales]
      selectScalesF = GV.select ("scalesF" <> fitLabel) GV.Interval [GV.BindScales]
      selectScalesFE = GV.select ("scalesFE" <> fitLabel) GV.Interval [GV.BindScales]
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
        , GV.mark GV.Area [GV.MOpacity 0.5, GV.MFillOpacity 0.3]
        , (GV.selection . selectScalesFE) []
        ]
      layers = GV.layer [ scatterSpec, scatterBarSpec, fitLineSpec, fitBandSpec]
      configuration = GV.configure
        . GV.configuration (GV.View [GV.ViewWidth 800, GV.ViewHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
      spec =
        GV.asSpec
        [
          (GV.transform . calcs) []
        , layers
        , dat
        ]
  in spec

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


