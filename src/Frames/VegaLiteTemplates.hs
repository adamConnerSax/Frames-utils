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
  ) where

import qualified Frames.VegaLite as FV
import           Math.Regression.LeastSquares (RegressionResult (..))

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


-- TODO: Add xErrors as well, in scatter and in fit
scatterWithFit :: forall x y ye fy fye. ( F.ColumnHeaders '[x]
                                        , F.ColumnHeaders '[y]
                                        , F.ColumnHeaders '[ye]
                                        , F.ColumnHeaders '[fy]
                                        , F.ColumnHeaders '[fye])
  => Text -> GV.Data -> GV.VegaLite
scatterWithFit title dat =
  let posEncodingS = GV.position GV.X [FV.pName @x, GV.PmType GV.Quantitative]
                   . GV.position GV.Y [FV.pName @y, GV.PmType GV.Quantitative]
      posEncodingSEB = posEncodingS . GV.position GV.YError [FV.pName @ye]
      posEncodingF = GV.position GV.X [FV.pName @x, GV.PmType GV.Quantitative]
                     . GV.position GV.Y [FV.pName @fy, GV.PmType GV.Quantitative]
      posEncodingFEB = posEncodingF . GV.position GV.YError [FV.pName @fye]
      selectScalesS = GV.select "scalesS" GV.Interval [GV.BindScales]
      selectScalesR = GV.select "scalesR" GV.Interval [GV.BindScales]
      scatterSpec = GV.fromVL $ GV.toVegaLite
        [
          (GV.encoding . posEncodingSEB) []
        , GV.mark GV.ErrorBar [GV.MType GV.Point]
        , (GV.selection . selectScalesS) []
        ]
      fitSpec = GV.fromVL $ GV.toVegaLite
        [
          (GV.encoding . posEncodingFEB) []
        , GV.mark GV.ErrorBand [GV.MType GV.Line]
        , (GV.selection . selectScalesR) []
        ]
      layers = GV.layer [scatterSpec, fitSpec]
      configuration = GV.configure
        . GV.configuration (GV.View [GV.ViewWidth 800, GV.ViewHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
      vl = GV.toVegaLite $ [layers, dat, configuration []]
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


