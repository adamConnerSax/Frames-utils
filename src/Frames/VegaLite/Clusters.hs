{-# LANGUAGE CPP                   #-}
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
module Frames.VegaLite.Clusters
  (
    clustersWithClickIntoVL
  ) where

import qualified Frames.VegaLite.Utils as FV

import           Data.Text              (Text)
import qualified Data.Vinyl.TypeLevel   as V
import qualified Frames                 as F
import qualified Graphics.Vega.VegaLite as GV


#if MIN_VERSION_hvega(0,4,0)
gvTitle :: Text -> GV.PropertySpec
gvTitle x = GV.title x []
#else
gvTitle :: Text -> (GV.VLProperty, GV.VLSpec)
gvTitle = GV.title
#endif


-- TODO: A nicer interface to all this

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
                        => Text
                        -> Text
                        -> Text
                        -> GV.BuildEncodingSpecs
                        -> GV.BuildTransformSpecs
                        -> GV.BuildSelectSpecs
                        -> GV.BuildTransformSpecs
                        -> GV.Data
                        -> GV.VegaLite
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
                  gvTitle (title <> " (Clustered)")
                , (GV.encoding . posEncodingT . pointEncoding) []
                , GV.mark GV.Point []
                , onlyCentroid []
                , (GV.selection . selectCluster . selectScalesT . extraSelection) []
                ]
      bottomSpec = GV.asSpec
                   [
                     gvTitle (title <> " (cluster detail)")
                   , labeledPoints
                   , onlySelectedCluster []
                   ]
      configuration = GV.configure
        . GV.configuration (GV.View [GV.ViewContinuousWidth 800, GV.ViewContinuousHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
--        . GV.configuration (GV.Scale [GV.SCMinSize 100])
      vl = GV.toVegaLite $
        [ GV.description "Vega-lite"
        , GV.background "white"
        , GV.vConcat [topSpec, bottomSpec]
        , configuration []
        , (GV.transform . calcFields) []
        , dat]
  in vl


