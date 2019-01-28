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
                        => Text -> Text -> Text -> GV.BuildLabelledSpecs -> GV.Data -> GV.VegaLite
clustersWithClickIntoVL xAxisTitle yAxisTitle title pointEncoding dat =
  let posEncodingT = GV.position GV.X
        [
          FV.pName @x
        , GV.PmType GV.Quantitative
        , GV.PAxis [GV.AxTitle xAxisTitle]
        ]
        . GV.position GV.Y
        [
          FV.pName @y
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
                                                                                                                              
      labelEncoding = GV.text [FV.tName @ml, GV.TmType GV.Nominal]
      ptSpec = GV.fromVL $ GV.toVegaLite [(GV.encoding . posEncodingB . pointEncoding) [], GV.mark GV.Point []]
      labelSpec = GV.fromVL $ GV.toVegaLite [(GV.encoding . posEncodingB . labelEncoding) [], GV.mark GV.Text []]      
      layers = GV.layer [ptSpec, labelSpec]
      transformCentroid = GV.transform . GV.filter (GV.FEqual (FV.colName @ic) (GV.Boolean True))
      selectCluster = GV.selection . GV.select "detail" GV.Single [GV.On "dblclick",GV.Fields $ FV.colNames @('[cid] V.++ as)]
      transformCluster = GV.transform . GV.filter (GV.FSelection "detail")
      topSpec = GV.fromVL $ GV.toVegaLite $
                [
                  GV.title (title <> " (Clustered)")
                , (GV.encoding . posEncodingT . pointEncoding) []
                , GV.mark GV.Point []
                , transformCentroid []
                , selectCluster []
                ]
      bottomSpec = GV.fromVL $ GV.toVegaLite $
                   [
                     GV.title (title <> " (cluster detail)")
                   , layers
                   , transformCluster []
                   ]
      configuration = GV.configure . GV.configuration (GV.View [GV.ViewWidth 800, GV.ViewHeight 400]) . GV.configuration (GV.Padding $ GV.PSize 50)
      vl = GV.toVegaLite $
        [ GV.description "Vega-lite"
        , GV.background "white"
        , GV.vConcat [topSpec, bottomSpec]
        , configuration []
        , dat]
  in vl
