{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
module Frames.ScatterMerge
  (
    BinsWithRescale (..)
  , Binnable 
  , binField
  , scatterMerge
  , scatterMergeOne
  , scatterMerge'
  , buildScatterMerge
  ) where

import qualified Frames.MapReduce   as MR
import qualified Frames.Utils        as FU
import qualified Math.Rescale as MR

import qualified Control.Foldl        as FL
import qualified Data.List            as List
import qualified Data.Map             as M
import           Data.Maybe           (fromMaybe)
import qualified Data.Text            as T
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Curry     as V
import qualified Data.Vinyl.TypeLevel as V
import           Data.Vinyl.Lens      (type (∈))
import           Data.Profunctor      as PF
import           Frames               ((&:))
import qualified Frames               as F
import qualified Frames.ShowCSV           as F
import qualified Frames.InCore        as FI
import qualified Frames.Melt          as F
import qualified Data.Vector as V


type Binnable x w =  (V.KnownField x, Real (V.Snd x),
                      V.KnownField w, Real (V.Snd w))

-- NB: Bins are unscaled; scaling function to be applied after binning  
data BinsWithRescale a = BinsWithRescale { bins :: [a],  shift :: a, scale :: Double} -- left in a form where a need not be a type that supports division 
                     
binField :: forall x w. Binnable x w => Int -> MR.RescaleType (V.Snd x) -> FL.Fold (F.Record '[x,w]) (BinsWithRescale (V.Snd x))
binField numBins rt =
  let process :: [(V.Snd x, V.Snd w)] -> F.Record '[x,w] -> [(V.Snd x, V.Snd w)]  
      process l r  = V.runcurryX (\x w -> (x, w) : l) r
      extract :: MR.RescaleType (V.Snd x) -> [(V.Snd x, V.Snd w)] -> BinsWithRescale (V.Snd x)
      extract rt' l =
        let compFst (x,_) (x',_) = compare x x'
            scaleInfo = FL.fold (MR.rescale rt') (fst <$> l)
            (totalWeight, listWithSummedWeights) = List.mapAccumL (\sw (x,w) -> (sw+w, (x,w,sw+w))) 0 $ List.sortBy compFst l
            weightPerBin :: Double = (realToFrac totalWeight)/(fromIntegral numBins)
            lowerBounds :: [(b, V.Snd w, V.Snd w)] -> [b] -> [b]
            lowerBounds x bs = case List.null x of
              True -> bs
              False ->
                let nextLB = (\(a,_,_) -> a) . head $ x
                    newX = List.dropWhile (\(_,_,sw) -> (realToFrac sw) < weightPerBin * (fromIntegral $ List.length bs + 1)) x
                    newBS = bs ++ [nextLB]
                in lowerBounds newX newBS
        in BinsWithRescale (lowerBounds listWithSummedWeights []) (fst scaleInfo) (snd scaleInfo)
  in FL.Fold process [] (extract rt)

type BinnableKeyedRecord rs ks x w = (F.AllConstrained (FU.CFieldOf Real [x,w]) '[x, w], ks F.⊆ rs, Ord (F.Record ks))
  
binFields :: forall ks x w rs. (BinnableKeyedRecord rs ks x w, '[x,w] F.⊆ rs)
           => Int -> MR.RescaleType (V.Snd x) -> FL.Fold (F.Record rs) (M.Map (F.Record ks) (BinsWithRescale (V.Snd x)))
binFields n rt = MR.mapRListFOrd MR.noUnpack (MR.assignKeysAndData @ks @[x,w]) (MR.Reduce $ \k xw -> M.singleton k $ FL.fold (binField n rt) xw)

-- NB: a can't be less than the 0th element because we build it that way.  So we drop it
sortedListToBinLookup' :: Ord a => [a] -> a -> Int
sortedListToBinLookup' as a = let xs = tail as in 1 + (fromMaybe (List.length xs) $ List.findIndex (>a) xs)

scatterMerge :: forall ks x y w rs. ( F.AllConstrained (FU.CFieldOf Real [x,y,w]) '[x, y, w]
                                    , [x,y,w] F.⊆ rs
                                    , Ord (F.Record ks)
                                    , ks F.⊆ rs
                                    , FI.RecVec (ks V.++ [x,y,w]))
              => (Double -> V.Snd x) -- when we put the averaged data back in the record with original types we need to convert back
              -> (Double -> V.Snd y)
              -> Int
              -> Int
              -> MR.RescaleType (V.Snd x)
              -> MR.RescaleType (V.Snd y)
              -> FL.Fold (F.Record rs) (F.FrameRec (ks V.++ [x, y, w]))
scatterMerge toX toY numBinsX numBinsY rtX rtY = 
  let doOne = scatterMergeOne numBinsX numBinsY rtX rtY
      toRecord :: (Double, Double, V.Snd w) -> F.Record [x,y,w]
      toRecord (x', y', w') = toX x' &: toY y' &: w' &: V.RNil
  in MR.mapRListFOrd MR.noUnpack (MR.assignKeysAndData @ks @[x,y,w]) (MR.makeRecsWithKey toRecord $ MR.Reduce $ \_ xws -> doOne xws) 

  
scatterMergeOne :: forall x y w f. (F.AllConstrained (FU.CFieldOf Real [x,y,w]) '[x, y, w], Foldable f)
                => Int
                -> Int
                -> MR.RescaleType (V.Snd x)
                -> MR.RescaleType (V.Snd y)
                -> f (F.Record '[x,y,w])
                -> [(Double, Double, V.Snd w)]
scatterMergeOne numBinsX numBinsY rtX rtY dataRows =
  let xBinF = PF.lmap (F.rcast @[x,w]) $ binField numBinsX rtX
      yBinF = PF.lmap (F.rcast @[y,w]) $ binField numBinsY rtY
      (xBins, yBins) = FL.fold ((,) <$> xBinF <*> yBinF) dataRows
      binningInfo (BinsWithRescale bs shft scle) = (sortedListToBinLookup' bs, (\x -> realToFrac (x - shft)/scle))
      (binX, scaleX) = binningInfo xBins
      (binY, scaleY) = binningInfo yBins
      binAndScale :: F.Record '[x,y,w] -> ((Int, Int), Double, Double, V.Snd w)
      binAndScale = V.runcurryX (\x y w -> ((binX x, binY y),scaleX x, scaleY y, w))
      getKey (k,_,_,_) = k
      getData (_,x,y,w) = (x,y,w)
      wgtdSumF :: FL.Fold (Double, Double, V.Snd w) (Double, Double, V.Snd w)
      wgtdSumF =
        let f (wX, wY, totW) (x, y, w) = let w' = realToFrac w in (wX + w' * x, wY + w' * y, totW + w)
        in FL.Fold f (0, 0 , 0) (\(wX, wY, totW) -> let tw = realToFrac totW in (wX/tw, wY/tw, totW))
      mapRFold = MR.basicListF @Ord (MR.simpleUnpack binAndScale) (MR.assign getKey getData) (MR.Reduce $ \_ xyws -> [FL.fold wgtdSumF xyws])
  in FL.fold mapRFold dataRows


-- All unused below but might be useful to have around.
data Bin2DT = Bin2D (Int, Int) deriving (Show, Eq, Ord)

F.declareColumn "Bin2D" ''Bin2DT

type instance FI.VectorFor Bin2DT = V.Vector
instance F.ShowCSV Bin2DT where
  showCSV = T.pack . show

type DblX = "double_x" F.:-> Double
type DblY = "double_y" F.:-> Double
  


type OutKeyCols ks = ks V.++ '[Bin2D]
type BinnedDblCols ks w = ks V.++ '[Bin2D, DblX, DblY, w]
type BinnedDblColsC ks w = (Bin2D ∈ BinnedDblCols ks w, DblX ∈ BinnedDblCols ks w, DblY ∈ BinnedDblCols ks w, w ∈ BinnedDblCols ks w)
type BinnedResultCols ks x y w = ks V.++ '[Bin2D, x, y, w]
type UseCols ks x y w = ks V.++ [x, y, w]
type UseColsC ks x y w = (ks F.⊆ UseCols ks x y w, x ∈ UseCols ks x y w, y ∈ UseCols ks x y w, w ∈ UseCols ks x y w)

type ScatterMergeable' rs ks x y w = (ks F.⊆ rs,
                                      Ord (F.Record ks),
                                      FI.RecVec (BinnedResultCols ks x y w),
                                      F.AllConstrained (FU.RealFieldOf rs) '[x, y, w],
                                      BinnedDblColsC ks w,
                                      UseCols ks x y w F.⊆ rs, UseColsC ks x y w,
                                      OutKeyCols ks F.⊆ BinnedDblCols ks w,
                                      Ord (F.Record (OutKeyCols ks)),
                                      UseCols ks x y w F.⊆ BinnedResultCols ks x y w,
                                      ((OutKeyCols ks) V.++ '[x,y,w]) ~ (BinnedResultCols ks x y w))

scatterMerge' :: forall ks x y w rs. ( ScatterMergeable' rs ks x y w
                                     , F.ElemOf [Bin2D, DblX, DblY, w] w
                                     , ks F.⊆ (BinnedDblCols ks w)
                                     , FI.RecVec (UseCols ks x y w)
                                     )
              
             => (Double -> V.Snd x) -- when we put the averaged data back in the record with original types we need to convert back
             -> (Double -> V.Snd y)
             -> M.Map (F.Record ks) (BinsWithRescale (V.Snd x))
             -> M.Map (F.Record ks) (BinsWithRescale (V.Snd y))
             -> FL.Fold (F.Record rs) (F.FrameRec (UseCols ks x y w))
scatterMerge' toX toY xBins yBins =
  let binningInfo :: Real c => BinsWithRescale c -> (c -> Int, c -> Double)
      binningInfo (BinsWithRescale bs shft scle) = (sortedListToBinLookup' bs, (\x -> realToFrac (x - shft)/scle))
      xBinF = binningInfo <$> xBins
      yBinF = binningInfo <$> yBins
      binRow :: F.Record (UseCols ks x y w) -> F.Record (BinnedDblCols ks w) -- 'ks ++ [Bin2D,X,Y,w]
      binRow r =
        let key = F.rcast @ks r
            xyw = F.rcast @[x,y,w] r
            (xBF, xSF) = fromMaybe (const 0, realToFrac) $ M.lookup key xBinF
            (yBF, ySF) = fromMaybe (const 0, realToFrac) $ M.lookup key yBinF
            binnedAndScaled :: F.Record '[Bin2D, DblX, DblY, w] = V.runcurryX (\x y w -> Bin2D (xBF x, yBF y) &: xSF x &: ySF y &: w &: V.RNil) xyw
        in key V.<+> binnedAndScaled 
      wgtdSum :: (Double, Double, V.Snd w) -> F.Record [DblX, DblY, w] -> (Double, Double, V.Snd w)
      wgtdSum (wX, wY, totW) r = V.runcurryX (\x y w -> let w' = realToFrac w in (wX + (w' * x), wY + (w' * y), totW + w)) r
      makeXYW :: (V.Snd x, V.Snd y, V.Snd w) -> F.Record [x,y,w]
      makeXYW (x,y,w) =  x &: y &: w &: V.RNil
      extractF :: FL.Fold (F.Record [Bin2D,DblX, DblY, w]) ([(V.Snd x, V.Snd y, V.Snd w)])
      extractF = MR.basicListF @Ord MR.noUnpack (MR.splitOnKeys @'[Bin2D])
        (MR.ReduceFold $ const $ FL.Fold wgtdSum (0,0,0) (\(wX, wY, totW) -> let totW' = realToFrac totW in [(toX (wX/totW'), toY (wY/totW'), totW)]))
  in MR.mapRListFOrd
     (MR.simpleUnpack $ binRow . F.rcast @(UseCols ks x y w))
     (MR.assignKeysAndData @ks @[Bin2D,DblX,DblY,w])
     (MR.makeRecsWithKey makeXYW $ MR.ReduceFold $ const $ extractF)

type BinMap ks x = M.Map (F.Record ks) (BinsWithRescale (V.Snd x))

buildScatterMerge :: forall ks x y w rs. ( BinnableKeyedRecord rs ks x w
                                         , BinnableKeyedRecord rs ks y w
                                         , ScatterMergeable' rs ks x y w
                                         , F.ElemOf [Bin2D, DblX, DblY, w] w
                                         , ks F.⊆ (BinnedDblCols ks w)
                                         , FI.RecVec (UseCols ks x y w)
                                         )
                  => Int
                  -> Int
                  -> MR.RescaleType (V.Snd x)
                  -> MR.RescaleType (V.Snd y)
                  -> (Double -> V.Snd x)
                  -> (Double -> V.Snd y)
                  -> (FL.Fold (F.Record rs) (BinMap ks x, BinMap ks y),
                      (BinMap ks x, BinMap ks y) -> FL.Fold (F.Record rs) (F.FrameRec (UseCols ks x y w)))
buildScatterMerge xNumBins yNumBins rtX rtY toX toY =
  let binXFold = binFields @ks @x @w xNumBins  rtX
      binYFold = binFields @ks @y @w yNumBins  rtY
      smFold (xBins, yBins) = scatterMerge' @ks @x @y @w toX toY xBins yBins
  in ((,) <$> binXFold <*> binYFold, smFold)
                      
  
