{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE ConstraintKinds           #-}
module Bench.MapReduce
  ( benchMapReduceIO
  )
where

import           Criterion
import           Control.DeepSeq                ( NFData(rnf) )

import qualified Frames.MapReduce              as MR
import qualified Control.MapReduce.Parallel    as MRP
import qualified Frames.Folds                  as FF
import qualified Frames.Transform              as FT
import           Frames.Table                   ( blazeTable
                                                , RecordColonnade
                                                )

import qualified Control.Foldl                 as FL
import qualified Data.List                     as List
import qualified Data.Profunctor               as P
import qualified Data.Text                     as T
import qualified Data.Sequence                 as Seq
import qualified Data.Vinyl                    as V
import qualified Data.Vector.Storable          as V
import qualified Frames                        as F

import qualified Numeric.LinearAlgebra         as LA
import           Numeric.LinearAlgebra          ( R
                                                , Matrix
                                                )
import           Control.Concurrent             ( getNumCapabilities )



createData :: Int -> IO (F.FrameRec AllCols)
createData rows = do
  putStrLn $ "Creating " ++ show rows ++ " rows of data."
  let vars   = unweighted rows
      yNoise = 1.0
      unweighted n = List.replicate n (1.0)
  noisyData vars yNoise (LA.fromList [1.0, 2.2]) >>= makeFrame vars

benchAvgXYByLabel :: Int -> F.FrameRec AllCols -> Benchmark
benchAvgXYByLabel numThreadsToUse dat = bgroup
  (show $ FL.fold FL.length dat)
  [ bench "serial (seq -> strict map)"
    $ nf (FL.fold $ mrAvgXYByLabel (MR.gathererSeqToStrictMap pure)) dat
  , bench "serial (seq -> strict hash map)"
    $ nf (FL.fold $ mrAvgXYByLabel (MR.gathererSeqToStrictHashMap pure)) dat
  , bench "parallel reduce (sequence -> strict hash map)"
    $ nf (FL.fold $ mrAvgXYByLabel (MRP.parReduceGathererHashableS pure)) dat
  , bench "parallel reduce (sequence -> lazy hash map)"
    $ nf (FL.fold $ mrAvgXYByLabel (MRP.parReduceGathererHashableL pure)) dat
  , bench "parallel map/reduce (seq -> strict hash map)"
  $ nf
      (FL.fold $ mrAvgXYByLabelPM numThreadsToUse
                                  (MRP.parReduceGathererHashableS pure)
      )
  $ dat
  , bench "parallel map/reduce (seq -> lazy hash map)"
  $ nf (FL.fold $ mrAvgXYByLabelPMS numThreadsToUse)
  $ dat
  ]

benchMapReduceIO :: IO Benchmark
benchMapReduceIO = do
  putStrLn "Creating data for benchmarks to use..."
  dat <- mapM createData [5000, 20000, 50000, 100000]
  nc  <- getNumCapabilities
  let threadsToUse = maximum (1, nc - 2) -- leave some for gc, if possible
  putStrLn
    $  "Runtime reports "
    ++ show nc
    ++ " capabilities (threads that can truly run simultaneously).  Using "
    ++ show threadsToUse
    ++ " (leaving some for GC, if possible)."
  return $ bgroup
    "MapReduce"
    [bgroup "AvgXYByLabel" $ fmap (benchAvgXYByLabel threadsToUse) dat]


type Label = "label" F.:-> T.Text
type Y = "y" F.:-> Double
type X = "x" F.:-> Double
type ZM = "zMaybe" F.:-> (Maybe Double)
type Weight = "weight" F.:-> Double
type IsDup = "is_duplicate" F.:-> Bool
type AllCols = [Label,Y,X,Weight]


-- let's make some map-reductions on Frames of AllCols

-- First some unpackings
noUnpack = MR.noUnpack
filterLabel ls = MR.filterUnpack (\r -> (F.rgetField @Label r) `List.elem` ls)
filterMinX minX = MR.filterUnpack ((>= minX) . F.rgetField @X)
editLabel f r = F.rputField @Label (f (F.rgetField @Label r)) r -- this would be better with lenses!!
unpackDup = MR.Unpack $ \r -> [r, editLabel (<> "2") r]

-- some assignings
assignToLabels = MR.assignKeysAndData @'[Label] @'[Y, X, Weight]
assignDups = MR.assign @(F.Record '[IsDup])
  (\r -> (T.length (F.rgetField @Label r) > 1) F.&: V.RNil)
  (F.rcast @'[Y, X, Weight])

-- some gatherings
--gatherLists = MR.gatherRecordList
--gatherFrames = MR.gatherRecordFrame

-- some reductions
--averageF :: FL.Fold (F.FrameRec '[X,Y]) F.Record '[X,Y]
averageF = FF.foldAllConstrained @RealFloat FL.mean

--maxX :: FL.Fold (F.Record '[X]) (F.Record '[ZM])
maxX = P.dimap (F.rgetField @X) (FT.recordSingleton @ZM) FL.maximum

--maxXY :: FL.Fold (F.Record '[X,Y]) (F.Record '[MX])
maxXY = P.dimap (\r -> Prelude.max (F.rgetField @X r) (F.rgetField @Y r))
                (FT.recordSingleton @ZM)
                FL.maximum

-- put them together
--mrAvgXYByLabel :: MR.GroupMap -> FL.Fold (F.Record AllCols) (F.FrameRec AllCols)
mrAvgXYByLabel gm =
  MR.mapReduceGF gm noUnpack assignToLabels (MR.foldAndAddKey averageF)

-- use the simple version
mrAvgXYByLabelPMS n = MR.parBasicListHashableFold 1000
                      n
                      noUnpack
                      assignToLabels
                      (MR.foldAndAddKey averageF)

-- construct it directly so we can compare different gatherer implementations
mrAvgXYByLabelPM n gm =
  let (MR.MapGather _ mapStep) =
        MR.uagMapAllGatherEachFold gm noUnpack assignToLabels
  in  MRP.parallelMapReduceF @[] 1000 n gm mapStep (MR.foldAndAddKey averageF)

{-
mrAvgXYByLabelStrict :: FL.Fold (F.Record AllCols) (F.FrameRec AllCols)
mrAvgXYByLabelStrict = MR.mapReduceFrame MR.groupMapStrict
                                   noUnpack
                                   assignToLabels
                                   (MR.foldAndAddKey averageF)


mrAvgXYByLabelP :: FL.Fold (F.Record AllCols) (F.FrameRec AllCols)
mrAvgXYByLabelP = MR.mapReduceFrame MRP.parReduceGroupMap
                                    noUnpack
                                    assignToLabels
                                    (MR.foldAndAddKey averageF)

mrAvgXYByLabelP2 :: FL.Fold (F.Record AllCols) (F.FrameRec AllCols)
mrAvgXYByLabelP2 = MR.mapReduceFrame
  (MRP.parAllGroupMap $ MRP.parFoldMonoidDC 100)
  noUnpack
  assignToLabels
  (MR.foldAndAddKey averageF)
-}


noisyData :: [Double] -> Double -> LA.Vector R -> IO (LA.Vector R, LA.Matrix R)
noisyData variances noiseObs coeffs = do
  -- generate random measurements
  let d    = LA.size coeffs
      nObs = List.length variances
--  xs0 <- LA.cmap (\x-> x - 0.5) <$> LA.rand nObs (d-1) -- 0 centered uniformly distributed random numbers
  let xs0 :: Matrix R =
        LA.asColumn
          $ LA.fromList
          $ [ -0.5 + (realToFrac i / realToFrac nObs) | i <- [0 .. (nObs - 1)] ]
--  xNoise <- LA.randn nObs (d-1) -- 0 centered normally (sigma=1) distributed random numbers
  let xsC = 1 LA.||| xs0
      ys0 = xsC LA.<> LA.asColumn coeffs
  yNoise <- fmap (List.head . LA.toColumns) (LA.randn nObs 1) -- 0 centered normally (sigma=1) distributed random numbers
  let ys = ys0 + LA.asColumn
        (LA.scale
          noiseObs
          (V.zipWith (*) yNoise (LA.cmap sqrt (LA.fromList variances)))
        )
  return (List.head (LA.toColumns ys), xsC)

makeFrame :: [Double] -> (LA.Vector R, LA.Matrix R) -> IO (F.FrameRec AllCols)
makeFrame vars (ys, xs) = do
  if snd (LA.size xs) /= 2
    then error
      ("Matrix of xs wrong size (" ++ show (LA.size xs) ++ ") for makeFrame")
    else return ()
  let rows     = LA.size ys
      rowIndex = [0 .. (rows - 1)]
      makeLabel :: Int -> Char
      makeLabel n = toEnum (fromEnum 'A' + n `mod` 26)
      makeRecord :: Int -> F.Record AllCols
      makeRecord n =
        T.pack [makeLabel n]
          F.&:         ys
          `LA.atIndex` n
          F.&:         xs
          `LA.atIndex` (n, 1)
          F.&:         vars
          List.!!      n
          F.&:         V.RNil -- skip bias column
  return $ F.toFrame $ makeRecord <$> rowIndex


