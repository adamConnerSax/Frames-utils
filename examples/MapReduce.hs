{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE TypeFamilies              #-}
module Main where

import qualified Control.Foldl                   as FL
import           Control.Monad.IO.Class           (MonadIO (..))
import qualified Data.List                       as List
--import           Data.Maybe                       (fromMaybe)
import qualified Data.Map                        as M
import qualified Data.Profunctor                 as P
import qualified Data.Text                       as T
import qualified Data.Text.IO                    as T
import qualified Data.Text.Lazy                  as TL
import qualified Data.Vector.Storable            as V
import qualified Data.Vector                     as VB
import qualified Data.Vinyl                      as V
import qualified Frames                          as F
import qualified Frames.InCore                   as FI

import qualified Numeric.LinearAlgebra           as LA
import           Numeric.LinearAlgebra            (R
                                                  , Matrix
                                                  )

import qualified Text.Blaze.Html.Renderer.Text   as BH
import qualified Text.Pandoc.Report              as P

import qualified Control.Monad.Freer.Logger      as Log
import qualified Control.Monad.Freer             as FR
import qualified Control.Monad.Freer.PandocMonad as FR
import qualified Control.Monad.Freer.Pandoc      as P

import qualified Frames.MapReduce as MR
--import qualified Control.MapReduce.Parallel      as MRP
import qualified Frames.Folds                    as FF
import qualified Frames.Transform                as FT
import           Frames.Table                    (blazeTable
                                                 , RecordColonnade
                                                 )

import           Data.String.Here

templateVars = M.fromList
  [
    ("lang", "English")
  , ("author", "Adam Conner-Sax")
  , ("pagetitle", "Map Reduce Examples")
--  , ("tufte","True")
  ]

-- this is annoying.  Where should this instance live?
type instance FI.VectorFor (Maybe a) = VB.Vector

mapReduceNotesMD
  = [here|
## Map Reduce Examples
|]

main :: IO ()
main = asPandoc
  
asPandoc :: IO ()
asPandoc = do
  let runAllP = FR.runPandocAndLoggingToIO Log.logAll
                . Log.wrapPrefix "Main"
                . fmap BH.renderHtml
  htmlAsTextE <- runAllP $ P.pandocWriterToBlazeDocument (Just "pandoc-templates/minWithVega-pandoc.html") templateVars P.mindocOptionsF $ do
    let rows :: Int = 20000
        vars = unweighted rows
        yNoise = 1.0
    Log.logLE Log.Info "Creating data"    
    frame <- liftIO (noisyData vars yNoise coeffs >>= makeFrame vars)    
    P.addMarkDown mapReduceNotesMD
    testMapReduce frame (showText rows <> " rows, average of X and Y by label.") mrAvgXYByLabel
    testMapReduce frame (showText rows <> " rows, filtered label, max of X and Y by label.") mrMaxXYByLabelABC
  case htmlAsTextE of
    Right htmlAsText -> T.writeFile "examples/html/mapReduce.html" $ TL.toStrict  $ htmlAsText
    Left err -> putStrLn $ "pandoc error: " ++ show err

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
assignToLabels = MR.assignKeys @'[Label]
assignDups = MR.assign @(F.Record '[IsDup]) (\r -> (T.length (F.rgetField @Label r) > 1) F.&: V.RNil) (F.rcast @[Y,X,Weight])


-- some reductions
--averageF :: FL.Fold (F.FrameRec '[X,Y]) F.Record '[X,Y]
averageF = FF.foldAllConstrained @RealFloat FL.mean

--maxX :: FL.Fold (F.Record '[X]) (F.Record '[ZM])
maxX = P.dimap (F.rgetField @X) (FT.recordSingleton @ZM) FL.maximum

--maxXY :: FL.Fold (F.Record '[X,Y]) (F.Record '[MX])
maxXY = P.dimap (\r -> Prelude.max (F.rgetField @X r) (F.rgetField @Y r)) (FT.recordSingleton @ZM) FL.maximum

-- put them together
--mrAvgXYByLabel :: FL.Fold (F.Record AllCols) (F.FrameRec AllCols)
mrAvgXYByLabel = MR.concatFold $ MR.mapReduceFold noUnpack (MR.splitOnKeys @'[Label]) (MR.foldAndAddKey averageF)

--mrAvgXYByLabelP :: FL.Fold (F.Record AllCols) (F.FrameRec AllCols)
--mrAvgXYByLabelP = MR.parBasicListHashableFold 1000 6 noUnpack (MR.splitOnKeys @'[Label]) (MR.foldAndAddKey averageF)
--  MR.MR.mapReduceGF (MRP.defaultParReduceGatherer pure) 

mrMaxXYByLabelABC :: FL.Fold (F.Record AllCols) (F.FrameRec '[Label,ZM])
mrMaxXYByLabelABC = MR.concatFold $ MR.mapReduceFold (filterLabel ["A","B","C"]) assignToLabels (MR.foldAndAddKey maxXY)

noisyData :: [Double] -> Double -> LA.Vector R -> IO (LA.Vector R, LA.Matrix R)
noisyData variances noiseObs coeffs = do
  -- generate random measurements
  let d = LA.size coeffs
      nObs = List.length variances
  let xs0 :: Matrix R = LA.asColumn $ LA.fromList $ [-0.5 + (realToFrac i/realToFrac nObs) | i <- [0..(nObs-1)]]
  let xsC = 1 LA.||| xs0
      ys0 = xsC LA.<> LA.asColumn coeffs
  yNoise <- fmap (List.head . LA.toColumns) (LA.randn nObs 1) -- 0 centered normally (sigma=1) distributed random numbers
  let ys = ys0 + LA.asColumn (LA.scale noiseObs (V.zipWith (*) yNoise (LA.cmap sqrt (LA.fromList variances))))
  return (List.head (LA.toColumns ys), xsC)

makeFrame :: [Double] -> (LA.Vector R, LA.Matrix R) -> IO (F.FrameRec AllCols)
makeFrame vars (ys, xs) = do
  if snd (LA.size xs) /= 2 then error ("Matrix of xs wrong size (" ++ show (LA.size xs) ++ ") for makeFrame") else return ()
  let rows = LA.size ys
      rowIndex = [0..(rows - 1)]
      makeLabel :: Int -> Char
      makeLabel n = toEnum (fromEnum 'A' + n `mod` 26)
      makeRecord :: Int -> F.Record AllCols
      makeRecord n = T.pack [makeLabel n] F.&: ys `LA.atIndex` n F.&: xs `LA.atIndex` (n,1) F.&: vars List.!! n F.&: V.RNil -- skip bias column
  return $ F.toFrame $ makeRecord <$> rowIndex

unweighted :: Int -> [Double]
unweighted n = List.replicate n (1.0)

coeffs :: LA.Vector R = LA.fromList [1.0, 2.2]

testMapReduce :: ( RecordColonnade as
                 , FR.Member P.ToPandoc effs
                 , Log.LogWithPrefixes effs
                 , FR.PandocEffects effs
                 , MonadIO (FR.Eff effs)
                 , Show (F.Record as))
              => F.FrameRec AllCols
              -> T.Text 
              -> FL.Fold (F.Record AllCols) (F.FrameRec as)
              -> FR.Eff effs ()
testMapReduce dataFrame title mrFold = do
  Log.logLE Log.Info $ "Doing map-reduce fold: " <> title    
  let resFrame = FL.fold mrFold dataFrame
      header _ _ = title
  P.addMarkDown $ "\n## " <> title 
  P.addBlaze $ blazeTable resFrame



showText :: Show a => a -> T.Text
showText = T.pack . show

