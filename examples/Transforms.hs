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
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE RankNTypes #-}
module Main where

import qualified Control.Foldl                   as FL
import           Control.Monad.IO.Class           (MonadIO (..))
import           Data.Map as M
import qualified Data.Text                       as T
import qualified Data.Vinyl                      as V
import qualified Frames                          as F

import           Data.List                        (intercalate, concat)
import           Data.Maybe                       (catMaybes)
import           Data.Text                        (Text)
import qualified Data.Text.IO                    as T
import qualified Data.Text.Lazy                  as TL
import qualified Text.Blaze.Html.Renderer.Text   as BH
import qualified Text.Pandoc.Report              as P

import qualified Control.Monad.Freer.Logger      as Log
import qualified Control.Monad.Freer             as FR
import qualified Control.Monad.Freer.PandocMonad as FR
import qualified Control.Monad.Freer.Pandoc      as P

import qualified Frames.Transform                as FT
import qualified Frames.MaybeUtils          as FM
import           Frames.Table                    (blazeTable
                                                 , RecordColonnade
                                                 )
import qualified Frames.TH                      as F
import qualified Pipes                      as P
import qualified Pipes.Prelude              as P

import           Data.String.Here

F.tableTypes "ExampleCols" "examples/SampleData.csv"

templateVars = M.fromList
  [
    ("lang", "English")
  , ("author", "Adam Conner-Sax")
  , ("pagetitle", "Transforms Examples")
--  , ("tufte","True")
  ]

transformNotesMD
  = [here|
## Transform Examples
|]

main :: IO ()
main = asPandoc

asPandoc :: IO ()
asPandoc = do
  let runAllP = FR.runPandocAndLoggingToIO Log.logAll
                . Log.wrapPrefix "Main"
                . fmap BH.renderHtml
  htmlAsTextE <- runAllP $ P.pandocWriterToBlazeDocument (Just "pandoc-templates/minWithVega-pandoc.html") templateVars P.mindocOptionsF $ do
    let exampleDataP :: F.MonadSafe m => P.Producer (FM.MaybeRow ExampleCols) m ()
        exampleDataP =  F.readTableMaybe "examples/SampleData.csv"
    exampleDataFrameM <- liftIO $ fmap F.boxedFrame $ F.runSafeEffect $ P.toListM $ exampleDataP
    exampleDataFrame <- liftIO $ F.inCoreAoS $ exampleDataP P.>-> P.map F.recMaybe P.>-> P.concat 
    Log.logLE Log.Info $ "Raw:\n" <> (T.pack $ intercalate "\n" $ fmap show $ FL.fold FL.list exampleDataFrameM)    
    Log.logLE Log.Info $ "Raw after recMaybe:\n" <> (T.pack $ intercalate "\n" $ fmap show $ FL.fold FL.list exampleDataFrame)
    let fieldDefaults = FT.FieldDefaults (Just False) (Just (-1)) Nothing (Just "N/A")
    exampleDataFrameM' <- liftIO $ fmap F.boxedFrame $ F.runSafeEffect $ P.toListM $ exampleDataP P.>-> P.map (FT.defaultRecord fieldDefaults)
    exampleDataFrame' <- liftIO $ F.inCoreAoS $ exampleDataP P.>-> P.map (F.recMaybe . FT.defaultRecord fieldDefaults) P.>-> P.concat 
    Log.logLE Log.Info $ "Defaulting all but Double:\n" <> (T.pack $ intercalate "\n" $ fmap show $ FL.fold FL.list exampleDataFrameM')
    Log.logLE Log.Info $ "Defaulted after recMaybe:\n" <> (T.pack $ intercalate "\n" $ fmap show $ FL.fold FL.list exampleDataFrame')
  case htmlAsTextE of
    Right htmlAsText -> T.writeFile "examples/html/transformations.html" $ TL.toStrict  $ htmlAsText
    Left err -> putStrLn $ "pandoc error: " ++ show err
