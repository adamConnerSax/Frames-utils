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
import qualified Frames                          as F

import           Data.List                        (intercalate)
import           Data.Text                        (Text)
import qualified Data.Text.IO                    as T
import qualified Data.Text.Lazy                  as TL
import qualified Text.Blaze.Html.Renderer.Text   as BH

import qualified Knit.Report           as K

{-
import qualified Knit.Effects.Logger           as Log
import qualified Knit.Effects.PandocMonad           as PM
import qualified Knit.Report.Pandoc           as RP
-}

import qualified Frames.Transform                as FT
import qualified Frames.MaybeUtils          as FM
import           Frames.Table                    (blazeTable
                                                 , RecordColonnade
                                                 )
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
  let pandocWriterConfig = K.PandocWriterConfig (Just "pandoc-templates/minWithVega-pandoc.html")  templateVars K.mindocOptionsF
  htmlAsTextE <- K.knitHtml (Just "Transforms.Main") K.logAll pandocWriterConfig $ do
    let exampleDataP :: F.MonadSafe m => P.Producer (FM.MaybeRow ExampleCols) m ()
        exampleDataP =  F.readTableMaybe "examples/SampleData.csv" -- create the Pipe
    exampleDataFrameM <- liftIO $ fmap F.boxedFrame $ F.runSafeEffect $ P.toListM $ exampleDataP 
    exampleDataFrame <- liftIO $ F.inCoreAoS $ exampleDataP P.>-> P.map F.recMaybe P.>-> P.concat  
    K.logLE K.Info $ "Raw:\n" <> (T.pack $ intercalate "\n" $ fmap show $ FL.fold FL.list exampleDataFrameM)    
    K.logLE K.Info $ "Raw after recMaybe:\n" <> (T.pack $ intercalate "\n" $ fmap show $ FL.fold FL.list exampleDataFrame)
    
    let fieldDefaults = FT.FieldDefaults (Just False) (Just (-1)) Nothing (Just "N/A")
    exampleDataFrameM' <- liftIO $ fmap F.boxedFrame $ F.runSafeEffect $ P.toListM $ exampleDataP P.>-> P.map (FT.defaultRecord fieldDefaults)
    exampleDataFrame' <- liftIO $ F.inCoreAoS $ exampleDataP P.>-> P.map (F.recMaybe . FT.defaultRecord fieldDefaults) P.>-> P.concat 
    K.logLE K.Info $ "Defaulting all but Double:\n" <> (T.pack $ intercalate "\n" $ fmap show $ FL.fold FL.list exampleDataFrameM')
    K.logLE K.Info $ "Defaulted after recMaybe:\n" <> (T.pack $ intercalate "\n" $ fmap show $ FL.fold FL.list exampleDataFrame')
  case htmlAsTextE of
    Right htmlAsText -> T.writeFile "examples/html/transformations.html" $ TL.toStrict  $ htmlAsText
    Left err -> putStrLn $ "pandoc error: " ++ show err

{-
Output:

Main [Informational] Raw:
                     {Just Name :-> "", Just Age :-> 22, Just Height :-> 1.5, Just HasChildren :-> False}
                     {Just Name :-> "Jim", Nothing, Just Height :-> 1.7, Just HasChildren :-> True}
                     {Just Name :-> "Boris", Just Age :-> 33, Nothing, Just HasChildren :-> False}
                     {Just Name :-> "Natasha", Just Age :-> 36, Just Height :-> 1.7, Nothing}
Main [Informational] Raw after recMaybe:
                     {Name :-> "", Age :-> 22, Height :-> 1.5, HasChildren :-> False}
Main [Informational] Defaulting all but Double:
                     {Just Name :-> "", Just Age :-> 22, Just Height :-> 1.5, Just HasChildren :-> False}
                     {Just Name :-> "Jim", Just Age :-> -1, Just Height :-> 1.7, Just HasChildren :-> True}
                     {Just Name :-> "Boris", Just Age :-> 33, Nothing, Just HasChildren :-> False}
                     {Just Name :-> "Natasha", Just Age :-> 36, Just Height :-> 1.7, Just HasChildren :-> False}
Main [Informational] Defaulted after recMaybe:
                     {Name :-> "", Age :-> 22, Height :-> 1.5, HasChildren :-> False}
                     {Name :-> "Jim", Age :-> -1, Height :-> 1.7, HasChildren :-> True}
                     {Name :-> "Natasha", Age :-> 36, Height :-> 1.7, HasChildren :-> False}
Main [Warning] IO Error (ignored): Couldn't find "pandoc-templates/minWithVega-pandoc.html"

...done
-}
