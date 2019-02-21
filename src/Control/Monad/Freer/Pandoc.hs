{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE DeriveFunctor       #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Control.Monad.Freer.Pandoc
  (
    Pandoc
  , PanDocs
  , NamedDoc(..)
  , pandoc
  , newPandoc
  , pandocToNamedHtmlText
  , pandocToHtmlText
  ) where

import qualified Text.Pandoc                 as P
--import qualified Data.Text.Lazy              as TL
import qualified Data.Text                   as T
import           Data.ByteString             (ByteString)
import           Text.Blaze.Html             (Html)
import qualified Control.Monad.Freer         as FR
import qualified Control.Monad.Freer.Writer  as FR
import           Control.Monad.Freer.Docs    (Docs, NamedDoc(..), newDoc, toNamedDocList)

-- For now, just handle the Html () case since then it's monoidal and we can interpret via writer
--newtype FreerHtml = FreerHtml { unFreer :: H.Html () }

data PandocReadFormat a where
  DocX :: PandocReadFormat ByteString
  MarkDown :: PandocReadFormat T.Text
  CommonMark :: PandocReadFormat T.Text
  RST :: PandocReadFormat T.Text
  LaTeX :: PandocReadFormat T.Text
  Html :: PandocReadFormat T.Text deriving (Show)

data PandocWriteFormat a where
  DocX :: PandocWriteFormat ByteString
  MarkDown :: T.Text
  CommonMark :: T.Text
  RST :: T.Text
  LaTeX :: T.Text
  Html5 :: Html
  Html5String :: T.Text deriving (Show)

data Pandoc r where
  AddFrom  :: PandocReadFormat a -> P.ReaderOptions -> a -> Pandoc () -- add to current doc
  WriteTo :: PandocWriteFormat a -> P.WriterOptions -> Pandoc a -- convert current doc to given format

addFrom :: FR.Member Pandoc effs => PandocReadFormat a -> P.ReaderOptions -> a -> FR.Eff effs ()
addFrom prf pro doc = FR.send $ AddFrom prf pro doc

writeTo :: FR.Member Pandoc effs => PandocWriterFormat a -> P.WriterOptions -> FR.Eff effs a
writeTo pwf pwo = FR.send $ WriteTo pwf pwo

toWriter :: FR.Eff (Pandoc ': effs) a -> FR.Eff (FR.Writer (P.Pandoc) ': effs) a
toWriter = FR.translate (\(Html x) -> FR.Tell x)

type PanDocs = Docs (P.Pandoc)

newPandocPure :: FR.Member PanDocs effs => T.Text -> P.Pandoc  -> FR.Eff effs ()
newPanDocPure = newDoc  

newPanDoc :: FR.Member PanDocs effs => T.Text -> FR.Eff (Pandoc ': effs) () -> FR.Eff effs ()
newPanDoc n l = (fmap snd $ FR.runWriter $ toWriter l) >>= newHtmlDocPure n 

htmlToNamedText :: FR.Eff (HtmlDocs ': effs) () -> FR.Eff effs [NamedDoc TL.Text]
htmlToNamedText = fmap (fmap (fmap H.renderText)) . toNamedDocList -- monad, list, NamedDoc itself
               
htmlToText :: FR.Eff (Html ': effs) () -> FR.Eff effs TL.Text
htmlToText = fmap (H.renderText . snd) . FR.runWriter . toWriter

