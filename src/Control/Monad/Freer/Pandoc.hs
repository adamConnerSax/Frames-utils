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

data ToPandoc r where
  AddFrom  :: PandocReadFormat a -> P.ReaderOptions -> a -> ToPandoc () -- add to current doc

data FromPandoc r   
  WriteTo  :: PandocWriteFormat a -> P.WriterOptions -> P.Pandoc -> FromPandoc a -- convert to given format

addFrom :: FR.Member ToPandoc effs => PandocReadFormat a -> P.ReaderOptions -> a -> FR.Eff effs ()
addFrom prf pro doc = FR.send $ AddFrom prf pro doc

writeTo :: FR.Member FromPandoc effs => PandocWriterFormat a -> P.WriterOptions -> P.Pandoc -> FR.Eff effs a
writeTo pwf pwo pdoc = FR.send $ WriteTo pwf pwo

toPandoc :: P.PandocMonad m => PandocReadFormat a -> P.ReaderOptions -> a -> m P.Pandoc
toPandoc prf pro x = case prf of
  DocX -> P.readDocX pro x
  MarkDown -> P.readMarkDown pro x
  CommonMark -> P.readCommonMark pro x
  RST -> P.readRST pro x
  LaTeX -> P.readLaTeX pro x
  Html -> P.readHtml pro x
  

toWriter :: (P.PandocMonad m, FR.LastMember m effs)
  => FR.Eff (PandocReader ': effs) a -> FR.Eff (FR.Writer (P.Pandoc) ': effs) a
toWriter = FR.translate (\(AddFrom rf ro x) -> FR.Tell toPax)

type PanDocs = Docs (P.Pandoc)

newPandocPure :: FR.Member PanDocs effs => T.Text -> P.Pandoc -> FR.Eff effs ()
newPanDocPure = newDoc  

newPanDoc :: FR.Member PanDocs effs => T.Text -> FR.Eff (Pandoc ': effs) () -> FR.Eff effs ()
newPanDoc n l = (fmap snd $ FR.runWriter $ toWriter l) >>= newHtmlDocPure n 

htmlToNamedText :: FR.Eff (HtmlDocs ': effs) () -> FR.Eff effs [NamedDoc TL.Text]
htmlToNamedText = fmap (fmap (fmap H.renderText)) . toNamedDocList -- monad, list, NamedDoc itself
               
htmlToText :: FR.Eff (Html ': effs) () -> FR.Eff effs TL.Text
htmlToText = fmap (H.renderText . snd) . FR.runWriter . toWriter

