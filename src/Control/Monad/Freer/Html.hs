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
module Control.Monad.Freer.Html
  (
    Html
  , HtmlDocs
  , NamedDoc(..)
  , html
  , newHtmlDoc
  , htmlToNamedText
  , htmlToText
  ) where

import qualified Lucid                       as H
import qualified Data.Text.Lazy              as TL
import qualified Data.Text                   as T
import qualified Control.Monad.Freer         as FR
import qualified Control.Monad.Freer.Writer  as FR
import           Control.Monad.Freer.Docs    (Docs, NamedDoc(..), newDoc, toNamedDocList)

-- For now, just handle the Html () case since then it's monoidal and we can interpret via writer
--newtype FreerHtml = FreerHtml { unFreer :: H.Html () }

data Html r where
  Html   :: H.Html () -> Html () -- add to current doc
  
html :: FR.Member Html effs => H.Html () -> FR.Eff effs ()
html = FR.send . Html

toWriter :: FR.Eff (Html ': effs) a -> FR.Eff (FR.Writer (H.Html ()) ': effs) a
toWriter = FR.translate (\(Html x) -> FR.Tell x)

type HtmlDocs = Docs (H.Html ())

newHtmlDocPure :: FR.Member HtmlDocs effs => T.Text -> H.Html () -> FR.Eff effs ()
newHtmlDocPure = newDoc  

newHtmlDoc :: FR.Member HtmlDocs effs => T.Text -> FR.Eff (Html ': effs) () -> FR.Eff effs ()
newHtmlDoc n l = (fmap snd $ FR.runWriter $ toWriter l) >>= newHtmlDocPure n 

htmlToNamedText :: FR.Eff (HtmlDocs ': effs) () -> FR.Eff effs [NamedDoc TL.Text]
htmlToNamedText = fmap (fmap (fmap H.renderText)) . toNamedDocList -- monad, list, NamedDoc itself
               
htmlToText :: FR.Eff (Html ': effs) () -> FR.Eff effs TL.Text
htmlToText = fmap (H.renderText . snd) . FR.runWriter . toWriter

