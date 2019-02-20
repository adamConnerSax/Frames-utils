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
module Control.Monad.Freer.Html where

--import           Control.Monad.IO.Class (MonadIO (..))
import qualified Lucid                  as H
import qualified Data.Text.Lazy              as T
import qualified Control.Monad.Freer    as FR
{-
import qualified Data.List              as List
import           Data.Monoid            ((<>))

import qualified Pipes                  as P

import qualified Control.Monad.Freer.State   as FR
import qualified Control.Monad.Freer.Writer   as FR
-}


data Html r where
  Html :: H.Html a -> Html a 

html :: FR.Member Html effs => H.Html a -> FR.Eff effs a
html = FR.send . Html

htmlToText :: FR.Eff (Html ': effs) a -> FR.Eff effs T.Text
htmlToText = fmap H.renderText . FR.interpret (\(Html x) -> pure x)

toTextHelper :: FR.Eff (Html ': effs) a -> FR.Eff (Html ': effs) T.Text
toTextHelper l = l >> (html $ pure "") -- add a pure text value so that we have the right type for FR.interpret  
