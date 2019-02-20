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
module Control.Monad.Freer.Docs
  (
    Docs
  , NamedDoc(..)
  , newDoc
  , toNamedDocList
  ) where

import qualified Lucid                       as H
import qualified Data.Text.Lazy              as TL
import qualified Data.Text                   as T
import qualified Control.Monad.Freer         as FR
import qualified Control.Monad.Freer.State  as FR

-- small effect for gathering up named documents into a list in order to handle output at one place

data Docs a r where
  NewDoc :: T.Text -> a -> Docs a ()
  
newDoc :: FR.Member (Docs a) effs => T.Text -> a -> FR.Eff effs ()
newDoc name doc = FR.send $ NewDoc name doc

-- interpret in State

data NamedDoc a = NamedDoc { ndName :: T.Text, ndDoc :: a } deriving (Functor)
{-
data MultiDocState a = MultiDocState { mdsCurrentName :: T.Text, mdsDocs :: [NamedDoc a] }

mdsNewDoc :: T.Text -> a -> MultiDocState a -> MultiDocState a
mdsNewDoc n d (MultiDocState cn docList) = MultiDocState n ((NamedDoc cn d) : docList)
-}

toState :: FR.Eff ((Docs a) ': effs) () -> FR.Eff (FR.State [NamedDoc a]  ': effs) ()
toState = FR.reinterpret f where
  f :: FR.Member (FR.State [NamedDoc a]) effs => Docs a x -> FR.Eff effs x
  f r = case r of
    NewDoc n d -> FR.modify (\l -> (NamedDoc n d) : l)

toNamedDocList :: FR.Eff ((Docs a) ': effs) () -> FR.Eff effs [NamedDoc a]
toNamedDocList = FR.execState [] . toState
      

