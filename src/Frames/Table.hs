{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE UndecidableInstances #-}
module Frames.Table where

import qualified Colonnade            as C
import qualified Lucid.Colonnade      as LC
import qualified Text.Blaze.Colonnade as BC
import qualified Lucid                as LH
import qualified Text.Blaze.Html      as BH
import           Data.Profunctor      (lmap)
import qualified Data.Text            as T
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames               as F
import qualified Frames.Melt          as F
import           GHC.TypeLits (symbolVal)

recordFieldToColonnadeG :: forall x rs b. (V.KnownField x, F.ElemOf rs x) => (V.Snd x -> b) -> (String -> b) -> C.Colonnade C.Headed (F.Record rs) b
recordFieldToColonnadeG valTo headerTo = C.headed (headerTo $ symbolVal (Proxy @(V.Fst x))) (valTo . F.rgetField @x)

recordFieldToColonnade :: forall x rs. (V.KnownField x, F.ElemOf rs x, Show (V.Snd x)) => C.Colonnade C.Headed (F.Record rs) T.Text
recordFieldToColonnade = recordFieldToColonnadeG @x @rs (T.pack . show) T.pack

class RecordColonnade rs where
  recColonnade :: C.Colonnade C.Headed (F.Record rs) T.Text

instance RecordColonnade '[] where
  recColonnade = mempty

-- I do not like all the rcasting here!  Maybe there is a type family solution that is cleaner??
instance (V.KnownField r, rs F.⊆ (r : rs), Show (V.Snd r), RecordColonnade rs) => RecordColonnade (r : rs) where
  recColonnade = recordFieldToColonnade @r @(r : rs) <> (lmap (F.rcast @rs) (recColonnade @rs))

textTable :: (RecordColonnade rs, Foldable f) => f (F.Record rs) -> T.Text
textTable = T.pack . C.ascii (fmap T.unpack recColonnade)

lucidTable :: (RecordColonnade rs, Foldable f) => f (F.Record rs) -> LH.Html ()
lucidTable = LC.encodeCellTable [] (fmap LC.textCell recColonnade)  

blazeTable :: (RecordColonnade rs, Foldable f) => f (F.Record rs) -> BH.Html
blazeTable = BC.encodeCellTable mempty (fmap BC.textCell recColonnade)  









