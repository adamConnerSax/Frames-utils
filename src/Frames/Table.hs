{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
module Frames.Table where

import qualified Colonnade       as C
import           Data.Monoid     (mempty, mconcat, (<>))
import           Data.Profunctor (lmap)
import           Data.Proxy      (Proxy (..))
import qualified Data.Text       as T
import qualified Data.Vinyl      as V
import qualified Data.Vinyl.TypeLevel      as V
import qualified Frames          as F
import qualified Frames.Melt     as F
import           GHC.TypeLits    (KnownSymbol, symbolVal)


recordFieldToColonnadeG :: forall x rs b. (V.KnownField x, F.ElemOf rs x) => (V.Snd x -> b) -> (String -> b) -> C.Colonnade C.Headed (F.Record rs) b
recordFieldToColonnadeG valTo headerTo = C.headed (headerTo $ symbolVal (Proxy @(V.Fst x))) (valTo . F.rgetField @x)

recordFieldToColonnade :: forall x rs. (V.KnownField x, F.ElemOf rs x, Show (V.Snd x)) => C.Colonnade C.Headed (F.Record rs) T.Text
recordFieldToColonnade = recordFieldToColonnadeG @x @rs (T.pack . show) T.pack

--frameColToColonnade :: forall x rs. (F.ElemOf rs x) => F.Record rs -> C.Colonnade C.Headed (F.Record rs) T.Text
--frameColToColonnade = lmap (F.rgetField @x) . elFieldToColonnade . F.rcast @'[x]

class RecordColonnade rs where
  recColonnade :: C.Colonnade C.Headed (F.Record rs) T.Text

instance RecordColonnade '[] where
  recColonnade = mempty

-- I do not like all the rcasting here!  Maybe there is a type family solution that is cleaner??
instance (V.KnownField r, rs F.⊆ (r : rs), Show (V.Snd r), RecordColonnade rs) => RecordColonnade (r : rs) where
  recColonnade = recordFieldToColonnade @r @(r : rs) <> (lmap (F.rcast @rs) (recColonnade @rs))


textTable :: (RecordColonnade rs, Foldable f) => f (F.Record rs) -> T.Text
textTable = T.pack . C.ascii (fmap T.unpack recColonnade)

{-
recordToColonnade :: ( V.RFoldMap rs
                     , F.AllConstrained V.KnownField rs
                     {-     , V.ReifyConstraint Show F.ElField rs-})
                    => F.Record rs -> C.Colonnade C.Headed (F.Record rs) T.Text
recordToColonnade = V.rfoldMap (const recordFieldToColonnade)
-}








