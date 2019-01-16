{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE PolyKinds            #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Frames.MaybeUtils where

import           Frames                  ((:.), (&:))
import qualified Frames                  as F
import qualified Frames.CSV              as F
import qualified Frames.ShowCSV          as F
import qualified Frames.Melt as F
import           Control.Monad (join)
import qualified Pipes                   as P
import qualified Data.Vinyl              as V
import qualified Data.Vinyl.Derived      as V
import qualified Data.Vinyl.TypeLevel      as V
import qualified Data.Vinyl.Class.Method as V
import qualified Data.Vinyl.Core         as V
import qualified Data.Vinyl.XRec         as V
import           Frames.InCore (RecVec)
import           Data.Vinyl.Curry        (runcurryX)
import qualified Data.Vinyl.Functor      as V
import           Data.Proxy              (Proxy (..))
import           Data.Maybe (fromMaybe, fromJust)
import           Data.Discrimination (Grouping)
import           GHC.TypeLits (KnownSymbol)

-- TBD: This should be an argument to produceCSV_Maybe and writeCSV_Maybe.
instance F.ShowCSV a => F.ShowCSV (Maybe a) where
  showCSV = fromMaybe "NA" . fmap F.showCSV

type AddMaybe rs = V.MapTyCon Maybe rs

unMaybeKeys :: forall ks rs as.(ks F.⊆ rs, as ~ F.RDeleteAll ks rs, as F.⊆ rs, V.RPureConstrained V.KnownField as, V.RecApplicative as, V.RApply as, V.RMap as)
  => Proxy ks -> F.Rec (Maybe F.:. F.ElField) rs -> F.Record (ks V.++ (AddMaybe as))
unMaybeKeys _ mr =
  let keys = fromJust $ F.recMaybe $ F.rcast @ks mr
      remainder = V.rsequenceInFields $ F.rcast @as mr
 in keys `V.rappend` remainder

joinMaybeOne :: KnownSymbol s => (Maybe :. V.ElField) '(s, Maybe a) -> (Maybe :. V.ElField) '(s,a)
joinMaybeOne = V.Compose . fmap V.Field . join . fmap V.getField . V.getCompose

class JoinMaybe rs where
  joinMaybe :: V.Rec (Maybe V.:. F.ElField) (AddMaybe rs) -> V.Rec (Maybe V.:. F.ElField) rs

instance JoinMaybe '[] where
  joinMaybe V.RNil = V.RNil

instance (KnownSymbol s, r ~ '(s, t), JoinMaybe rs) => JoinMaybe (r ': rs) where
  joinMaybe (r V.:& rs) = joinMaybeOne r V.:& joinMaybe rs


leftJoinMaybe :: forall fs rs rs2  rs2' rs'.
                 ( fs    F.⊆ rs
                 , fs   F.⊆ rs2
                 , rs2' ~ F.RDeleteAll fs rs2
                 , rs' ~ F.RDeleteAll fs rs
                 , Grouping (F.Record fs)
                 , JoinMaybe (rs' V.++ rs2') 
                 , V.RPureConstrained V.KnownField rs', V.RApply rs', V.RMap rs', V.RecApplicative rs'
                 , V.RPureConstrained V.KnownField rs2', V.RApply rs2', V.RMap rs2', V.RecApplicative rs2'
                 , V.RMap (fs V.++ AddMaybe rs')
                 , V.RMap ((fs V.++ AddMaybe rs') V.++ F.RDeleteAll fs (fs V.++ AddMaybe rs2'))
                 , V.RecApplicative (F.RDeleteAll fs (fs V.++ AddMaybe rs2'))
                 , RecVec (fs V.++ AddMaybe rs')
                 , RecVec (F.RDeleteAll fs (fs V.++ AddMaybe rs2'))
                 , RecVec ((fs V.++ AddMaybe rs') V.++ F.RDeleteAll fs (fs V.++ AddMaybe rs2'))
                 , fs F.⊆ (fs V.++ AddMaybe rs')
                 , fs F.⊆ (fs V.++ AddMaybe rs2')
                 , fs F.⊆ ((fs V.++ AddMaybe rs') V.++ F.RDeleteAll fs (fs V.++ AddMaybe rs2'))
                 , AddMaybe (rs' V.++ rs2') F.⊆ ((fs V.++ AddMaybe rs') V.++ F.RDeleteAll fs (fs V.++ AddMaybe rs2'))
                 , rs' F.⊆ rs
                 , rs2' F.⊆ rs2
                 , (fs V.++ AddMaybe rs') F.⊆ ((fs V.++ AddMaybe rs') V.++ F.RDeleteAll fs (fs V.++ AddMaybe rs2'))
                 , F.RDeleteAll fs (fs V.++ AddMaybe rs2') F.⊆ (fs V.++ AddMaybe rs2')                 
                 , (fs V.++ AddMaybe (rs' V.++ rs2')) ~ ((fs V.++ AddMaybe rs') V.++ F.RDeleteAll fs (fs V.++ AddMaybe rs2'))  
                 )
              => Proxy fs
              -> F.Frame (V.Rec (Maybe :. F.ElField) rs)  -- ^ The left frame
              -> F.Frame (V.Rec (Maybe :. F.ElField) rs2) -- ^ The right frame
              -> [V.Rec (Maybe :. F.ElField) (fs V.++ (rs' V.++ rs2'))] -- ^ A list of the merged records, now in the Maybe functor
leftJoinMaybe proxy_keys lf rf =
  let umLeft = fmap (unMaybeKeys proxy_keys) lf
      umRight = fmap (unMaybeKeys proxy_keys) rf
      lj :: [V.Rec (Maybe :. F.ElField) (fs V.++ AddMaybe (rs' V.++ rs2'))]
      lj = F.leftJoin @fs umLeft umRight -- [Rec (Maybe :. ElField) (fs ++ (AddMaybe rs1') ++ (AddMaybe rs2'))
      ljKeys :: V.Rec (Maybe :. F.ElField) (fs V.++ AddMaybe (rs' V.++ rs2')) -> V.Rec (Maybe :. F.ElField) fs
      ljKeys = F.rcast @fs
      ljRemainder :: V.Rec (Maybe :. F.ElField) (fs V.++ (AddMaybe (rs' V.++ rs2'))) -> V.Rec (Maybe :. F.ElField) (AddMaybe (rs' V.++ rs2'))
      ljRemainder = F.rcast
      ljCollectMaybes :: V.Rec (Maybe :. F.ElField) (AddMaybe (rs' V.++ rs2')) -> V.Rec (Maybe :. F.ElField) (rs' V.++ rs2')
      ljCollectMaybes = joinMaybe 
      ljNewRow :: V.Rec (Maybe :. F.ElField) (fs V.++ AddMaybe (rs' V.++ rs2')) -> V.Rec (Maybe :. F.ElField) (fs V.++ (rs' V.++ rs2'))
      ljNewRow x = (ljKeys x) `V.rappend` (ljCollectMaybes . ljRemainder $ x)
  in fmap ljNewRow lj


produceCSV_Maybe :: forall f ts m.
                   (F.ColumnHeaders (AddMaybe ts), Foldable f, Functor f, Monad m, V.RecordToList (AddMaybe ts),
                    V.RPureConstrained V.KnownField ts, V.RecApplicative ts, V.RApply ts, V.RMap ts,
                    V.RecMapMethod F.ShowCSV F.ElField (AddMaybe ts)) => f (F.Rec (Maybe :. F.ElField) ts) -> P.Producer String m ()                  
produceCSV_Maybe = F.produceCSV . fmap V.rsequenceInFields


writeCSV_Maybe :: (F.ColumnHeaders (AddMaybe ts), Foldable f, Functor f, V.RecordToList (AddMaybe ts),
                   V.RPureConstrained V.KnownField ts, V.RecApplicative ts, V.RApply ts, V.RMap ts,
                   V.RecMapMethod F.ShowCSV F.ElField (AddMaybe ts))
              => FilePath -> f (F.Rec (Maybe :. F.ElField) ts) -> IO ()
writeCSV_Maybe fp = F.writeCSV fp . fmap V.rsequenceInFields
