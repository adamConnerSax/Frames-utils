{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.Aggregations.Folds
  ( aggregateAndFoldF
  , aggregateAndFoldSubsetF
  , liftFold
  , recordFold
  , foldAll
  , foldAllConstrained
  , foldAllMonoid
  )
where

import qualified Frames.Aggregations.Monoidal  as FAM

import qualified Control.Foldl                 as FL
import qualified Control.Newtype               as N
import           Data.Monoid                    ( (<>)
                                                , Monoid(..)
                                                )
import qualified Data.Profunctor               as P
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
import qualified Frames.InCore                 as FI

-- | Simplified interfaces for aggregations where the operation on the collected rows is a fold
-- aggregateAndFoldF leaves in the unpack step and allows for folds from one set of fields (the `as`)
-- to another (the `cs`)
aggregateAndFoldF
  :: forall ks rs as cs f g
   . ( ks F.⊆ as
     , Monoid (f (F.Record as))
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Applicative f
     )
  => (F.Rec g rs -> f (F.Record as))
  -> FL.Fold (F.Record as) (F.Record cs)
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateAndFoldF unpack foldAtKey =
  FAM.aggregateMonoidalF @ks unpack (pure @f) (FL.fold foldAtKey)


-- | aggregateAndFoldSubset simplifies further by assuming that the inner fold is `cs` to `cs` and that the `cs` are a subset of the `rs`
aggregateAndFoldSubsetF
  :: forall ks cs rs
   . ( ks F.⊆ rs
     , cs F.⊆ rs
     , ks F.⊆ (ks V.++ cs)
     , cs F.⊆ (ks V.++ cs)
     , (ks V.++ cs) F.⊆ rs
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     )
  => FL.Fold (F.Record cs) (F.Record cs)
  -> FL.Fold (F.Record rs) (F.FrameRec (ks V.++ cs))
aggregateAndFoldSubsetF f = aggregateAndFoldF @ks
  (pure @[] . F.rcast @(ks V.++ cs))
  (FL.premap (F.rcast @cs) f)

-- TODO: Monadic versions of these

-- | Folding tools
-- Given a foldable h, `Fold a b` represents a fold over `(h a)` producing a `b` 
-- If we have an endo-fold, `Fold a a`, we can get a `Fold (ElField '(s, a)) (ElField '(s,a))`
fieldFold
  :: V.KnownField t
  => FL.Fold (V.Snd t) (V.Snd t)
  -> FL.Fold (F.ElField t) (F.ElField t)
fieldFold = P.dimap (\(V.Field x) -> x) V.Field

-- | We need types to act as "intepretation functors" for records of endo-folds
-- One for an endo-fold on one field and one for a fold over a record to one field
newtype FoldEndo f a = FoldEndo { unFoldEndo :: FL.Fold (f a) (f a) }
newtype FoldInRecord f rs a = FoldInRecord { unFoldInRecord :: FL.Fold (F.Record rs) (f a) }

-- | Given a record of folds over one list of fields, we can fold over a larger list of fields via rcast
expandFoldInRecord
  :: forall rs as
   . (as F.⊆ rs, V.RMap as)
  => F.Rec (FoldInRecord F.ElField as) as
  -> F.Rec (FoldInRecord F.ElField rs) as
expandFoldInRecord = V.rmap (FoldInRecord . FL.premap F.rcast . unFoldInRecord)

-- | Change a record of single field folds to a record of folds from the entire record to each field
class EndoFoldsToRecordFolds rs where
  endoFoldsToRecordFolds :: F.Rec (FoldEndo F.ElField) rs -> F.Rec (FoldInRecord F.ElField rs) rs

instance EndoFoldsToRecordFolds '[] where
  endoFoldsToRecordFolds _ = V.RNil

instance (EndoFoldsToRecordFolds rs, rs F.⊆ (r ': rs), V.RMap rs) => EndoFoldsToRecordFolds (r ': rs) where
  endoFoldsToRecordFolds (fe V.:& fes) = FoldInRecord (FL.premap (V.rget @r) (unFoldEndo fe)) V.:& expandFoldInRecord @(r ': rs) (endoFoldsToRecordFolds fes)

-- | turn a record of folds over each field, into a fold over records 
recordFold
  :: EndoFoldsToRecordFolds rs
  => F.Rec (FoldEndo F.ElField) rs
  -> FL.Fold (F.Record rs) (F.Record rs)
recordFold = V.rtraverse unFoldInRecord . endoFoldsToRecordFolds

liftFold
  :: V.KnownField t => FL.Fold (V.Snd t) (V.Snd t) -> FoldEndo F.ElField t
liftFold = FoldEndo . fieldFold

{-
-- | With simpler initial types
newtype FoldEndoT t = FoldEndoT { unFoldEndoT :: FL.Fold (V.Snd t) (V.Snd t) }
-- | turn a record of folds over each field, into a fold over records 
recordFoldT
  :: forall rs
   . (V.RMap rs, EndoFoldsToRecordFolds rs, V.AllConstrained V.KnownField rs)
  => F.Rec FoldEndoT rs
  -> FL.Fold (F.Record rs) (F.Record rs)
recordFoldT = V.rtraverse unFoldInRecord . endoFoldsToRecordFolds . V.rmap
  (FoldEndo . fieldFold . unFoldEndoT)
-}

-- | apply an unconstrained endo-fold, e.g., a fold which takes the last item in a container, to every field in a record
foldAll
  :: (V.RPureConstrained V.KnownField rs, EndoFoldsToRecordFolds rs)
  => (forall a . FL.Fold a a)
  -> FL.Fold (F.Record rs) (F.Record rs)
foldAll f =
  recordFold $ V.rpureConstrained @V.KnownField (FoldEndo $ fieldFold f)

class (c (V.Snd t), V.KnownField t) => ConstrainedField c t
instance (c (V.Snd t), V.KnownField t) => ConstrainedField c t

-- | Apply a constrained endo-fold to all fields of a record.
-- May require a use of TypeApplications, e.g., foldAllConstrained @Num FL.sum
foldAllConstrained
  :: forall c rs
   . (V.RPureConstrained (ConstrainedField c) rs, EndoFoldsToRecordFolds rs)
  => (forall a . c a => FL.Fold a a)
  -> FL.Fold (F.Record rs) (F.Record rs)
foldAllConstrained f =
  recordFold $ V.rpureConstrained @(ConstrainedField c) (FoldEndo $ fieldFold f)

-- | Given a monoid-wrapper, e.g., Sum, and functions to wrap and unwrap, we can produce an endo-fold on a
monoidWrapperToFold
  :: forall f a . (N.Newtype (f a) a, Monoid (f a)) => FL.Fold a a
monoidWrapperToFold = FL.Fold (\w a -> N.pack a <> w) (mempty @(f a)) N.unpack -- is this the correct order in (<>) ?

class (N.Newtype (f a) a, Monoid (f a)) => MonoidalField f a
instance (N.Newtype (f a) a, Monoid (f a)) => MonoidalField f a

-- | Given a monoid-wrapper, e.g., Sum, apply the derived endo-fold to all fields of a record
-- This is strictly less powerful than foldAllConstrained but might be simpler to use in some cases
foldAllMonoid
  :: forall f rs
   . ( V.RPureConstrained (ConstrainedField (MonoidalField f)) rs
     , EndoFoldsToRecordFolds rs
     )
  => FL.Fold (F.Record rs) (F.Record rs)
foldAllMonoid = foldAllConstrained @(MonoidalField f) $ monoidWrapperToFold @f



--  aggregateMonoidalF @ks unpack (pure @f) (FL.fold foldAtKey)  

{-
instance (V.KnownField t, N.Newtype (h (F.ElField t)) (F.ElField t)) => N.Newtype ((h F.:. F.ElField) t) (F.ElField t) where
  pack = V.Compose . N.pack
  unpack = N.unpack . V.getCompose

--type NewtypeFormer f a = N.Newtype (f a) a

class N.Newtype (f a) a => NewtypeWrapped f a
class N.Newtype (f (F.ElField a)) (F.ElField a) => NewtypeUnWrapped f a

--data DictWrapped c f a where
--  DictWrapped :: c a => f a -> DictWrapped c f a

{-
class ReifyWrapped c f rs where
  reifyWrapped
    :: V.Rec f rs
    -> V.Rec (DictWrapped c f :. f) rs

instance ReifyWrapped c f '[] where
  reifyWrapped V.RNil = V.RNil
  {-# INLINE reifyWrapped #-}

instance (c x, ReifyWrapped c f xs)
  => ReifyWrapped c f (x ': xs) where
  reifyWrapped (x V.:& xs) = V.Compose (DictWrapped x) V.:& reifyWrapped xs
  {-# INLINE reifyWrapped #-}
-}



aggregateMonoidalWrappedF
  :: forall ks bs h rs as f g
   . ( ks F.⊆ as
     , bs F.⊆ as
     , V.RMap bs
--     , F.RecAll V.Snd bs (N.Newtype h)
     , V.ReifyConstraint (NewtypeWrapped h) F.ElField bs
     , V.RecMapMethod (NewtypeWrapped h) (h :. F.ElField) bs
     , Monoid (F.Rec (h F.:. F.ElField) bs)
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ bs)
     , Foldable f
     )
  => (F.Rec g rs -> f (F.Record as))
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ bs))
aggregateMonoidalWrappedF unpack = aggregateMonoidalF @ks
  unpack
  (wrap . F.rcast @bs)
  unWrap
 where
  wrap :: F.Rec F.ElField bs -> F.Rec (h F.:. F.ElField) bs
  wrap =
    V.rmap (\(V.Compose (V.Dict x)) -> V.Compose $ N.pack x)
      . V.reifyConstraint @(NewtypeWrapped h) --V.rmap (V.Compose . N.pack)

  unWrap :: F.Rec (h F.:. F.ElField) bs -> F.Rec F.ElField bs
  unWrap = V.rmapMethod @(NewtypeWrapped h) (N.unpack . V.getCompose)
-}
{-  fix
    :: V.KnownField t => F.ElField '(V.Fst t, Identity (V.Snd t)) -> F.ElField t
  fix = V.Field . V.getIdentity . V.getField -}
--    V.rmap (\(V.Compose (V.Dict x)) -> N.unpack x)
--      . V.reifyConstraint @(NewtypeUnWrapped h) . V.rsequenceIn

{-
  unWrap = V.rapply unwrappers
  unWrappers :: V.Rec (V.Lift (->) (h F.:. F.ElField) F.ElField) bs
  unWrappers = Lift (N.unpack . V.getCompose)
-}


{-  reifyNewtype
    :: F.Rec (h F.:. F.ElField) qs
    -> F.Rec (V.DictWrapped (NewtypeWrapped h) F.:. (h F.:. F.ElField)) qs
  reifyNewtype r = case r of
    V.RNil      -> V.RNil
    (x V.:& xs) -> V.Compose (V.Dict x) V.:& reifyNewtype xs
--  (N.unpack . V.getCompose)
-}

