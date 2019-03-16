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
  ( unpackAggregateAndFoldF
  , unpackAggregateAndFoldSubsetF
  , aggregateAndFoldSubsetF
--  , liftFold
  , FoldRecord(..)
  , recFieldF
  , sequenceRecFold
  , FoldEndo(..)
  , sequenceEndoFolds
  , foldAll
  , foldAllConstrained
  , foldAllMonoid
  )
where

import qualified Frames.Aggregations.Monoidal  as FAM
import qualified Frames.MapReduce              as FMR
import qualified Control.Foldl                 as FL
import qualified Control.Newtype               as N
import           Data.Functor.Product           ( Product(Pair) )
import           Data.Monoid                    ( (<>)
                                                , Monoid(..)
                                                )
import qualified Data.Profunctor               as P
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Functor            as V
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import           GHC.TypeLits                   ( KnownSymbol )

-- | Simplified interfaces for aggregations where the operation on the collected rows is a fold
-- aggregateAndFoldF leaves in the unpack step and allows for folds from one set of fields (the `as`)
-- to another (the `cs`)
unpackAggregateAndFoldF
  :: forall ks as rs cs f g
   . ( ks F.⊆ as
     , as F.⊆ as
     , (ks V.++ cs) F.⊆ (ks V.++ cs)
     , Monoid (f (F.Record as)) -- can I do without this by not using the Monoidal version of aggregate??
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Applicative f
     )
  => (F.Rec g rs -> f (F.Record as))
  -> FL.Fold (F.Record as) (F.Record cs)
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
unpackAggregateAndFoldF unpack foldAtKey =
  FMR.aggregateMonoidalF @ks unpack (pure @f) (FL.fold foldAtKey)


unpackAggregateAndFoldSubsetF
  :: forall ks cs rs f g
   . ( ks F.⊆ (ks V.++ cs)
     , cs F.⊆ (ks V.++ cs)
     , (ks V.++ cs) F.⊆ (ks V.++ cs)
     , Monoid (f (F.Record (ks V.++ cs)))
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Applicative f
     )
  => (F.Rec g rs -> f (F.Record (ks V.++ cs)))
  -> FL.Fold (F.Record cs) (F.Record cs)
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
unpackAggregateAndFoldSubsetF unpack foldAtKey =
  unpackAggregateAndFoldF @ks @(ks V.++ cs)
    unpack
    (FL.premap (F.rcast @cs) foldAtKey)


-- | aggregateAndFoldSubset simplifies further by assuming that the inner fold is `cs` to `cs` and that the `cs` are a subset of the `rs`
aggregateAndFoldSubsetF
  :: forall ks cs rs
   . ( ks F.⊆ rs
     , cs F.⊆ rs
     , ks F.⊆ (ks V.++ cs)
     , cs F.⊆ (ks V.++ cs)
     , (ks V.++ cs) F.⊆ rs
     , (ks V.++ cs) F.⊆ (ks V.++ cs)
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     )
  => FL.Fold (F.Record cs) (F.Record cs)
  -> FL.Fold (F.Record rs) (F.FrameRec (ks V.++ cs))
aggregateAndFoldSubsetF f = unpackAggregateAndFoldF @ks
  (pure @[] . F.rcast @(ks V.++ cs))
  (FL.premap (F.rcast @cs) f)

-- TODO: Monadic versions of these ??

-- | Folding tools
-- Given a foldable h, `Fold a b` represents a fold over `(h a)` producing a `b` 
-- If we have an endo-fold, `Fold a a`, we can get a `Fold (ElField '(s, a)) (ElField '(s,a))`
fieldFold
  :: (V.KnownField t, a ~ V.Snd t)
  => FL.Fold a a
  -> FL.Fold (F.ElField t) (F.ElField t)
fieldFold = P.dimap (\(V.Field x) -> x) V.Field

-- | A Type synonym to help us remember when the folds are "monoidal"
type EndoFold a = FL.Fold a a

-- | We need types to act as "intepretation functors" for records of folds

-- | Wrapper or Endo-folds of plain types for ElFields
newtype FoldEndo t = FoldEndo { unFoldEndo :: EndoFold (V.Snd t) }  -- type FoldEndo a = FL.Fold a a

-- | Wrapper for endo-folds on an interpretation f.  Usually f ~ ElField 
newtype FoldFieldEndo f a = FoldFieldEndo { unFoldFieldEndo :: EndoFold (f a) } -- type FoldFieldEndo f a = FoldEndo (f a)

-- | Wrapper for folds from a record to an interpreted field.  Usually f ~ ElField
newtype FoldRecord f rs a = FoldRecord { unFoldRecord :: FL.Fold (F.Record rs) (f a) }

-- how do we build FoldRecords?
recFieldF
  :: forall t rs a
   . V.KnownField t
  => FL.Fold a (V.Snd t)
  -> (F.Record rs -> a)
  -> FoldRecord V.ElField rs t
recFieldF fld fromRec = FoldRecord $ P.dimap fromRec V.Field fld


fieldToFieldFold
  :: forall x y rs
   . (V.KnownField x, V.KnownField y, F.ElemOf rs x)
  => FL.Fold (V.Snd x) (V.Snd y)
  -> FoldRecord F.ElField rs y
fieldToFieldFold = FoldRecord . P.dimap (F.rgetField @x) (V.Field)


-- | Folds are contravariant in their input type.
-- So given a record of folds from records on a list of fields, we can fold over a records of a superset of fields via rcast
expandFoldInRecord
  :: forall rs as
   . (as F.⊆ rs, V.RMap as)
  => F.Rec (FoldRecord F.ElField as) as
  -> F.Rec (FoldRecord F.ElField rs) as
expandFoldInRecord = V.rmap (FoldRecord . FL.premap F.rcast . unFoldRecord)

-- | Change a record of single field folds to a record of folds from the entire record to each field
-- This requires a class since it is a function on types
class EndoFieldFoldsToRecordFolds rs where
  endoFieldFoldsToRecordFolds :: F.Rec (FoldFieldEndo F.ElField) rs -> F.Rec (FoldRecord F.ElField rs) rs

instance EndoFieldFoldsToRecordFolds '[] where
  endoFieldFoldsToRecordFolds _ = V.RNil

instance (EndoFieldFoldsToRecordFolds rs, rs F.⊆ (r ': rs), V.RMap rs) => EndoFieldFoldsToRecordFolds (r ': rs) where
  endoFieldFoldsToRecordFolds (fe V.:& fes) = FoldRecord (FL.premap (V.rget @r) (unFoldFieldEndo fe)) V.:& expandFoldInRecord @(r ': rs) (endoFieldFoldsToRecordFolds fes)


-- can we do all/some of this via F.Rec (Fold as) bs?
sequenceRecFold
  :: forall as rs
   . F.Rec (FoldRecord F.ElField as) rs
  -> FL.Fold (F.Record as) (F.Record rs)
sequenceRecFold = V.rtraverse unFoldRecord


-- | turn a record of folds over each field, into a fold over records 
sequenceFieldEndoFolds
  :: EndoFieldFoldsToRecordFolds rs
  => F.Rec (FoldFieldEndo F.ElField) rs
  -> FL.Fold (F.Record rs) (F.Record rs)
sequenceFieldEndoFolds = sequenceRecFold . endoFieldFoldsToRecordFolds

liftFold
  :: V.KnownField t => FL.Fold (V.Snd t) (V.Snd t) -> FoldFieldEndo F.ElField t
liftFold = FoldFieldEndo . fieldFold

-- This is not a natural transformation, FoldEndoT ~> FoldEndo F.EField, because of the constraint
liftFoldEndo :: V.KnownField t => FoldEndo t -> FoldFieldEndo F.ElField t
liftFoldEndo = FoldFieldEndo . fieldFold . unFoldEndo

liftFolds
  :: (V.RPureConstrained V.KnownField rs, V.RApply rs)
  => F.Rec FoldEndo rs
  -> F.Rec (FoldFieldEndo F.ElField) rs
liftFolds = V.rapply liftedFs
  where liftedFs = V.rpureConstrained @V.KnownField $ V.Lift liftFoldEndo

-- | turn a record of folds over each field, into a fold over records 
sequenceEndoFolds
  :: forall rs
   . ( V.RApply rs
     , V.RPureConstrained V.KnownField rs
     , EndoFieldFoldsToRecordFolds rs
     )
  => F.Rec FoldEndo rs
  -> FL.Fold (F.Record rs) (F.Record rs)
sequenceEndoFolds = sequenceFieldEndoFolds . liftFolds
--  V.rtraverse unFoldInRecord . endoFieldFoldsToRecordFolds . liftFolds

-- | apply an unconstrained endo-fold, e.g., a fold which takes the last item in a container, to every field in a record
foldAll
  :: ( V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs
     )
  => (forall a . FL.Fold a a)
  -> FL.Fold (F.Record rs) (F.Record rs)
foldAll f = sequenceEndoFolds $ V.rpureConstrained @V.KnownField (FoldEndo f)

class (c (V.Snd t)) => ConstrainedField c t
instance (c (V.Snd t)) => ConstrainedField c t

-- | Apply a constrained endo-fold to all fields of a record.
-- May require a use of TypeApplications, e.g., foldAllConstrained @Num FL.sum
foldAllConstrained
  :: forall c rs
   . ( V.RPureConstrained (ConstrainedField c) rs
     , V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs
     )
  => (forall a . c a => FL.Fold a a)
  -> FL.Fold (F.Record rs) (F.Record rs)
foldAllConstrained f =
  sequenceEndoFolds $ V.rpureConstrained @(ConstrainedField c) (FoldEndo f)

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
     , V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs
     )
  => FL.Fold (F.Record rs) (F.Record rs)
foldAllMonoid = foldAllConstrained @(MonoidalField f) $ monoidWrapperToFold @f

