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
module Frames.Aggregations
  ( FType
  , DblX
  , DblY
  , goodDataCount
  , goodDataByKey
  , filterField
  , filterMaybeField
  , aggregateToMap
  , aggregateGeneral
  , aggregateFiltered
  , aggregateFM
  , aggregateF
  , aggregateFsM
  , aggregateFs
  , aggregateAndAnalyzeEachM
  , aggregateAndAnalyzeEachM'
  , aggregateAndAnalyzeEach
  , aggregateToMonoidalMap
  , aggregateMonoidalGeneral
  , aggregateMonoidalFiltered
  , aggregateMonoidalFM
  , aggregateMonoidalF
  , aggregateMonoidalFsM
  , aggregateMonoidalFs
  , aggregateAndFoldF
  , aggregateAndFoldSubsetF
  , foldAll
  , foldAllConstrained
  , foldAllMonoid
  , RealField
  , RealFieldOf
  , TwoColData
  , ThreeColData
  , ThreeDTransformable
  , KeyedRecord
  )
where

import qualified Control.Foldl                 as FL
import qualified Control.Newtype               as N
import           Data.Traversable               ( sequenceA )
import           Data.Functor.Identity          ( Identity(Identity) )
import qualified Data.Foldable                 as Foldable
import qualified Data.List                     as List
import qualified Data.Map                      as M
import qualified Data.Map.Monoidal             as MM
import           Data.Maybe                     ( fromMaybe
                                                , isJust
                                                , fromJust
                                                )
import           Data.Monoid                    ( (<>)
                                                , Monoid(..)
                                                )
import qualified Data.Profunctor               as P
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.Functor            as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.XRec               as V
import qualified Data.Vinyl.Class.Method       as V
import           Frames                         ( (:.) )
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import           Control.Arrow                  ( second )

-- THe functions below should move.  To MaybeUtils?  What about filterField?
-- returns a map, keyed by F.Record ks, of (number of rows, number of rows with all cols parsed)
-- required TypeApplications for ks
goodDataByKey
  :: forall ks rs
   . (ks F.⊆ rs, Ord (F.Record ks))
  => FL.Fold
       (F.Rec (Maybe F.:. F.ElField) rs)
       (M.Map (F.Record ks) (Int, Int))
goodDataByKey =
  let getKey = F.recMaybe . F.rcast @ks
  in  FL.prefilter (isJust . getKey) $ FL.Fold
        (aggregateToMap (fromJust . getKey) (flip (:)) [])
        M.empty
        (fmap $ FL.fold goodDataCount)

goodDataCount :: FL.Fold (F.Rec (Maybe F.:. F.ElField) rs) (Int, Int)
goodDataCount =
  (,) <$> FL.length <*> FL.prefilter (isJust . F.recMaybe) FL.length

filterField
  :: forall k rs
   . (V.KnownField k, F.ElemOf rs k)
  => (V.Snd k -> Bool)
  -> F.Record rs
  -> Bool
filterField test = test . F.rgetField @k

filterMaybeField
  :: forall k rs
   . (F.ElemOf rs k, V.IsoHKD F.ElField k)
  => (V.HKD F.ElField k -> Bool)
  -> F.Rec (Maybe :. F.ElField) rs
  -> Bool
filterMaybeField test = maybe False test . V.toHKD . F.rget @k


aggregateToMap
  :: Ord k => (a -> k) -> (b -> a -> b) -> b -> M.Map k b -> a -> M.Map k b
aggregateToMap getKey combine initial m r =
  let key    = getKey r
      newVal = Just . flip combine r . fromMaybe initial
  in  M.alter newVal key m --M.insert key newVal m 

aggregateToMonoidalMap
  :: (Ord k, Monoid b)
  => (a -> k)
  -> (a -> b)
  -> MM.MonoidalMap k b
  -> a
  -> MM.MonoidalMap k b
aggregateToMonoidalMap getKey getB m r =
  let key    = getKey r
      newVal = Just . (<> getB r) . fromMaybe mempty
  in  MM.alter newVal key m --M.insert key newVal m 

-- fold over c.  But c may become zero or many a (bad data, or melting rows). So we process c, then fold over the result.
aggregateGeneral
  :: (Ord k, Foldable f)
  => (c -> f a)
  -> (a -> k)
  -> (b -> a -> b)
  -> b
  -> M.Map k b
  -> c
  -> M.Map k b
aggregateGeneral unpack getKey combine initial m x =
  let aggregate = FL.Fold (aggregateToMap getKey combine initial) m id
  in  FL.fold aggregate (unpack x)

aggregateMonoidalGeneral
  :: (Ord k, Foldable f, Monoid b)
  => (c -> f a)
  -> (a -> k)
  -> (a -> b)
  -> MM.MonoidalMap k b
  -> c
  -> MM.MonoidalMap k b
aggregateMonoidalGeneral unpack getKey getB m x =
  let aggregate = FL.Fold (aggregateToMonoidalMap getKey getB) m id
  in  FL.fold aggregate (unpack x)

-- Maybe is delightfully foldable!  
aggregateFiltered
  :: Ord k
  => (c -> Maybe a)
  -> (a -> k)
  -> (b -> a -> b)
  -> b
  -> M.Map k b
  -> c
  -> M.Map k b
aggregateFiltered = aggregateGeneral

-- Maybe is delightfully foldable!  
aggregateMonoidalFiltered
  :: (Ord k, Monoid b)
  => (c -> Maybe a)
  -> (a -> k)
  -> (a -> b)
  -> MM.MonoidalMap k b
  -> c
  -> MM.MonoidalMap k b
aggregateMonoidalFiltered = aggregateMonoidalGeneral


liftCombine :: Applicative m => (a -> b -> a) -> (a -> b -> m a)
liftCombine f a = pure . f a

-- specific version for our record folds via Control.Foldl
-- extract--the processing of one aggregate's data--may be monadic 
aggregateFsM
  :: forall ks rs as b cs f g h m
   . ( ks F.⊆ as
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Foldable h
     , Functor h
     , Applicative m
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (b -> F.Record as -> b)
  -> b
  -> (b -> m (h (F.Record cs)))
  -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFsM unpack process initial extract =
  let addKey
        :: (F.Record ks, m (h (F.Record cs))) -> m (h (F.Record (ks V.++ cs)))
      addKey (k, mhcs) = fmap (fmap (V.rappend k)) mhcs
  in  FL.FoldM
        (liftCombine $ aggregateGeneral unpack (F.rcast @ks) process initial)
        (pure M.empty)
        ( fmap (F.toFrame . List.concat)
        . sequenceA
        . fmap ((fmap Foldable.toList) . addKey . second extract)
        . M.toList
        )

aggregateMonoidalFsM
  :: forall ks rs as b cs f g h m
   . ( ks F.⊆ as
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Monoid b
     , Foldable f
     , Foldable h
     , Functor h
     , Applicative m
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (F.Record as -> b)
  -> (b -> m (h (F.Record cs)))
  -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalFsM unpack toMonoid extract =
  let addKey
        :: (F.Record ks, m (h (F.Record cs))) -> m (h (F.Record (ks V.++ cs)))
      addKey (k, mhcs) = fmap (fmap (V.rappend k)) mhcs
  in  FL.FoldM
        (liftCombine $ aggregateMonoidalGeneral unpack (F.rcast @ks) toMonoid)
        (pure MM.empty)
        ( fmap (F.toFrame . List.concat)
        . sequenceA
        . fmap ((fmap Foldable.toList) . addKey . second extract)
        . MM.toList
        )

aggregateFs
  :: forall ks rs as cs b f g h
   . ( ks F.⊆ as
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Foldable h
     , Functor h
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (b -> F.Record as -> b)
  -> b
  -> (b -> h (F.Record cs))
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFs unpack process initial extract =
  FL.simplify $ aggregateFsM @ks unpack process initial (return . extract)

aggregateMonoidalFs
  :: forall ks rs as cs b f g h
   . ( ks F.⊆ as
     , Monoid b
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Foldable h
     , Functor h
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (F.Record as -> b)
  -> (b -> h (F.Record cs))
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalFs unpack toMonoid extract =
  FL.simplify $ aggregateMonoidalFsM @ks unpack toMonoid (return . extract)

aggregateFM
  :: forall ks rs as b cs f g m
   . ( ks F.⊆ as
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Applicative m
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (b -> F.Record as -> b)
  -> b
  -> (b -> m (F.Record cs))
  -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFM unpack process initial extract =
  aggregateFsM @ks unpack process initial (fmap V.Identity . extract)

aggregateMonoidalFM
  :: forall ks rs as b cs f g m
   . ( ks F.⊆ as
     , Monoid b
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Applicative m
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (F.Record as -> b)
  -> (b -> m (F.Record cs))
  -> FL.FoldM m (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalFM unpack toMonoid extract =
  aggregateMonoidalFsM @ks unpack toMonoid (fmap V.Identity . extract)

aggregateF
  :: forall ks rs as b cs f g
   . (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f)
  => (F.Rec g rs -> f (F.Record as))
  -> (b -> F.Record as -> b)
  -> b
  -> (b -> F.Record cs)
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateF unpack process initial extract =
  aggregateFs @ks unpack process initial (V.Identity . extract)

aggregateMonoidalF
  :: forall ks rs as b cs f g
   . ( ks F.⊆ as
     , Monoid b
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (F.Record as -> b)
  -> (b -> F.Record cs)
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalF unpack toMonoid extract =
  aggregateMonoidalFs @ks unpack toMonoid (V.Identity . extract)


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
  aggregateMonoidalF @ks unpack (pure @f) (FL.fold foldAtKey)

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

type FType x = V.Snd x

type DblX = "double_x" F.:-> Double
type DblY = "double_y" F.:-> Double

type UseCols ks x y w = ks V.++ '[x,y,w]
type RealField x = (V.KnownField x, Real (FType x))

-- This thing is...unfortunate. Is there something built into Frames or Vinyl that would do this?
class (RealField x, x V.∈ rs) => RealFieldOf rs x
instance (RealField x, x V.∈ rs) => RealFieldOf rs x

type TwoColData x y = F.AllConstrained (RealFieldOf [x,y]) '[x, y]
type ThreeColData x y z = ([x,z] F.⊆ [x,y,z], [y,z] F.⊆ [x,y,z], [x,y] F.⊆ [x,y,z], F.AllConstrained (RealFieldOf [x,y,z]) '[x, y, z])

type KeyedRecord ks rs = (ks F.⊆ rs, Ord (F.Record ks))

type ThreeDTransformable rs ks x y w = (ThreeColData x y w, FI.RecVec (ks V.++ [x,y,w]),
                                        KeyedRecord ks rs,
                                        ks F.⊆ (ks V.++ [x,y,w]), (ks V.++ [x,y,w]) F.⊆ rs,
                                        F.ElemOf (ks V.++ [x,y,w]) x, F.ElemOf (ks V.++ [x,y,w]) y, F.ElemOf (ks V.++ [x,y,w]) w)

-- These use the monoid instance of [], since the first argument is ([F.Record '[x,y,w]] -> ...)
-- could we use a smarter structure here?  Sequence? The frame itself?
aggregateAndAnalyzeEachM
  :: forall ks x y w rs m
   . (ThreeDTransformable rs ks x y w, Applicative m)
  => ([F.Record '[x, y, w]] -> m [F.Record '[x, y, w]])
  -> FL.FoldM m (F.Record rs) (F.FrameRec (UseCols ks x y w))
aggregateAndAnalyzeEachM doOne = aggregateMonoidalFsM @ks
  (V.Identity . (F.rcast @(ks V.++ '[x, y, w])))
  (pure . F.rcast @'[x, y, w])
  doOne

aggregateAndAnalyzeEach
  :: forall ks x y w rs
   . (ThreeDTransformable rs ks x y w)
  => ([F.Record '[x, y, w]] -> [F.Record '[x, y, w]])
  -> FL.Fold (F.Record rs) (F.FrameRec (UseCols ks x y w))
aggregateAndAnalyzeEach doOne =
  FL.simplify $ aggregateAndAnalyzeEachM @ks (Identity . doOne)

aggregateAndAnalyzeEachM'
  :: forall ks rs as m
   . (KeyedRecord ks rs, FI.RecVec (ks V.++ as), Applicative m)
  => ([F.Record rs] -> m [F.Record as])
  -> FL.FoldM m (F.Record rs) (F.FrameRec (ks V.++ as))
aggregateAndAnalyzeEachM' doOne =
  aggregateMonoidalFsM @ks V.Identity pure doOne


