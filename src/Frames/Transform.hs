{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DerivingVia         #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.Transform
  (
    frameConcat
  , mutate
  , mutateM
  , transform
  , transformMaybe
  , fieldEndo
  , recordSingleton
  , addColumn
  , dropColumn
  , replaceColumn
  , Rename
  , rename
  , ReType
  , reType
  , dropColumns
  , retypeColumn
  , addOneFromOne
  , addOneFrom
  , addName
  , addOneFromValue
  , RetypeColumns(..)
  , reshapeRowSimple
  , reshapeRowSimpleOnOne
  , FieldDefaults (..)
  , DefaultRecord (..)
  -- unused/deprecated
  , BinaryFunction(..)
  , bfApply
  , bfPlus
  , bfLHS
  , bfRHS
  ) where

import qualified Data.Text            as T
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Curry     as V
import qualified Data.Vinyl.Functor   as V
import           Data.Vinyl.TypeLevel as V --(type (++), Snd)
import qualified Frames               as F
import qualified Frames.InCore        as FI
import           Frames.Melt          (RDeleteAll, ElemOf)

import           GHC.TypeLits         (KnownSymbol, Symbol)
import Unsafe.Coerce (unsafeCoerce)

frameConcat :: (Functor f, Foldable f, FI.RecVec rs) => f (F.FrameRec rs) -> F.FrameRec rs
frameConcat x = if length x < 600
                then mconcat $ toList x
                else F.toFrame $ concatMap toList x
{-# INLINEABLE frameConcat #-}
-- |  mutation functions

-- | Type preserving single-field mapping
fieldEndo :: forall x rs. (V.KnownField x, ElemOf rs x) => (Snd x -> Snd x) -> F.Record rs -> F.Record rs
fieldEndo f r = F.rputField @x (f $ F.rgetField @x r) r
{-# INLINE fieldEndo #-}
{-
f :: KnownSymbol s => (s,(a -> b)) -> F.Rec ((->) a) ['(s,b)]
f (_, fieldFunc) = fieldFunc V.:& V.RNil
-}

-- | replace subset with a calculated different set of fields
transform :: forall rs as bs. (as F.⊆ rs, RDeleteAll as rs F.⊆ rs)
             => (F.Record as -> F.Record bs) -> F.Record rs -> F.Record (RDeleteAll as rs V.++ bs)
transform f xs = F.rcast @(RDeleteAll as rs) xs `F.rappend` f (F.rcast xs)
{-# INLINEABLE transform #-}

transformMaybe :: forall rs as bs. (as F.⊆ rs, RDeleteAll as rs F.⊆ rs, V.RMap (RDeleteAll as rs))
               => (F.Record as -> F.Rec (Maybe F.:. F.ElField) bs) -> F.Record rs -> F.Rec (Maybe F.:. F.ElField) (RDeleteAll as rs V.++ bs)
transformMaybe f xs = V.rmap (V.Compose . Just) (F.rcast @(RDeleteAll as rs) xs) `V.rappend` f (F.rcast xs)
{-# INLINEABLE transformMaybe #-}

-- | append calculated subset
mutate :: forall rs bs. (F.Record rs -> F.Record bs) -> F.Record rs -> F.Record (rs V.++ bs)
mutate f xs = xs `F.rappend` f xs
{-# INLINEABLE mutate #-}

mutateM ::  forall rs bs m. Functor m => (F.Record rs -> m (F.Record bs)) -> F.Record rs -> m (F.Record (rs V.++ bs))
mutateM f xs = F.rappend xs <$> f xs
{-# INLINEABLE mutateM #-}

-- | Create a record with one field from a value.  Use a TypeApplication to choose the field.
recordSingleton :: forall af s a. (KnownSymbol s, af ~ '(s,a)) => a -> F.Record '[af]
recordSingleton a = a F.&: V.RNil
{-# INLINEABLE recordSingleton #-}


replaceColumn :: forall t t' rs. (V.KnownField t
                                 , V.KnownField t'
                                 , (RDelete t rs V.++ '[t']) F.⊆ (rs V.++ '[t'])
                                 , ElemOf rs t
                                 )
              => (V.Snd t -> V.Snd t')
              -> F.Record rs -> F.Record (RDelete t rs V.++ '[t'])
replaceColumn f = F.rcast @(RDelete t rs V.++ '[t']) . mutate (recordSingleton @t' . f . F.rgetField @t)
{-# INLINEABLE replaceColumn #-}


addOneFromOne :: forall t t' rs. (V.KnownField t, V.KnownField t', ElemOf rs t) => (V.Snd t -> V.Snd t') -> F.Record rs -> F.Record (t' ': rs)
addOneFromOne f xs = f (F.rgetField @t xs) F.&: xs
{-# INLINEABLE addOneFromOne #-}

addOneFrom
  :: forall as t rs
     . (V.IsoXRec F.ElField as
       , V.KnownField t, as F.⊆ rs)
  => V.CurriedX V.ElField as (V.Snd t) -> F.Record rs -> F.Record (t ': rs)
addOneFrom f xs = V.runcurryX f (F.rcast @as xs) F.&: xs
{-# INLINEABLE addOneFrom #-}

addName :: forall t t' rs. (V.KnownField t, V.KnownField t', V.Snd t ~ V.Snd t', ElemOf rs t) => F.Record rs -> F.Record (t' ': rs)
addName = addOneFromOne @t @t' id
{-# INLINEABLE addName #-}

addOneFromValue :: forall t rs. V.KnownField t => V.Snd t -> F.Record rs -> F.Record (t ': rs)
addOneFromValue = (F.&:)
{-# INLINEABLE addOneFromValue #-}

-- | add a column
addColumn :: forall t bs. (V.KnownField t) => V.Snd t -> F.Record bs -> F.Record (t ': bs)
addColumn x r = recordSingleton x `V.rappend` r
{-# INLINEABLE addColumn #-}

-- | Drop a column from a record.  Just a specialization of rcast.
dropColumn :: forall x rs. (F.RDelete x rs F.⊆ rs) => F.Record rs -> F.Record (F.RDelete x rs)
dropColumn = F.rcast
{-# INLINEABLE dropColumn #-}

-- | Drop a set of columns from a record. Just a specialization of rcast.
dropColumns :: forall xs rs. (RDeleteAll xs rs F.⊆ rs) => F.Record rs -> F.Record (RDeleteAll xs rs)
dropColumns = F.rcast
{-# INLINEABLE dropColumns #-}

-- |  change a column "name" at the type level
retypeColumn :: forall x y rs. ( V.KnownField x
                               , V.KnownField y
                               , V.Snd x ~ V.Snd y
                               , ElemOf rs x
                               , F.RDelete x rs F.⊆ rs
--                               , Rename (Fst x) (Fst y) rs ~ (RDelete '(Fst x, Snd y) rs ++ '[ '(Fst y, Snd y)]))
                               )
  => F.Record rs -> F.Record (F.RDelete x rs V.++ '[y])
retypeColumn = transform @rs @'[x] @'[y] (\r -> F.rgetField @x r F.&: V.RNil)
{-# INLINEABLE retypeColumn #-}


-- TODO: replace all of the renaming with this.  But it will add contraints everywhere (and remove a bunch and I've not time right now! -}
{- From a vinyl PR.  This way os better -}
-- | @Rename old new fields@ replaces the first occurence of the
-- field label @old@ with @new@ in a list of @fields@. Used by
-- 'rename'.
type family Rename old new ts where
  Rename old new '[] = '[]
  Rename old new ('(old,x) ': xs) = '(new,x) ': xs
  Rename old new ('(s,x) ': xs) = '(s,x) ': Rename old new xs

-- | Replace a field label. Example:
--
-- @rename \@"name" \@"handle" (fieldRec (#name =: "Joe", #age =: (40::Int)))
rename :: forall old new ts. V.Rec V.ElField ts -> V.Rec V.ElField (Rename old new ts)
rename = unsafeCoerce

type family ReType old new ts where
  ReType old new '[] = '[]
  ReType old new (old ': xs) = new ': ReType old new xs
  ReType old new (t ': xs) = t ': ReType old new xs

-- | Replace a field type. Example:
--
reType :: forall old new ts. V.Rec V.ElField ts -> V.Rec V.ElField (ReType old new ts)
reType = unsafeCoerce



-- take a type-level-list of (fromName, toName, type) and use it to rename columns in suitably typed record
type family FromRecList (a :: [(Symbol, Symbol, Type)]) :: [(Symbol, Type)] where
  FromRecList '[] = '[]
  FromRecList ('(fs, ts, t) ': rs) = '(fs, t) ': FromRecList rs

type family ToRecList (a :: [(Symbol, Symbol, Type)]) :: [(Symbol, Type)] where
  ToRecList '[] = '[]
  ToRecList ('(fs, ts, t) ': rs) = '(ts, t) ': ToRecList rs

class (FromRecList cs F.⊆ rs) => RetypeColumns (cs :: [(Symbol, Symbol, Type)]) (rs :: [(Symbol, Type)]) where
  retypeColumns :: (rs ~ (rs V.++ '[])) => F.Record rs -> F.Record (RDeleteAll (FromRecList cs) rs V.++ ToRecList cs)

instance RetypeColumns '[] rs where
  retypeColumns = id

instance (RetypeColumns cs rs
         , V.KnownField '(fs, t)
         , V.KnownField '(ts, t)
         , ElemOf rs '(fs, t)
         , (RDelete '(fs, t) (RDeleteAll (FromRecList cs) rs V.++ ToRecList cs) V.++ '[ '(ts, t)])
         ~ (RDeleteAll (FromRecList cs) (RDelete '(fs, t) rs) V.++ ('(ts, t) ': ToRecList cs))
         , ElemOf (RDeleteAll (FromRecList cs) rs ++ ToRecList cs) '(fs, t)
         , RDelete '(fs, t) (RDeleteAll (FromRecList cs) rs ++ ToRecList cs) F.⊆ (RDeleteAll (FromRecList cs) rs ++ ToRecList cs)
         , Rename fs ts (RDeleteAll (FromRecList cs) rs ++ ToRecList cs) ~ (RDeleteAll (FromRecList cs) (RDelete '(fs, t) rs) ++ ('(ts, t) : ToRecList cs))
         )
    => RetypeColumns ('(fs, ts, t) ': cs) rs where
  retypeColumns = retypeColumn @'(fs, t) @'(ts, t) @(RDeleteAll (FromRecList cs) rs V.++ ToRecList cs)  . retypeColumns @cs @rs

-- This is an anamorphic step.
-- You could also use meltRow here.  That is also (Record as -> [Record bs])
-- requires typeApplications for ss
-- | Given a set of classifier fields cs, and a function which combines the classifier and the data, a row with fields ts,
-- turn a list of classifier values and a data row into a list of rows, each with the classifier value, the result of applying the function
-- and any other columns from the original row that you specify (the ss fields).
reshapeRowSimple :: forall ss ts cs ds. (ss F.⊆ ts)
                 => [F.Record cs] -- ^ list of classifier values
                 -> (F.Record cs -> F.Record ts -> F.Record ds) -- ^ map classifier value and data to new shape
                 -> F.Record ts -- ^ original data
                 -> [F.Record (ss V.++ cs V.++ ds)] -- ^ reshaped data
reshapeRowSimple classifiers newDataF r =
  let ids = F.rcast r :: F.Record ss
  in flip fmap classifiers $ \c -> (ids F.<+> c) F.<+> newDataF c r
{-# INLINEABLE reshapeRowSimple #-}

reshapeRowSimpleOnOne :: forall ss c ts ds. (ss F.⊆ ts, V.KnownField c)
                      => [V.Snd c] -- ^ list of classifier values
                      -> (V.Snd c -> F.Record ts -> F.Record ds) -- ^ map classifier values to new shape
                      -> F.Record ts -- ^ original data
                      -> [F.Record (ss V.++ '[c] V.++ ds)] -- ^ reshaped data
reshapeRowSimpleOnOne classifiers newDataF =
  let toRec x = V.Field x V.:& V.RNil
  in reshapeRowSimple @ss @ts @'[c] (fmap toRec classifiers) (newDataF . F.rgetField @c)
{-# INLINEABLE reshapeRowSimpleOnOne #-}

--
-- | This type holds default values for the DefaultField and DefaultRecord classes to use. You can set a default with @Just@ or leave
-- the value in that type field alone with @Nothing@
data FieldDefaults =
  FieldDefaults
  {
    boolDefault :: Maybe Bool
  , intDefault :: Maybe Int
  , doubleDefault :: Maybe Double
  , textDefault :: Maybe T.Text
  }

-- | Optionally set a (Maybe :. ElfField) to a default value, d, replacing @Nothing@ with @Just d@
class DefaultField a where
  defaultField :: (V.KnownField t, V.Snd t ~ a) => FieldDefaults -> (Maybe F.:. F.ElField) t -> (Maybe F.:. F.ElField) t

instance DefaultField Bool where
  defaultField d x = V.Compose $ fmap V.Field $ (V.getField <$> V.getCompose x) <|> boolDefault d

instance DefaultField Int where
  defaultField d x = V.Compose $ fmap V.Field $ (V.getField <$> V.getCompose x) <|> intDefault d

instance DefaultField Double where
  defaultField d x = V.Compose $ fmap V.Field $ (V.getField <$> V.getCompose x) <|> doubleDefault d

instance DefaultField T.Text where
  defaultField d x = V.Compose $ fmap V.Field $ (V.getField <$> V.getCompose x) <|> textDefault d

-- | apply defaultField to each field of a Rec (Maybe :. ElField)
class DefaultRecord rs where
  defaultRecord :: FieldDefaults -> F.Rec (Maybe F.:. F.ElField) rs -> F.Rec (Maybe F.:. F.ElField) rs

instance DefaultRecord '[] where
  defaultRecord _ V.RNil = V.RNil

instance (DefaultRecord rs, V.KnownField t, DefaultField (V.Snd t)) => DefaultRecord  (t ': rs) where
  defaultRecord d (x V.:& xs) = defaultField d x V.:& defaultRecord d xs

-- for aggregations

newtype BinaryFunction a = BinaryFunction { appBinaryFunction :: a -> a -> a }

bfApply :: forall (rs :: [(Symbol, Type)]). (V.RMap (V.Unlabeled rs), V.RApply (V.Unlabeled rs), V.StripFieldNames rs)
         => F.Rec BinaryFunction (V.Unlabeled rs) -> (F.Record rs -> F.Record rs -> F.Record rs)
bfApply binaryFunctions xs ys = V.withNames $ V.rapply applyLHS (V.stripNames ys) where
  applyLHS = V.rzipWith (\bf ia -> V.Lift (V.Identity . appBinaryFunction bf (V.getIdentity ia) . V.getIdentity)) binaryFunctions (V.stripNames xs)
{-# INLINEABLE bfApply #-}

bfPlus :: Num a => BinaryFunction a
bfPlus = BinaryFunction (+)
{-# INLINEABLE bfPlus #-}

bfLHS :: BinaryFunction a
bfLHS = BinaryFunction const
{-# INLINEABLE bfLHS #-}

bfRHS :: BinaryFunction a
bfRHS = BinaryFunction $ \ _ x -> x
{-# INLINEABLE bfRHS #-}
