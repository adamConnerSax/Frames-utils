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
    mutate    
  , transform
  , transformMaybe
  , fieldEndo
  , recordSingleton
  , addColumn
  , dropColumn
  , replaceColumn
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

--import qualified Control.Newtype as N
import           Control.Applicative   ((<|>))
import           Data.Kind (Type)
import qualified Data.Text            as T
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Curry     as V
import           Data.Vinyl.TypeLevel as V --(type (++), Snd)
import           Data.Vinyl.Functor   (Lift(..), Identity(..), Compose(..))
import qualified Frames               as F
import           Frames.Melt          (RDeleteAll, ElemOf)

import           GHC.TypeLits         (KnownSymbol, Symbol)
import Unsafe.Coerce (unsafeCoerce)

-- |  mutation functions

-- | Type preserving single-field mapping
fieldEndo :: forall x rs. (V.KnownField x, ElemOf rs x) => (Snd x -> Snd x) -> F.Record rs -> F.Record rs
fieldEndo f r = F.rputField @x (f $ F.rgetField @x r) r

{-
f :: KnownSymbol s => (s,(a -> b)) -> F.Rec ((->) a) ['(s,b)]
f (_, fieldFunc) = fieldFunc V.:& V.RNil
-}

-- | replace subset with a calculated different set of fields
transform :: forall rs as bs. (as F.⊆ rs, RDeleteAll as rs F.⊆ rs)
             => (F.Record as -> F.Record bs) -> F.Record rs -> F.Record (RDeleteAll as rs V.++ bs)
transform f xs = F.rcast @(RDeleteAll as rs) xs `F.rappend` f (F.rcast xs)

transformMaybe :: forall rs as bs. (as F.⊆ rs, RDeleteAll as rs F.⊆ rs, V.RMap (RDeleteAll as rs))
               => (F.Record as -> F.Rec (Maybe F.:. F.ElField) bs) -> F.Record rs -> F.Rec (Maybe F.:. F.ElField) (RDeleteAll as rs V.++ bs)
transformMaybe f xs = V.rmap (Compose . Just) (F.rcast @(RDeleteAll as rs) xs) `V.rappend` f (F.rcast xs) 

-- | append calculated subset 
mutate :: forall rs bs. (F.Record rs -> F.Record bs) -> F.Record rs -> F.Record (rs V.++ bs)
mutate f xs = xs `F.rappend` f xs 

-- | Create a record with one field from a value.  Use a TypeApplication to choose the field.
recordSingleton :: forall af s a. (KnownSymbol s, af ~ '(s,a)) => a -> F.Record '[af]
recordSingleton a = a F.&: V.RNil


replaceColumn :: forall t t' rs. (V.KnownField t
                                 , V.KnownField t'
                                 , (RDelete t rs V.++ '[t']) F.⊆ (rs V.++ '[t'])
                                 , ElemOf rs t
                                 )
              => (V.Snd t -> V.Snd t')
              -> F.Record rs -> F.Record (RDelete t rs V.++ '[t'])
replaceColumn f = F.rcast @(RDelete t rs V.++ '[t']) . mutate (recordSingleton @t' . f . F.rgetField @t)


addOneFromOne :: forall t t' rs. (V.KnownField t, V.KnownField t', ElemOf rs t) => (V.Snd t -> V.Snd t') -> F.Record rs -> F.Record (t' ': rs)
addOneFromOne f xs = (f $ F.rgetField @t xs) F.&: xs

addOneFrom
  :: forall as t rs
     . (V.IsoXRec F.ElField as
       , V.KnownField t, as F.⊆ rs)
  => V.CurriedX V.ElField as (V.Snd t) -> F.Record rs -> F.Record (t ': rs) 
addOneFrom f xs = (V.runcurryX f $ F.rcast @as xs) F.&: xs 

addName :: forall t t' rs. (V.KnownField t, V.KnownField t', V.Snd t ~ V.Snd t', ElemOf rs t) => F.Record rs -> F.Record (t' ': rs)
addName = addOneFromOne @t @t' id 

addOneFromValue :: forall t rs. V.KnownField t => V.Snd t -> F.Record rs -> F.Record (t ': rs)
addOneFromValue = (F.&:)

-- | add a column
addColumn :: forall t bs. (V.KnownField t) => V.Snd t -> F.Record bs -> F.Record (t ': bs)
addColumn x r = recordSingleton x `V.rappend` r

-- | Drop a column from a record.  Just a specialization of rcast.
dropColumn :: forall x rs. (F.RDelete x rs F.⊆ rs) => F.Record rs -> F.Record (F.RDelete x rs)
dropColumn = F.rcast

-- | Drop a set of columns from a record. Just a specialization of rcast.
dropColumns :: forall xs rs. (RDeleteAll xs rs F.⊆ rs) => F.Record rs -> F.Record (RDeleteAll xs rs)
dropColumns = F.rcast

-- |  change a column "name" at the type level
retypeColumn :: forall x y rs. ( V.KnownField x
                               , V.KnownField y
                               , V.Snd x ~ V.Snd y
                               , ElemOf rs x
                               , F.RDelete x rs F.⊆ rs
--                               , Rename (Fst x) (Fst y) rs ~ (RDelete '(Fst x, Snd y) rs ++ '[ '(Fst y, Snd y)]))
                               )
  => F.Record rs -> F.Record (F.RDelete x rs V.++ '[y])
retypeColumn = transform @rs @'[x] @'[y] (\r -> (F.rgetField @x r F.&: V.RNil))


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


-- take a type-level-list of (fromName, toName, type) and use it to rename columns in suitably typed record
type family FromRecList (a :: [(Symbol, Symbol, Type)]) :: [(Symbol, Type)] where
  FromRecList '[] = '[]
  FromRecList ('(fs, ts, t) ': rs) = '(fs, t) ': FromRecList rs

type family ToRecList (a :: [(Symbol, Symbol, Type)]) :: [(Symbol, Type)] where
  ToRecList '[] = '[]
  ToRecList ('(fs, ts, t) ': rs) = '(ts, t) ': ToRecList rs  
  
class (FromRecList cs F.⊆ rs) => RetypeColumns (cs :: [(Symbol, Symbol, Type)]) (rs :: [(Symbol, Type)]) where
  retypeColumns :: (rs ~ (rs V.++ '[])) => F.Record rs -> F.Record (RDeleteAll (FromRecList cs) rs V.++ (ToRecList cs))

instance RetypeColumns '[] rs where
  retypeColumns = id

instance (RetypeColumns cs rs
         , V.KnownField '(fs, t)
         , V.KnownField '(ts, t)
         , ElemOf rs '(fs, t)
         , (RDelete '(fs, t) (RDeleteAll (FromRecList cs) rs V.++ ToRecList cs) V.++ '[ '(ts, t)])
         ~ (RDeleteAll (FromRecList cs) (RDelete '(fs, t) rs) V.++ ('(ts, t) ': ToRecList cs))
         , ElemOf (RDeleteAll (FromRecList cs) rs ++ ToRecList cs) '(fs, t)
         , (RDelete '(fs, t) (RDeleteAll (FromRecList cs) rs ++ ToRecList cs)) F.⊆ (RDeleteAll (FromRecList cs) rs ++ ToRecList cs)
         , Rename fs ts (RDeleteAll (FromRecList cs) rs ++ ToRecList cs) ~ (RDeleteAll (FromRecList cs) (RDelete '(fs, t) rs) ++ ('(ts, t) : ToRecList cs))
         )
    => RetypeColumns ('(fs, ts, t) ': cs) rs where
  retypeColumns = retypeColumn @'(fs, t) @'(ts, t) @(RDeleteAll (FromRecList cs) rs V.++ (ToRecList cs))  . retypeColumns @cs @rs

{-
-- take a type-level-list of (fromName, toName, type -> type) and use it to transform columns in suitably typed record
type family FromTList (a :: [(Symbol, Symbol, Type -> Type)]) :: [(Symbol, Type)] where
  FromRecList '[] = '[]
  FromRecList ('(fs, ts, x -> y) ': rs) = '(fs, x) ': FromRecList rs

type family ToTList (a :: [(Symbol, Symbol, Type -> Type)]) :: [(Symbol, Type)] where
  ToRecList '[] = '[]
  ToRecList ('(fs, ts, x -> y) ': rs) = '(ts, y) ': ToRecList rs  
  
class (FromRecList cs F.⊆ rs) => RetypeColumns (cs :: [(Symbol, Symbol, Type -> Type)]) (rs :: [(Symbol, Type)]) where
  retypeColumns :: (rs ~ (rs V.++ '[])) => F.Record rs -> F.Record (RDeleteAll (FromRecList cs) rs V.++ (ToRecList cs))

instance TransformColumns '[] rs where
  retypeColumns = id

instance (RetypeColumns cs rs
         )
    => RetypeColumns ('(fs, ts, t) ': cs) rs where
  retypeColumns = retypeColumn @'(fs, t) @'(ts, t) @(RDeleteAll (FromRecList cs) rs V.++ (ToRecList cs))  . retypeColumns @cs @rs
-}

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

reshapeRowSimpleOnOne :: forall ss c ts ds. (ss F.⊆ ts, V.KnownField c)
                      => [V.Snd c] -- ^ list of classifier values
                      -> (V.Snd c -> F.Record ts -> F.Record ds) -- ^ map classifier values to new shape
                      -> F.Record ts -- ^ original data
                      -> [F.Record (ss V.++ '[c] V.++ ds)] -- ^ reshaped data                
reshapeRowSimpleOnOne classifiers newDataF =
  let toRec x = V.Field x V.:& V.RNil 
  in reshapeRowSimple @ss @ts @'[c] (fmap toRec classifiers) (\fc -> newDataF (F.rgetField @c fc))

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
  defaultField d x = Compose $ fmap V.Field $ (fmap V.getField $ getCompose x) <|> (boolDefault d)
  
instance DefaultField Int where
  defaultField d x = Compose $ fmap V.Field $ (fmap V.getField $ getCompose x) <|> (intDefault d)
  
instance DefaultField Double where
  defaultField d x = Compose $ fmap V.Field $ (fmap V.getField $ getCompose x) <|> (doubleDefault d)

instance DefaultField T.Text where
  defaultField d x = Compose $ fmap V.Field $ (fmap V.getField $ getCompose x) <|> (textDefault d)

-- | apply defaultField to each field of a Rec (Maybe :. ElField)
class DefaultRecord rs where
  defaultRecord :: FieldDefaults -> F.Rec (Maybe F.:. F.ElField) rs -> F.Rec (Maybe F.:. F.ElField) rs

instance DefaultRecord '[] where
  defaultRecord _ V.RNil = V.RNil

instance (DefaultRecord rs, V.KnownField t, DefaultField (V.Snd t)) => DefaultRecord  (t ': rs) where
  defaultRecord d (x V.:& xs) = defaultField d x V.:& defaultRecord d xs 

-- for aggregations

newtype BinaryFunction a = BinaryFunction { appBinaryFunction :: a -> a -> a } 

bfApply :: forall (rs :: [(Symbol,*)]). (V.RMap (V.Unlabeled rs), V.RApply (V.Unlabeled rs), V.StripFieldNames rs)
         => F.Rec BinaryFunction (V.Unlabeled rs) -> (F.Record rs -> F.Record rs -> F.Record rs)
bfApply binaryFunctions xs ys = V.withNames $ V.rapply applyLHS (V.stripNames ys) where
  applyLHS = V.rzipWith (\bf ia -> Lift (\ib -> Identity $ appBinaryFunction bf (getIdentity ia) (getIdentity ib))) binaryFunctions (V.stripNames xs)

bfPlus :: Num a => BinaryFunction a
bfPlus = BinaryFunction (+)

bfLHS :: BinaryFunction a
bfLHS = BinaryFunction const

bfRHS :: BinaryFunction a
bfRHS = BinaryFunction $ flip const


{-
class MapCurried f as where
  mapCurried :: (a -> b) -> V.CurriedX f as a -> V.CurriedX f as b

instance MapCurried f '[] where
  mapCurried = id

instance MapCurried f as => MapCurried f (t ': as) where
  mapCurried fab cf = mapCurried @f @as fab . cf

rearrangeZipWith :: forall a b c as bs f. (MapCurried f as, MapCurried f bs)
                 => (a -> b -> c) -> V.CurriedX f as a -> V.CurriedX f bs b -> V.CurriedX f as (V.CurriedX f bs c)
rearrangeZipWith combine ca cb =
  let g :: x -> a -> (a, x)
      g x a = (a, x)
      x :: V.CurriedX f as (a, V.CurriedX f bs b)
      x = mapCurried @f @as (g cb) ca -- V.CurriedX f as (a, V.CurriedX f bs b)
      y = mapCurried @f @as (\(i, cb) -> mapCurried @f @bs (\j -> combine i j) cb) x
  in y

class UnNestCurried f as bs c where
  unNestCurried :: V.CurriedX f as (V.CurriedX f bs c) -> V.CurriedX f (as V.++ bs) c

instance (bs V.++ '[] ~ as) => UnNestCurried f '[] bs c where
  unNestCurried g = g

instance UnNestCurried f as bs c => UnNestCurried f (t': as) bs c where
  unNestCurried f = \x -> unNestCurried @f @as @bs @c (f x)  
  
zipCurriedWith :: forall f as bs a b c. (UnNestCurried f as bs c, MapCurried f as, MapCurried f bs)
  => (a -> b -> c) -> V.CurriedX f as a -> V.CurriedX f bs b -> V.CurriedX f (as V.++ bs) c
zipCurriedWith combine ca cb = unNestCurried @f @as @bs @c $ rearrangeZipWith @a @b @c @as @bs @f combine ca cb 

newtype ColMaker (as :: [(Symbol, Type)]) (t :: (Symbol, Type)) = ColMaker { unColMaker :: V.KnownField t => V.CurriedX F.ElField as (V.Snd t) }

addColMaker
  :: forall cs as bs t.
     (V.KnownField t
     , UnNestCurried F.ElField cs as (F.Record (t ': bs))
     , MapCurried F.ElField cs
     , MapCurried F.ElField as
     ) 
  => ColMaker cs t -> V.CurriedX F.ElField as (F.Record bs) -> V.CurriedX F.ElField (cs V.++ as) (F.Record (t ': bs))
addColMaker (ColMaker cf) =  zipCurriedWith @F.ElField @cs @as @(V.Snd t) @(F.Record bs) @(F.Record (t ': bs)) (\x rb -> x F.&: rb) cf

lastColMaker :: V.CurriedX F.ElField '[] (F.Record '[])
lastColMaker = V.RNil

(|++|) :: forall cs as bs t.
          (V.KnownField t
          , UnNestCurried F.ElField cs as (F.Record (t ': bs))
          , MapCurried F.ElField cs
          , MapCurried F.ElField as
          ) 
  => ColMaker cs t -> V.CurriedX F.ElField as (F.Record bs) -> V.CurriedX F.ElField (cs V.++ as) (F.Record (t ': bs))
(|++|) = addColMaker @cs @as @bs @t 

infixr 7 |++|

transformFromCurried
  :: forall qs as bs rs
     . (as F.⊆ rs
       , RDeleteAll as rs F.⊆ rs
       , qs F.⊆ (RDeleteAll as rs V.++ bs)
       , qs F.⊆ (rs V.++ bs)
       , V.IsoXRec F.ElField as
       )
  =>  V.CurriedX F.ElField as (F.Record bs) -> F.Record rs -> F.Record qs --(RDeleteAll as rs V.++ bs)
transformFromCurried cf =
  let g :: F.Record as -> F.Record bs
      g = V.runcurryX cf
  in  F.rcast . mutate (g . F.rcast)


cmPlusNonEmptyRL
  :: forall as bs as' bs' cs t.
     (as ~ (cs V.++ as')
     , bs ~ (t ': bs')
     , V.KnownField t
     , UnNestCurried F.ElField cs as' (F.Record bs)
     , MapCurried F.ElField cs
     , MapCurried F.ElField bs'
     , MapCurried F.ElField as'
     ) => ColMaker cs t -> ReplacerList as' bs' ->  V.CurriedX F.ElField as (F.Record bs)
cmPlusNonEmptyRL rh rl = addColMaker @cs @as' @bs' @t rh (buildReplacer rl) 

buildReplacer
  :: forall as bs.
  ( 
    MapCurried F.ElField bs
  , MapCurried F.ElField as  
  )
  => ReplacerList as bs -> V.CurriedX F.ElField as (F.Record bs)
buildReplacer RLNil = V.RNil
buildReplacer ((rh :: ColMaker cs t) `RLCons` (rl :: ReplacerList as' bs')) = cmPlusNonEmptyRL @as @bs @as' @bs' @cs @t rh rl

-}
