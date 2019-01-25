{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE ApplicativeDo #-}
module Frames.KMeans
  (
    kMeans
  , forgyCentroids
  , partitionCentroids
  , partitionCentroids'
  , euclidSq
  , l1dist
  , linfdist
  ) where

import qualified Frames.Aggregations as FA
import qualified Frames.Transform as FT
import qualified Math.Rescale as MR

import qualified Control.Foldl        as FL
import           Control.Lens         ((^.))
import qualified Control.Lens         as L
import           Data.Traversable    (sequenceA)
import           Control.Monad.State (evalState)
import           Data.Functor.Identity (Identity (Identity), runIdentity)
import qualified Data.Foldable     as Foldable
import qualified Data.List            as List
import qualified Data.Map             as M
import           Data.Maybe           (fromMaybe, isJust, catMaybes, fromJust)
import           Data.Text            (Text)
import qualified Data.Text            as T
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Curry     as V
import qualified Data.Vinyl.Functor   as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.XRec as V
import qualified Data.Vinyl.Class.Method as V
import qualified Data.Vinyl.Core      as V
import           Data.Vinyl.Lens      (type (∈))
import           Data.Kind            (Type,Constraint)
import           Data.Profunctor      as PF
import           Frames               ((:.), (&:))
import qualified Frames               as F
import qualified Frames.CSV           as F
import qualified Frames.ShowCSV           as F
import qualified Frames.Melt           as F
import qualified Frames.InCore        as FI
import qualified Pipes                as P
import qualified Pipes.Prelude        as P
import           Control.Arrow (second)
import           Data.Proxy (Proxy(..))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import           GHC.TypeLits (KnownSymbol)
import           Data.Random.Source.PureMT as R
import           Data.Random as R
import           Data.Function (on)


-- k-means
-- use the weights when computing the centroid location
-- initialize with random centers (Forgy) for now.

data Weighted a w = Weighted { dimension :: Int, location :: a -> U.Vector Double, weight :: a -> w } 
newtype Cluster a = Cluster { members :: [a] } deriving (Eq, Show)
newtype Clusters a = Clusters (V.Vector (Cluster a))  deriving (Show, Eq)
newtype Centroids = Centroids { centers :: V.Vector (U.Vector Double) } deriving (Show)
type Distance = U.Vector Double -> U.Vector Double -> Double

emptyCluster :: Cluster a 
emptyCluster = Cluster []

-- compute some initial random locations
-- TODO: use normal dist around mean and std-dev
forgyCentroids :: forall x y w f m. (F.AllConstrained (FA.DataFieldOf [x,y,w]) '[x, y, w], Foldable f, R.MonadRandom m)
               => Int -- number of clusters
               -> f (F.Record '[x,y,w])
               -> m [U.Vector Double]
forgyCentroids n dataRows = do
  let h = fromMaybe (0 :: Double) . fmap realToFrac   
      (xMin, xMax, yMin, yMax) = FL.fold ((,,,)
                                           <$> PF.dimap (F.rgetField @x) h FL.minimum
                                           <*> PF.dimap (F.rgetField @x) h FL.maximum
                                           <*> PF.dimap (F.rgetField @y) h FL.minimum
                                           <*> PF.dimap (F.rgetField @y) h FL.maximum) dataRows
      uniformPair = do
        ux <- R.uniform xMin xMax -- TODO: this is not a good way to deal with the Maybe here
        uy <- R.uniform yMin yMax
        return $ U.fromList [ux,uy]
  R.sample $ mapM (const uniformPair) $ replicate n ()

partitionCentroids :: forall x y w f. (F.AllConstrained (FA.DataFieldOf [x,y,w]) '[x, y, w], Foldable f)
                   => Int
                   -> f (F.Record '[x,y,w])
                   -> Identity [U.Vector Double]
partitionCentroids k dataRows = Identity $ fmap (fst . centroid (weighted2DRecord (Proxy @[x,y,w]))) $ go vs
  where go l = case List.splitAt n l of
          (vs', []) -> [vs']
          (vs', vss) -> vs' : go vss
        n = (List.length vs + k - 1) `div` k
        vs = FL.fold FL.list dataRows
        
weighted2DRecord :: forall x y w rs. ( FA.DataField x, FA.DataField y, FA.DataField w
                                     , F.ElemOf rs x, F.ElemOf rs y, F.ElemOf rs w)
                 => Proxy '[x,y,w] -> Weighted (F.Record rs) (FA.FType w)
weighted2DRecord _ =
  let getX = realToFrac . F.rgetField @x
      getY = realToFrac . F.rgetField @y
      makeV r = U.fromList [getX r, getY r]
  in Weighted 2 makeV (F.rgetField @w)

-- compute the centroid of the data.  Useful for the kMeans algo and computing the centroid of the final clusters
centroid :: forall g w a. (Foldable g, Real w) => Weighted a w -> g a -> (U.Vector Double, w)
centroid weighted as =
  let addOne :: (U.Vector Double, w) -> a -> (U.Vector Double, w)
      addOne (sumV, sumW) x =
        let w = weight weighted $ x
            v = location weighted $ x
        in (U.zipWith (+) sumV (U.map (* (realToFrac w)) v), sumW + w)
      finishOne (sumV, sumW) = U.map (/(realToFrac sumW)) sumV
  in FL.fold ((,) <$> FL.Fold addOne (U.replicate (dimension weighted) 0, 0) finishOne <*> FL.premap (weight weighted) FL.sum) as

-- | The euclidean distance without taking the final square root
--   This would waste cycles without changing the behavior of the algorithm
euclidSq :: Distance
euclidSq v1 v2 = U.sum $ U.zipWith diffsq v1 v2
  where diffsq a b = (a-b)^(2::Int)
{-# INLINE euclidSq #-}

-- | L1 distance of two vectors: d(v1, v2) = sum on i of |v1_i - v2_i|
l1dist :: Distance
l1dist v1 v2 = U.sum $ U.zipWith diffabs v1 v2
  where diffabs a b = abs (a - b)
{-# INLINE l1dist #-}

-- | L-inf distance of two vectors: d(v1, v2) = max |v1_i - v2_i]
linfdist :: Distance
linfdist v1 v2 = U.maximum $ U.zipWith diffabs v1 v2
  where diffabs a b = abs (a - b)
{-# INLINE linfdist #-}

type WithScaledCols rs = rs V.++ [FA.DblX, FA.DblY]
type WithScaled rs = F.Record (WithScaledCols rs)

-- TODOS:
-- Fix scaling.
kMeans :: forall rs ks x y w m f. ( FA.ThreeDTransformable rs ks x y w
                                  , F.ElemOf [FA.DblX,FA.DblY,w] w
                                  , F.ElemOf rs x
                                  , F.ElemOf rs y
                                  , F.ElemOf rs w
                                  , F.ElemOf (WithScaledCols rs) w
                                  , F.ElemOf (WithScaledCols rs) FA.DblX
                                  , F.ElemOf (WithScaledCols rs) FA.DblY
                                  , Monad m
                                  , Eq (F.Record (rs V.++ [FA.DblX, FA.DblY])))
       => Proxy ks
       -> Proxy '[x,y,w]
       -> FL.Fold (F.Record [x,y,w]) (MR.ScaleAndUnscale (FA.FType x))
       -> FL.Fold (F.Record [x,y,w]) (MR.ScaleAndUnscale (FA.FType y))
       -> Int
       -> (Int -> [F.Record [FA.DblX, FA.DblY, w]] -> m [U.Vector Double])  -- initial centroids
       -> Distance
       -> FL.FoldM m (F.Record rs) (F.FrameRec (ks V.++ [x,y,w]))
kMeans proxy_ks _ sunXF sunYF numClusters makeInitial distance =
  let toRecord :: (FA.FType x, FA.FType y, FA.FType w) -> F.Record [x,y,w] 
      toRecord (x, y, w) = x &: y &: w &: V.RNil 
      computeOne = kMeansOne' sunXF sunYF numClusters makeInitial (weighted2DRecord (Proxy @[FA.DblX, FA.DblY, w])) distance
  in FA.aggregateAndAnalyzeEachM' proxy_ks (fmap (fmap toRecord) . computeOne)

kMeansOne :: forall x y w f m. (FA.ThreeColData x y w, Foldable f, Functor f, Monad m)
          => FL.Fold (F.Record [x,y,w]) (MR.ScaleAndUnscale (FA.FType x))
          -> FL.Fold (F.Record [x,y,w]) (MR.ScaleAndUnscale (FA.FType y))
          -> Int 
          -> (Int -> f (F.Record '[FA.DblX,FA.DblY,w]) -> m [U.Vector Double])  -- initial centroids, monadic because may need randomness
          -> Weighted (F.Record '[FA.DblX,FA.DblY,w]) (FA.FType w) 
          -> Distance
          -> f (F.Record '[x,y,w])
          -> m [(FA.FType x, FA.FType y, FA.FType w)]
kMeansOne sunXF sunYF numClusters makeInitial weighted distance dataRows = do
  let (sunX, sunY) = FL.fold ((,) <$> sunXF <*> sunYF) dataRows
      scaledRows = fmap (V.runcurryX (\x y w -> (MR.from sunX) x &: (MR.from sunY) y &: w &: V.RNil)) dataRows  
  initial <- makeInitial numClusters scaledRows 
  let initialCentroids = Centroids $ V.fromList $ initial
      (Clusters clusters) = weightedKMeans initialCentroids weighted distance scaledRows
      fix :: (U.Vector Double, FA.FType w) -> (FA.FType x, FA.FType y, FA.FType w)
      fix (v, wgt) = ((MR.backTo sunX) (v U.! 0), (MR.backTo sunY) (v U.! 1), wgt)
  return $ V.toList $ fmap (fix . centroid weighted . members) clusters


kMeansOne' :: forall rs x y w f m. (FA.ThreeColData x y w, Foldable f, Functor f, Monad m, F.ElemOf rs x, F.ElemOf rs y, F.ElemOf rs w
                                   , [FA.DblX, FA.DblY,w] F.⊆ WithScaledCols rs
                                   , Eq (WithScaled rs))
          => FL.Fold (F.Record [x,y,w]) (MR.ScaleAndUnscale (FA.FType x))
          -> FL.Fold (F.Record [x,y,w]) (MR.ScaleAndUnscale (FA.FType y))
          -> Int 
          -> (Int -> f (F.Record '[FA.DblX, FA.DblY, w]) -> m [U.Vector Double])  -- initial centroids, monadic because may need randomness
          -> Weighted (WithScaled rs) (FA.FType w) 
          -> Distance
          -> f (F.Record rs)
          -> m [(FA.FType x, FA.FType y, FA.FType w)]
kMeansOne' sunXF sunYF numClusters makeInitial weighted distance dataRows = do
  let (sunX, sunY) = FL.fold ((,) <$> sunXF <*> sunYF) (fmap (F.rcast @[x,y,w]) dataRows)
      addX = FT.recordSingleton @FA.DblX . MR.from sunX . F.rgetField @x
      addY = FT.recordSingleton @FA.DblY . MR.from sunY . F.rgetField @y
      addXY r = addX r F.<+> addY r
      plusScaled = fmap (FT.mutate addXY) dataRows
  initial <- makeInitial numClusters $ fmap F.rcast plusScaled -- here we can throw away the other cols
  let initialCentroids = Centroids $ V.fromList $ initial
      (Clusters clusters) = weightedKMeans initialCentroids weighted distance plusScaled -- here we can't 
      fix :: (U.Vector Double, FA.FType w) -> (FA.FType x, FA.FType y, FA.FType w)
      fix (v, wgt) = ((MR.backTo sunX) (v U.! 0), (MR.backTo sunY) (v U.! 1), wgt)
  return $ V.toList $ fmap (fix . centroid weighted . members) clusters

    
weightedKMeans :: forall a w f. (Foldable f, Real w, Eq a)
               => Centroids -- initial guesses at centers 
               -> Weighted a w -- location/weight from data
               -> Distance
               -> f a
               -> Clusters a 
weightedKMeans initial weighted distF as = 
  -- put them all in one cluster just to start.  Doesn't matter since we have initial centroids
  let k = V.length (centers initial) -- number of clusters
      d = U.length (V.head $ centers initial) -- number of dimensions
      clusters0 = Clusters (V.fromList $ Cluster (Foldable.toList as) : (List.replicate (k - 1) emptyCluster))
      nearest :: Centroids -> a -> Int
      nearest cs a = V.minIndexBy (compare `on` distF (location weighted a)) (centers cs)
      updateClusters :: Centroids -> Clusters a -> Clusters a
      updateClusters centroids (Clusters oldCs) =
        let doOne :: [(Int, a)] -> Cluster a -> [(Int, a)]
            doOne soFar (Cluster as) = FL.fold (FL.Fold (\l x -> (nearest centroids x, x) : l) soFar id) as
            repackage :: [(Int, a)] -> Clusters a
            repackage = Clusters . V.unsafeAccum (\(Cluster l) x -> Cluster (x : l)) (V.replicate k emptyCluster)
        in FL.fold (FL.Fold doOne [] repackage) oldCs
      centroids :: Clusters a -> Centroids
      centroids (Clusters cs) = Centroids $ fmap (fst. centroid weighted . members) cs
      doStep cents clusters = let newClusters = updateClusters cents clusters in (newClusters, centroids newClusters) 
      go oldClusters (newClusters, centroids) =
        case (oldClusters == newClusters) of
          True -> newClusters
          False -> go newClusters (doStep centroids newClusters)
  in go clusters0 (doStep initial clusters0)

  
