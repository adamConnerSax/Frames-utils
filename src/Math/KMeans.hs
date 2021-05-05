{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE ConstraintKinds           #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# LANGUAGE TupleSections             #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE UndecidableInstances      #-}
module Math.KMeans
  ( Weighted(..)
  , Cluster(..)
  , Clusters(..)
  , Centroids(..)
  , Distance
  , emptyCluster
  , unweightedVec
  , centroid
  , weightedKMeans
  , kMeansCostWeighted
  , forgyCentroids
  , partitionCentroids
  , kMeansPPCentroids
  , euclidSq
  , l1dist
  , linfdist
  )
where

import qualified Control.Foldl                 as FL
import qualified Data.Foldable                 as Foldable
import qualified Data.List                     as List
import           Data.Random                   as R
import qualified Data.Random.Distribution.Categorical
                                               as R

import qualified Data.Vector                   as V
import qualified Data.Vector.Unboxed           as U
import Data.List.NonEmpty ((<|))

-- k-means
-- use the weights when computing the centroid location
-- sometimes we get fewer than we expect.

data Weighted a w = Weighted { dimension :: Int, location :: a -> U.Vector Double, weight :: a -> w }
newtype Cluster a = Cluster { members :: [a] } deriving (Eq, Show)
newtype Clusters a = Clusters (V.Vector (Cluster a))  deriving (Show, Eq)
newtype Centroids = Centroids { centers :: V.Vector (U.Vector Double) } deriving (Show)
type Distance = U.Vector Double -> U.Vector Double -> Double

emptyCluster :: Cluster a
emptyCluster = Cluster []


weightedKMeans
  :: forall a w f m
   . (Show a, MonadIO m, Foldable f, Real w, Eq a, Show w)
  => Centroids -- initial guesses at centers
  -> Weighted a w -- location/weight from data
  -> Distance
  -> f a
  -> m (Clusters a, Int)
weightedKMeans initial weighted distF as = do
  -- put them all in one cluster just to start.  Doesn't matter since we have initial centroids
  let
    k         = V.length (centers initial) -- number of clusters
    clusters0 = Clusters
      ( V.fromList
      $ Cluster (Foldable.toList as)
      : List.replicate (k - 1) emptyCluster
      )
    nearest :: Centroids -> a -> Int -- compute the nearest centroid to a and return the index to it in the Vector (centers cs)
    nearest cs a =
      V.minIndexBy (compare `on` distF (location weighted a)) (centers cs)
    updateClusters :: Centroids -> Clusters a -> Clusters a -- compute new clusters
    updateClusters centroids (Clusters oldCs) =
      -- Given a list [(closestIndex, pt)], a vector of centroids, a Cluster of pts, compute the closest centroid for each pt and add them each to the list
      let doOneCluster :: [(Int, a)] -> Cluster a -> [(Int, a)]
          doOneCluster soFar (Cluster as') = FL.fold
            (FL.Fold (\l x -> (nearest centroids x, x) : l) soFar id)
            as'
          repackage :: [(Int, a)] -> Clusters a -- put things into clusters by index
          repackage = Clusters . V.accum (\(Cluster l) x -> Cluster (x : l))
                                         (V.replicate k emptyCluster) -- x ~ a
      in  FL.fold (FL.Fold doOneCluster [] repackage) oldCs
    newCentroids :: Clusters a -> Centroids
    newCentroids (Clusters cs) =
      Centroids $ V.mapMaybe (fmap fst . centroid weighted . members) cs
    go n oldClusters newClusters = do
--        let centroids' = newCentroids newClusters
--        Log.log Log.Diagnositc $ T.pack $ "centroids=" ++ show centroids'
      if oldClusters == newClusters
        then return (newClusters, n)
        else go (n + 1) newClusters
             $ updateClusters (newCentroids newClusters) newClusters
--  Log.log Log.Diagnostic $ T.pack $ "centroids0=" ++ show initial
  go 0 clusters0 (updateClusters initial clusters0)


-- compute some initial random locations
-- TODO: use normal dist around mean and std-dev
-- precondition: all input vectors are same length
-- we could enforce with sized vectors.  But not today.
forgyCentroids
  :: (Foldable f, Functor f, R.MonadRandom m)
  => Int -- number of clusters
  -> f (U.Vector Double)
  -> m [U.Vector Double]
forgyCentroids m dataRows = do
  let n = fromMaybe 0 $ FL.fold FL.minimum $ fmap U.length dataRows -- safe
      foldsAt :: Int -> FL.Fold (U.Vector Double) (Double, Double)
      foldsAt l = FL.premap (U.! l) $ (,) <$> FL.mean <*> FL.std
      folds :: FL.Fold (U.Vector Double) [(Double, Double)] =
        traverse foldsAt [0 .. (n - 1)]
      stats   = FL.fold folds dataRows
      distVec = U.fromList <$> traverse (uncurry normal) stats
  R.sample $ replicateM m distVec

unweightedVec :: Int -> Weighted (U.Vector Double) Double
unweightedVec n = Weighted n id (const 1)

partitionCentroids
  :: (Real w, Foldable f) => Weighted a w -> Int -> f a -> [U.Vector Double]
partitionCentroids weighted k dataRows =
  fmap fst $ catMaybes $ centroid weighted <$> go vs
 where
  go l = case List.splitAt n l of
    (vs', [] ) -> [vs']
    (vs', vss) -> vs' : go vss
  n  = (List.length vs + k - 1) `div` k
  vs = FL.fold FL.list dataRows


-- Choose first from input at random
-- Now, until you have enough
-- compute the minimum distance of every point to any of the current centroids
-- choose a new one by using those minimum distances as probabilities
-- NB: no already chosen center can be chosen since it has distance 0 from an existing one
-- should this factor in weights?  E.g., make pts with higher weights more likely?
kMeansPPCentroids
  :: (R.MonadRandom m, Foldable f)
  => Distance
  -> Int
  -> f (U.Vector Double)
  -> m [U.Vector Double]
kMeansPPCentroids distF k dataRows = R.sample $ do
  let pts = FL.fold FL.list dataRows -- is this the best structure here?
      neMinimum :: Ord a => NonEmpty a -> a
      neMinimum nel = foldl' (\m x -> if x < m then x else m) (head nel) (tail nel)
      shortestToAny :: NonEmpty (U.Vector Double) -> U.Vector Double -> Double
      shortestToAny cs c = neMinimum $ fmap (distF c) cs
      minDists :: NonEmpty (U.Vector Double) -> [Double]
      minDists cs = fmap (shortestToAny cs) pts -- shortest distance to any of the cs
      probs :: NonEmpty (U.Vector Double) -> [Double]
      probs cs =
        let dists = minDists cs
            s     = FL.fold FL.sum dists -- can this be 0?  If m inputs overlap and we want >= k-m centers
        in  fmap (/ s) dists
      distributionNew :: NonEmpty (U.Vector Double) -> R.RVar (U.Vector Double)
      distributionNew cs = R.categorical $ List.zip (probs cs) pts
      go :: NonEmpty (U.Vector Double) -> R.RVar [U.Vector Double]
      go cs = if length cs == k
        then return $ toList cs
        else do
          newC <- distributionNew cs
          go (newC <| cs)
  firstIndex <- R.uniform 0 (List.length pts - 1)
  go $ one $ pts List.!! firstIndex


kMeansCostWeighted
  :: (Real w, Show w) => Distance -> Weighted a w -> Clusters a -> Double
kMeansCostWeighted distF weighted (Clusters cs) =
  let clusterCost (Cluster m) =
        let cm = centroid weighted m
            f c x =
              realToFrac (weight weighted x) * distF c (location weighted x)
        in  case cm of
              Nothing -> 0
              Just c  -> FL.fold (FL.premap (f (fst c)) FL.sum) m
  in  FL.fold (FL.premap clusterCost FL.sum) cs

-- compute the centroid of the data.  Useful for the kMeans algo and computing the centroid of the final clusters
centroid
  :: forall g w a
   . (Foldable g, Real w, Functor g)
  => Weighted a w
  -> g a
  -> Maybe (U.Vector Double, w)
centroid weighted as
  = let
      addOne :: (U.Vector Double, w) -> a -> (U.Vector Double, w)
      addOne (sumV, sumW) x =
        let w = weight weighted x
            v = location weighted x
        in  (U.zipWith (+) sumV (U.map (* realToFrac w) v), sumW + w)
      finishOne (sumV, sumW) = if sumW > 0
        then Just (U.map (/ realToFrac sumW) sumV, sumW)
        else Nothing
    in
      FL.fold
        (FL.Fold addOne (U.replicate (dimension weighted) 0, 0) finishOne)
        as
{-# INLINE centroid #-}

-- | The euclidean distance without taking the final square root
--   This would waste cycles without changing the behavior of the algorithm
euclidSq :: Distance
euclidSq v1 v2 = U.sum $ U.zipWith diffsq v1 v2
  where diffsq a b = (a - b) ^ (2 :: Int)
{-# INLINE euclidSq #-}

-- | L1 distance of two vectors: d(v1, v2) = sum on i of |v1_i - v2_i|
l1dist :: Distance
l1dist v1 v2 = U.sum $ U.zipWith diffabs v1 v2 where diffabs a b = abs (a - b)
{-# INLINE l1dist #-}

-- | L-inf distance of two vectors: d(v1, v2) = max |v1_i - v2_i]
linfdist :: Distance
linfdist v1 v2 = U.maximum $ U.zipWith diffabs v1 v2
  where diffabs a b = abs (a - b)
{-# INLINE linfdist #-}
