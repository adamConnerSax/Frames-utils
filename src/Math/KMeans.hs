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
  (
    Weighted (..)
  , Cluster (..)
  , Clusters (..)
  , Centroids (..)
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
  ) where

import qualified Control.Monad.Freer                  as FR
import qualified Control.Monad.Freer.Logger           as Log


import qualified Control.Foldl                        as FL
import qualified Data.Foldable                        as Foldable
import           Data.Function                        (on)
import qualified Data.List                            as List
import           Data.Maybe                           (catMaybes, fromMaybe)
import           Data.Random                          as R
import qualified Data.Random.Distribution.Categorical as R
import qualified Data.Text                            as T
import           Data.Traversable                     (sequenceA, traverse)
import qualified Data.Vector                          as V
import qualified Data.Vector.Unboxed                  as U


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


weightedKMeans :: forall a w f effs. (Show a, Log.LogWithPrefixes effs, Foldable f, Real w, Eq a, Show w)
               => Centroids -- initial guesses at centers
               -> Weighted a w -- location/weight from data
               -> Distance
               -> f a
               -> FR.Eff effs (Clusters a, Int)
weightedKMeans initial weighted distF as = Log.wrapPrefix "weightedKMeans" $ do
  -- put them all in one cluster just to start.  Doesn't matter since we have initial centroids
  let k = V.length (centers initial) -- number of clusters
      clusters0 = Clusters (V.fromList $ Cluster (Foldable.toList as) : (List.replicate (k - 1) emptyCluster))
      nearest :: Centroids -> a -> Int -- compute the nearest centroid to a and return the index to it in the Vector (centers cs)
      nearest cs a = V.minIndexBy (compare `on` distF (location weighted a)) (centers cs)
      updateClusters :: Centroids -> Clusters a -> Clusters a -- compute new clusters
      updateClusters centroids (Clusters oldCs) =
        -- Given a list [(closestIndex, pt)], a vector of centroids, a Cluster of pts, compute the closest centroid for each pt and add them each to the list
        let doOneCluster :: [(Int, a)] -> Cluster a -> [(Int, a)]
            doOneCluster soFar (Cluster as') = FL.fold (FL.Fold (\l x -> (nearest centroids x, x) : l) soFar id) as'
            repackage :: [(Int, a)] -> Clusters a -- put things into clusters by index
            repackage = Clusters . V.accum (\(Cluster l) x -> Cluster (x : l)) (V.replicate k emptyCluster) -- x ~ a
        in FL.fold (FL.Fold doOneCluster [] repackage) oldCs
      newCentroids :: Clusters a -> Centroids
      newCentroids (Clusters cs) = Centroids $ (V.mapMaybe (fmap fst . centroid weighted . members) cs)
      go n oldClusters newClusters = do
--        let centroids' = newCentroids newClusters
--        Log.log Log.Diagnositc $ T.pack $ "centroids=" ++ show centroids'
        case (oldClusters == newClusters) of
          True -> return $ (newClusters, n)
          False -> go (n+1) newClusters $ updateClusters (newCentroids newClusters)  newClusters
--  Log.log Log.Diagnostic $ T.pack $ "centroids0=" ++ show initial
  go 0 clusters0 (updateClusters initial clusters0)


-- compute some initial random locations
-- TODO: use normal dist around mean and std-dev
-- precondition: all input vectors are same length
-- we could enforce with sized vectors.  But not today.
forgyCentroids :: (Foldable f, Functor f, R.MonadRandom m)
               => Int -- number of clusters
               -> f (U.Vector Double)
               -> m [U.Vector Double]
forgyCentroids m dataRows = do
  let n = fromMaybe 0 $ FL.fold FL.minimum $ fmap U.length dataRows -- safe
      foldsAt :: Int -> FL.Fold (U.Vector Double) (Double, Double)
      foldsAt l = FL.premap (U.! l) $ (,) <$> FL.mean <*> FL.std
      folds :: FL.Fold (U.Vector Double) [(Double, Double)] = sequenceA $ fmap foldsAt [0..(n-1)]
      stats = FL.fold folds dataRows
      distVec = U.fromList <$> traverse (\(mean,sd) -> R.normal mean sd) stats
  R.sample $ mapM (const distVec) $ replicate m ()

unweightedVec :: Int -> Weighted (U.Vector Double) Double
unweightedVec n = Weighted n id (const 1)

partitionCentroids :: (Real w, Foldable f) => Weighted a w -> Int -> f a -> [U.Vector Double]
partitionCentroids weighted k dataRows = fmap fst $ catMaybes $ fmap (centroid weighted) $ go vs
  where go l = case List.splitAt n l of
          (vs', [])  -> [vs']
          (vs', vss) -> vs' : go vss
        n = (List.length vs + k - 1) `div` k
        vs = FL.fold FL.list dataRows


-- Choose first from input at random
-- Now, until you have enough
-- compute the minimum distance of every point to any of the current centroids
-- choose a new one by using those minimum distances as probabilities
-- NB: no already chosen center can be chosen since it has distance 0 from an existing one
-- should this factor in weights?  E.g., make pts with higher weights more likely?
kMeansPPCentroids :: (R.MonadRandom m, Foldable f)
                   => Distance
                   -> Int
                   -> f (U.Vector Double)
                   -> m [U.Vector Double]
kMeansPPCentroids distF k dataRows = R.sample $ do
  let pts = FL.fold FL.list dataRows -- is this the best structure here?
      shortestToAny :: [U.Vector Double] -> U.Vector Double -> Double
      shortestToAny cs c = minimum $ fmap (distF c) cs
      minDists :: [U.Vector Double] -> [Double]
      minDists cs = fmap (shortestToAny cs) pts -- shortest distance to any of the cs
      probs :: [U.Vector Double] -> [Double]
      probs cs =
        let dists = minDists cs
            s = FL.fold FL.sum dists -- can this be 0?  If m inputs overlap and we want >= k-m centers
        in fmap (/s) dists
      distributionNew :: [U.Vector Double] -> R.RVar (U.Vector Double)
      distributionNew cs = R.categorical $ List.zip (probs cs) pts
      go :: [U.Vector Double] -> R.RVar [U.Vector Double]
      go cs = if List.length cs == k
        then return cs
        else do
          newC <- distributionNew cs
          go (newC : cs)
  firstIndex <- R.uniform 0 (List.length pts - 1)
  go [pts List.!! firstIndex]


kMeansCostWeighted :: (Real w, Show w) => Distance -> Weighted a w -> Clusters a -> Double
kMeansCostWeighted distF weighted (Clusters cs) =
  let clusterCost (Cluster m) =
        let cm = centroid weighted m
            f c x = (realToFrac $ weight weighted x) * distF c (location weighted x)
        in case cm of
          Nothing -> 0
          Just c  -> FL.fold (FL.premap (f (fst c)) FL.sum) m
  in FL.fold (FL.premap clusterCost FL.sum) cs

-- compute the centroid of the data.  Useful for the kMeans algo and computing the centroid of the final clusters
centroid :: forall g w a. (Foldable g, Real w, Functor g)
  => Weighted a w -> g a -> Maybe (U.Vector Double, w)
centroid weighted as =
  let addOne :: (U.Vector Double, w) -> a -> (U.Vector Double, w)
      addOne (sumV, sumW) x =
        let w = weight weighted $ x
            v = location weighted $ x
        in (U.zipWith (+) sumV (U.map (* (realToFrac w)) v), sumW + w)
      finishOne (sumV, sumW) = if (sumW > 0) then Just (U.map (/(realToFrac sumW)) sumV, sumW) else Nothing
  in FL.fold (FL.Fold addOne (U.replicate (dimension weighted) 0, 0) finishOne) as


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


{-



weighted2DRecord :: forall x y w rs. ( FA.RealField x, FA.RealField y, FA.RealField w
                                     , F.ElemOf rs x, F.ElemOf rs y, F.ElemOf rs w)
                 => Weighted (F.Record rs) (FA.FType w)
weighted2DRecord =
  let getX = realToFrac . F.rgetField @x
      getY = realToFrac . F.rgetField @y
      makeV r = U.fromList [getX r, getY r]
  in Weighted 2 makeV (F.rgetField @w)




type WithScaledCols rs = rs V.++ [FA.DblX, FA.DblY]
type WithScaled rs = F.Record (WithScaledCols rs)

kMeans :: forall ks x y w rs effs. ( FA.ThreeDTransformable rs ks x y w
                                   , F.ElemOf [FA.DblX,FA.DblY,w] w
                                   , F.ElemOf rs x
                                   , F.ElemOf rs y
                                   , F.ElemOf rs w
                                   , F.ElemOf (WithScaledCols rs) w
                                   , F.ElemOf (WithScaledCols rs) FA.DblX
                                   , F.ElemOf (WithScaledCols rs) FA.DblY
                                   , Show (FA.FType w)
                                   , Eq (F.Record (rs V.++ [FA.DblX, FA.DblY]))
                                   , Log.LogWithPrefixes effs)
       => FL.Fold (F.Record [x,w]) (MR.ScaleAndUnscale (FA.FType x))
       -> FL.Fold (F.Record [y,w]) (MR.ScaleAndUnscale (FA.FType y))
       -> Int
       -> (Int -> [F.Record [FA.DblX, FA.DblY, w]] -> FR.Eff effs [U.Vector Double])  -- initial centroids
       -> Distance
       -> FL.FoldM (FR.Eff effs) (F.Record rs) (F.FrameRec (ks V.++ [x,y,w]))
kMeans sunXF sunYF numClusters makeInitial distance =
  let toRecord :: (FA.FType x, FA.FType y, FA.FType w) -> F.Record [x,y,w]
      toRecord (x, y, w) = x &: y &: w &: V.RNil
      computeOne = kMeansOne sunXF sunYF numClusters makeInitial (weighted2DRecord @FA.DblX @FA.DblY @w) distance . fmap (F.rcast @[x,y,w])
  in FA.aggregateAndAnalyzeEachM' @ks (fmap (fmap toRecord) . computeOne)

kMeansOne :: forall x y w f effs. ( FA.ThreeColData x y w
                                  , Foldable f
                                  , Functor f
                                  , Log.LogWithPrefixes effs
                                  , Show (FA.FType w))
          => FL.Fold (F.Record [x,w]) (MR.ScaleAndUnscale (FA.FType x))
          -> FL.Fold (F.Record [y,w]) (MR.ScaleAndUnscale (FA.FType y))
          -> Int
          -> (Int -> f (F.Record '[FA.DblX,FA.DblY,w]) -> FR.Eff effs [U.Vector Double])  -- initial centroids, monadic because may need randomness
          -> Weighted (F.Record '[FA.DblX,FA.DblY,w]) (FA.FType w)
          -> Distance
          -> f (F.Record '[x,y,w])
          -> FR.Eff effs [(FA.FType x, FA.FType y, FA.FType w)]
kMeansOne sunXF sunYF numClusters makeInitial weighted distance dataRows = Log.wrapPrefix "KMeansOne" $ do
  let (sunX, sunY) = FL.fold ((,) <$> FL.premap F.rcast sunXF <*> FL.premap F.rcast sunYF) dataRows
      scaledRows = fmap (V.runcurryX (\x y w -> (MR.from sunX) x &: (MR.from sunY) y &: w &: V.RNil)) dataRows
  initial <- makeInitial numClusters scaledRows
  let initialCentroids = Centroids $ V.fromList $ initial
  (Clusters clusters) <- fst <$> weightedKMeans initialCentroids weighted distance scaledRows
  let fix :: (U.Vector Double, FA.FType w) -> (FA.FType x, FA.FType y, FA.FType w)
      fix (v, wgt) = ((MR.backTo sunX) (v U.! 0), (MR.backTo sunY) (v U.! 1), wgt)
  return $ catMaybes $ V.toList $ fmap (fmap fix . centroid weighted . members) clusters -- we drop empty clusters ??

kMeansWithClusters :: forall ks x y w effs rs. ( FA.ThreeDTransformable rs ks x y w
                                               , F.ElemOf [FA.DblX,FA.DblY,w] w
                                               , F.ElemOf rs x
                                               , F.ElemOf rs y
                                               , F.ElemOf rs w
                                               , F.ElemOf (WithScaledCols rs) w
                                               , F.ElemOf (WithScaledCols rs) FA.DblX
                                               , F.ElemOf (WithScaledCols rs) FA.DblY
                                               , rs F.⊆ (WithScaledCols rs)
                                               , V.RMap (rs V.++ '[FA.DblX, FA.DblY])
                                               , V.ReifyConstraint Show F.ElField (rs V.++ '[FA.DblX, FA.DblY])
                                               , V.RecordToList (rs V.++ '[FA.DblX, FA.DblY])
                                               , Log.LogWithPrefixes effs
                                               , Show (FA.FType w)
                                               , Eq (F.Record (rs V.++ [FA.DblX, FA.DblY])))
                   => FL.Fold (F.Record [x,w]) (MR.ScaleAndUnscale (FA.FType x))
                   -> FL.Fold (F.Record [y,w]) (MR.ScaleAndUnscale (FA.FType y))
                   -> Int
                   -> Int
                   -> (Int -> [F.Record [FA.DblX, FA.DblY, w]] -> FR.Eff effs [U.Vector Double])  -- initial centroids
                   -> Distance
                   -> FL.FoldM (FR.Eff effs) (F.Record rs) (M.Map (F.Record ks) [((FA.FType x, FA.FType y, FA.FType w), [F.Record rs])])
kMeansWithClusters sunXF sunYF numClusters numTries makeInitial distance =
  let computeOne = kMeansOneWithClusters sunXF sunYF numClusters numTries makeInitial (weighted2DRecord @FA.DblX @FA.DblY @w) distance
  in FL.FoldM (\m -> return . FA.aggregateGeneral V.Identity (F.rcast @ks) (flip (:)) [] m) (return M.empty) (traverse computeOne)

kMeansOneWithClusters :: forall rs x y w f effs. ( FA.ThreeColData x y w
                                                 , Foldable f
                                                 , Functor f
                                                 , F.ElemOf rs x
                                                 , F.ElemOf rs y
                                                 , F.ElemOf rs w
                                                 , [FA.DblX, FA.DblY,w] F.⊆ WithScaledCols rs
                                                 , rs F.⊆ WithScaledCols rs
                                                 , Eq (WithScaled rs)
                                                 , V.RMap (rs V.++ '[FA.DblX, FA.DblY])
                                                 , V.ReifyConstraint Show F.ElField (rs V.++ '[FA.DblX, FA.DblY])
                                                 , V.RecordToList (rs V.++ '[FA.DblX, FA.DblY])
                                                 , Show (FA.FType w)
                                                 , Log.LogWithPrefixes effs)
                      => FL.Fold (F.Record [x,w]) (MR.ScaleAndUnscale (FA.FType x))
                      -> FL.Fold (F.Record [y,w]) (MR.ScaleAndUnscale (FA.FType y))
                      -> Int
                      -> Int
                      -> (Int -> f (F.Record '[FA.DblX, FA.DblY, w]) -> FR.Eff effs [U.Vector Double])  -- initial centroids, monadic because may need randomness
                      -> Weighted (WithScaled rs) (FA.FType w)
                      -> Distance
                      -> f (F.Record rs)
                      -> FR.Eff effs [((FA.FType x, FA.FType y, FA.FType w), [F.Record rs])]
kMeansOneWithClusters sunXF sunYF numClusters numTries makeInitial weighted distance dataRows = Log.wrapPrefix "kMeansOneWithClusters" $ do
  let (sunX, sunY) = FL.fold ((,) <$> FL.premap F.rcast sunXF <*> FL.premap F.rcast sunYF) (fmap (F.rcast @[x,y,w]) dataRows)
      addX = FT.recordSingleton @FA.DblX . MR.from sunX . F.rgetField @x
      addY = FT.recordSingleton @FA.DblY . MR.from sunY . F.rgetField @y
      addXY r = addX r F.<+> addY r
      plusScaled = fmap (FT.mutate addXY) dataRows
  initials <- mapM (const $ fmap (Centroids . V.fromList) $ makeInitial numClusters $ fmap F.rcast plusScaled) (V.replicate numTries ()) -- here we can throw away the other cols
--  let initialCentroids = Centroids $ V.fromList $ initial
  tries <- mapM (\cs -> weightedKMeans cs weighted distance plusScaled) initials -- here we can't
  let costs = fmap (kMeansCostWeighted distance weighted . fst) tries
  Log.logLE Log.Diagnostic $ "Costs: " <> (T.pack $ show costs)
  let (Clusters clusters, iters) = tries V.! (V.minIndex costs)
      fix :: (U.Vector Double, FA.FType w) -> (FA.FType x, FA.FType y, FA.FType w)
      fix (v, wgt) = ((MR.backTo sunX) (v U.! 0), (MR.backTo sunY) (v U.! 1), wgt)
      clusterOut (Cluster m) = case List.length m of
                                 0 -> Nothing
                                 _ -> fmap (, fmap (F.rcast @rs) m) (fix <$> centroid weighted m)
      allClusters = V.toList $ fmap clusterOut clusters
  let result = catMaybes allClusters
      nullClusters = List.length allClusters - List.length result
  Log.logLE Log.Diagnostic $ "Required " <> (T.pack $ show iters) <> " iterations to converge."
  if (nullClusters > 0)
    then Log.logLE Log.Warning $ (T.pack $ show nullClusters) <> " null clusters dropped."
    else Log.logLE Log.Diagnostic "All clusters have at least one member."
  return result

type IsCentroid = "is_centroid" F.:-> Bool
type ClusterId = "cluster_id" F.:-> Int
type MarkLabel = "mark_label" F.:-> Text

type ClusteredRow ks x y w = F.Record (ks V.++ [IsCentroid, ClusterId, MarkLabel] V.++ [x,y,w])

clusteredRows :: forall x y w ks rs. (FA.RealField x, FA.RealField y, FA.RealField w
                                     , ClusteredRow ks x y w ~ F.Record (ks V.++ [IsCentroid, ClusterId, MarkLabel, x ,y ,w])
                                     , F.ElemOf rs x
                                     , F.ElemOf rs y
                                     , F.ElemOf rs w)
              => (F.Record rs -> Text) -- label for each row
              -> M.Map (F.Record ks) [((FA.FType x, FA.FType y, FA.FType w), [F.Record rs])]
              -> [ClusteredRow ks x y w]
clusteredRows labelF m =
  let makeXYW :: (FA.FType x, FA.FType y, FA.FType w) -> F.Record [x,y,w]
      makeXYW (x,y,w) = x &: y &: w &: V.RNil
      idAndLabel :: Bool -> Int -> Text -> F.Record [IsCentroid,ClusterId, MarkLabel]
      idAndLabel ic cId l = ic &: cId &: l &: V.RNil
      doCentroid :: F.Record ks -> Int -> (FA.FType x, FA.FType y, FA.FType w) -> ClusteredRow ks x y w
      doCentroid k cId xyw = k F.<+> (idAndLabel True cId "") F.<+> makeXYW xyw
      doClusteredRow :: F.Record ks -> Int -> F.Record rs -> ClusteredRow ks x y w
      doClusteredRow k cId xs = k F.<+> (idAndLabel False cId (labelF xs)) F.<+> (F.rcast @[x,y,w] xs)
      doCluster :: Int -> F.Record ks -> ((FA.FType x, FA.FType y, FA.FType w),[F.Record rs]) -> [ClusteredRow ks x y w]
      doCluster cId k (ctrd, rows) = doCentroid k cId ctrd : (fmap (doClusteredRow k cId) rows)
      doListOfClusters :: F.Record ks -> [((FA.FType x, FA.FType y, FA.FType w),[F.Record rs])] -> [ClusteredRow ks x y w]
      doListOfClusters k l = List.concat $ fmap (\(cId,cluster) -> doCluster cId k cluster) $ List.zip [1..] l
  in List.concat $ fmap (\(k,loc) -> doListOfClusters k loc) $ M.toList m


-}
