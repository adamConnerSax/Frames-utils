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
--{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE TupleSections #-}
module Frames.KMeans
  (
    kMeans
  , kMeansWithClusters -- this one returns the centroid and the list of clustered rows
  , clusteredRows -- turn the output of the above into one set of labeled rows
  , IsCentroid, ClusterId, MarkLabel
  , forgyCentroids
  , partitionCentroids
  , euclidSq
  , l1dist
  , linfdist
  ) where

import qualified Frames.Aggregations as FA
import qualified Frames.Transform as FT
import qualified Math.Rescale as MR
import qualified System.PipesLogger as SL

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
-- sometimes we get fewer than we expect.

data Weighted a w = Weighted { dimension :: Int, location :: a -> U.Vector Double, weight :: a -> w } 
newtype Cluster a = Cluster { members :: [a] } deriving (Eq, Show)
newtype Clusters a = Clusters (V.Vector (Cluster a))  deriving (Show, Eq)
newtype Centroids = Centroids { centers :: V.Vector (U.Vector Double) } deriving (Show)
type Distance = U.Vector Double -> U.Vector Double -> Double

emptyCluster :: Cluster a 
emptyCluster = Cluster []

-- compute some initial random locations
-- TODO: use normal dist around mean and std-dev
forgyCentroids :: forall x y w f m. (F.AllConstrained (FA.RealFieldOf [x,y,w]) '[x, y, w], Foldable f, R.MonadRandom m)
               => Int -- number of clusters
               -> f (F.Record '[x,y,w])
               -> m [U.Vector Double]
forgyCentroids n dataRows = do
  let h = id -- realToFrac --fromMaybe (0 :: Double) . fmap realToFrac   
      (xMean, xStd, yMean, yStd) = FL.fold ((,,,)
                                           <$> PF.dimap (realToFrac . F.rgetField @x) h FL.mean
                                           <*> PF.dimap (realToFrac . F.rgetField @x) h FL.std
                                           <*> PF.dimap (realToFrac . F.rgetField @y) h FL.mean
                                           <*> PF.dimap (realToFrac . F.rgetField @y) h FL.std) dataRows
      normalPair = do
        ux <- R.normal xMean xStd -- TODO: this is not a good way to deal with the Maybe here
        uy <- R.normal yMean yStd
        return $ U.fromList [ux,uy]
  R.sample $ mapM (const normalPair) $ replicate n ()

partitionCentroids :: forall x y w f. (F.AllConstrained (FA.RealFieldOf [x,y,w]) '[x, y, w], Foldable f, Show (FA.FType w))
                   => Int
                   -> f (F.Record '[x,y,w])
                   -> [U.Vector Double]
partitionCentroids k dataRows = catMaybes $ fmap (fmap fst . centroid (weighted2DRecord (Proxy @[x,y,w]))) $ go vs
  where go l = case List.splitAt n l of
          (vs', []) -> [vs']
          (vs', vss) -> vs' : go vss
        n = (List.length vs + k - 1) `div` k
        vs = FL.fold FL.list dataRows
        
weighted2DRecord :: forall x y w rs. ( FA.RealField x, FA.RealField y, FA.RealField w
                                     , F.ElemOf rs x, F.ElemOf rs y, F.ElemOf rs w)
                 => Proxy '[x,y,w] -> Weighted (F.Record rs) (FA.FType w)
weighted2DRecord _ =
  let getX = realToFrac . F.rgetField @x
      getY = realToFrac . F.rgetField @y
      makeV r = U.fromList [getX r, getY r]
  in Weighted 2 makeV (F.rgetField @w)

-- compute the centroid of the data.  Useful for the kMeans algo and computing the centroid of the final clusters
centroid :: forall g w a. (Foldable g, Real w, Show w, Show (g (U.Vector Double, w)), Functor g)
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

type WithScaledCols rs = rs V.++ [FA.DblX, FA.DblY]
type WithScaled rs = F.Record (WithScaledCols rs)

kMeans :: forall rs ks x y w m f. ( FA.ThreeDTransformable rs ks x y w
                                  , F.ElemOf [FA.DblX,FA.DblY,w] w
                                  , F.ElemOf rs x
                                  , F.ElemOf rs y
                                  , F.ElemOf rs w
                                  , F.ElemOf (WithScaledCols rs) w
                                  , F.ElemOf (WithScaledCols rs) FA.DblX
                                  , F.ElemOf (WithScaledCols rs) FA.DblY
                                  , Show (FA.FType w)
                                  , Monad m
                                  , Eq (F.Record (rs V.++ [FA.DblX, FA.DblY])))
       => Proxy ks
       -> Proxy '[x,y,w]
       -> FL.Fold (F.Record [x,w]) (MR.ScaleAndUnscale (FA.FType x))
       -> FL.Fold (F.Record [y,w]) (MR.ScaleAndUnscale (FA.FType y))
       -> Int
       -> (Int -> [F.Record [FA.DblX, FA.DblY, w]] -> m [U.Vector Double])  -- initial centroids
       -> Distance
       -> FL.FoldM (SL.Logger m) (F.Record rs) (F.FrameRec (ks V.++ [x,y,w]))
kMeans proxy_ks _ sunXF sunYF numClusters makeInitial distance =
  let toRecord :: (FA.FType x, FA.FType y, FA.FType w) -> F.Record [x,y,w] 
      toRecord (x, y, w) = x &: y &: w &: V.RNil 
      computeOne = kMeansOne sunXF sunYF numClusters makeInitial (weighted2DRecord (Proxy @[FA.DblX, FA.DblY, w])) distance . fmap (F.rcast @[x,y,w])
  in FA.aggregateAndAnalyzeEachM' proxy_ks (fmap (fmap toRecord) . computeOne)

kMeansOne :: forall x y w f m. (FA.ThreeColData x y w, Foldable f, Functor f, Monad m, Show (FA.FType w))
          => FL.Fold (F.Record [x,w]) (MR.ScaleAndUnscale (FA.FType x))
          -> FL.Fold (F.Record [y,w]) (MR.ScaleAndUnscale (FA.FType y))
          -> Int 
          -> (Int -> f (F.Record '[FA.DblX,FA.DblY,w]) -> m [U.Vector Double])  -- initial centroids, monadic because may need randomness
          -> Weighted (F.Record '[FA.DblX,FA.DblY,w]) (FA.FType w) 
          -> Distance
          -> f (F.Record '[x,y,w])
          -> SL.Logger m [(FA.FType x, FA.FType y, FA.FType w)]
kMeansOne sunXF sunYF numClusters makeInitial weighted distance dataRows = SL.wrapPrefix "KMeansOne" $ do
  let (sunX, sunY) = FL.fold ((,) <$> FL.premap F.rcast sunXF <*> FL.premap F.rcast sunYF) dataRows
      scaledRows = fmap (V.runcurryX (\x y w -> (MR.from sunX) x &: (MR.from sunY) y &: w &: V.RNil)) dataRows  
  initial <- SL.liftLog $ makeInitial numClusters scaledRows 
  let initialCentroids = Centroids $ V.fromList $ initial
  (Clusters clusters) <- fst <$> weightedKMeans initialCentroids weighted distance scaledRows
  let fix :: (U.Vector Double, FA.FType w) -> (FA.FType x, FA.FType y, FA.FType w)
      fix (v, wgt) = ((MR.backTo sunX) (v U.! 0), (MR.backTo sunY) (v U.! 1), wgt)
  return $ catMaybes $ V.toList $ fmap (fmap fix . centroid weighted . members) clusters -- we drop empty clusters ??

kMeansWithClusters :: forall rs ks x y w m f. ( FA.ThreeDTransformable rs ks x y w
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
                                              , Monad m
                                              , Show (FA.FType w)
                                              , Eq (F.Record (rs V.++ [FA.DblX, FA.DblY])))
                   => Proxy ks
                   -> Proxy '[x,y,w]
                   -> FL.Fold (F.Record [x,w]) (MR.ScaleAndUnscale (FA.FType x))
                   -> FL.Fold (F.Record [y,w]) (MR.ScaleAndUnscale (FA.FType y))
                   -> Int
                   -> (Int -> [F.Record [FA.DblX, FA.DblY, w]] -> m [U.Vector Double])  -- initial centroids
                   -> Distance
                   -> FL.FoldM (SL.Logger m) (F.Record rs) (M.Map (F.Record ks) [((FA.FType x, FA.FType y, FA.FType w), [F.Record rs])])
kMeansWithClusters proxy_ks _ sunXF sunYF numClusters makeInitial distance =
  let toRecord :: (FA.FType x, FA.FType y, FA.FType w) -> F.Record [x,y,w] 
      toRecord (x, y, w) = x &: y &: w &: V.RNil 
      computeOne = kMeansOneWithClusters sunXF sunYF numClusters makeInitial (weighted2DRecord (Proxy @[FA.DblX, FA.DblY, w])) distance 
  in FL.FoldM (\m -> return . FA.aggregateGeneral V.Identity (F.rcast @ks) (flip (:)) [] m) (return M.empty) (sequence . fmap computeOne)

kMeansOneWithClusters :: forall rs x y w f m. ( FA.ThreeColData x y w, Foldable f, Functor f, Monad m, F.ElemOf rs x, F.ElemOf rs y, F.ElemOf rs w
                                              , [FA.DblX, FA.DblY,w] F.⊆ WithScaledCols rs
                                              , rs F.⊆ WithScaledCols rs
                                              , Eq (WithScaled rs)
                                              , V.RMap (rs V.++ '[FA.DblX, FA.DblY])
                                              , V.ReifyConstraint Show F.ElField (rs V.++ '[FA.DblX, FA.DblY])
                                              , V.RecordToList (rs V.++ '[FA.DblX, FA.DblY])
                                              , Show (FA.FType w)
                                              , Monad m)
                      => FL.Fold (F.Record [x,w]) (MR.ScaleAndUnscale (FA.FType x))
                      -> FL.Fold (F.Record [y,w]) (MR.ScaleAndUnscale (FA.FType y))
                      -> Int 
                      -> (Int -> f (F.Record '[FA.DblX, FA.DblY, w]) -> m [U.Vector Double])  -- initial centroids, monadic because may need randomness
                      -> Weighted (WithScaled rs) (FA.FType w) 
                      -> Distance
                      -> f (F.Record rs)
                      -> SL.Logger m [((FA.FType x, FA.FType y, FA.FType w), [F.Record rs])]
kMeansOneWithClusters sunXF sunYF numClusters makeInitial weighted distance dataRows = SL.wrapPrefix "kMeansOneWithClusters" $ do  
  let (sunX, sunY) = FL.fold ((,) <$> FL.premap F.rcast sunXF <*> FL.premap F.rcast sunYF) (fmap (F.rcast @[x,y,w]) dataRows)
      addX = FT.recordSingleton @FA.DblX . MR.from sunX . F.rgetField @x
      addY = FT.recordSingleton @FA.DblY . MR.from sunY . F.rgetField @y
      addXY r = addX r F.<+> addY r
      plusScaled = fmap (FT.mutate addXY) dataRows
  initial <- SL.liftLog $ makeInitial numClusters $ fmap F.rcast plusScaled -- here we can throw away the other cols
  let initialCentroids = Centroids $ V.fromList $ initial
  (Clusters clusters, iters) <- weightedKMeans initialCentroids weighted distance plusScaled -- here we can't 
  let fix :: (U.Vector Double, FA.FType w) -> (FA.FType x, FA.FType y, FA.FType w)
      fix (v, wgt) = ((MR.backTo sunX) (v U.! 0), (MR.backTo sunY) (v U.! 1), wgt)
      clusterOut (Cluster m) = case List.length m of
                                 0 -> Nothing
                                 _ -> fmap (, fmap (F.rcast @rs) m) (fix <$> centroid weighted m)
      allClusters = V.toList $ fmap clusterOut clusters 
  let result = catMaybes allClusters
      nullClusters = List.length allClusters - List.length result
  SL.log SL.Info $ "Required " <> (T.pack $ show iters) <> " iterations to converge."
  if (nullClusters > 0)
    then SL.log SL.Warning $ (T.pack $ show nullClusters) <> " null clusters dropped."
    else SL.log SL.Info "All clusters have at least one member."
  return result

type IsCentroid = "is_centroid" F.:-> Bool
type ClusterId = "cluster_id" F.:-> Int
type MarkLabel = "mark_label" F.:-> Text

type ClusteredRow ks x y w = F.Record (ks V.++ [IsCentroid, ClusterId, MarkLabel] V.++ [x,y,w])

clusteredRows :: forall ks rs x y w. (FA.RealField x, FA.RealField y, FA.RealField w
                                     , ClusteredRow ks x y w ~ F.Record (ks V.++ [IsCentroid, ClusterId, MarkLabel, x ,y ,w])
                                     , F.ElemOf rs x
                                     , F.ElemOf rs y
                                     , F.ElemOf rs w)
              => Proxy '[x,y,w]
              -> (F.Record rs -> Text) -- label for each row
              -> M.Map (F.Record ks) [((FA.FType x, FA.FType y, FA.FType w), [F.Record rs])]
              -> [ClusteredRow ks x y w]
clusteredRows _ labelF m =
  let makeXYW :: (FA.FType x, FA.FType y, FA.FType w) -> F.Record [x,y,w]
      makeXYW (x,y,w) = x &: y &: w &: V.RNil
      idAndLabel :: Bool -> Int -> Text -> F.Record [IsCentroid,ClusterId, MarkLabel]
      idAndLabel ic cId l = ic &: cId &: l &: V.RNil
      doCentroid :: F.Record ks -> Int -> (FA.FType x, FA.FType y, FA.FType w) -> ClusteredRow ks x y w
      doCentroid k cId xyw = k F.<+> (idAndLabel True cId "") F.<+> makeXYW xyw
      doClusteredRow :: F.Record ks -> Int -> F.Record rs -> ClusteredRow ks x y w
      doClusteredRow k cId xs = k F.<+> (idAndLabel False cId (labelF xs)) F.<+> (F.rcast @[x,y,w] xs)
      doCluster :: Int -> F.Record ks -> ((FA.FType x, FA.FType y, FA.FType w),[F.Record rs]) -> [ClusteredRow ks x y w]
      doCluster cId k (centroid, rows) = doCentroid k cId centroid : (fmap (doClusteredRow k cId) rows)
      doListOfClusters :: F.Record ks -> [((FA.FType x, FA.FType y, FA.FType w),[F.Record rs])] -> [ClusteredRow ks x y w]
      doListOfClusters k l = List.concat $ fmap (\(cId,cluster) -> doCluster cId k cluster) $ List.zip [1..] l
  in List.concat $ fmap (\(k,loc) -> doListOfClusters k loc) $ M.toList m

    
weightedKMeans :: forall a w f m. (Show a, Monad m, Foldable f, Real w, Eq a, Show w)
               => Centroids -- initial guesses at centers 
               -> Weighted a w -- location/weight from data
               -> Distance
               -> f a
               -> SL.Logger m (Clusters a, Int)
weightedKMeans initial weighted distF as = SL.wrapPrefix "weightedKMeans" $ do
  -- put them all in one cluster just to start.  Doesn't matter since we have initial centroids
  let k = V.length (centers initial) -- number of clusters
      d = U.length (V.head $ centers initial) -- number of dimensions
      clusters0 = Clusters (V.fromList $ Cluster (Foldable.toList as) : (List.replicate (k - 1) emptyCluster))
      nearest :: Centroids -> a -> Int -- compute the nearest centroid to a and return the index to it in the Vector (centers cs)
      nearest cs a = V.minIndexBy (compare `on` distF (location weighted a)) (centers cs)
      updateClusters :: Centroids -> Clusters a -> Clusters a -- compute new clusters
      updateClusters centroids (Clusters oldCs) =
        -- Given a list [(closestIndex, pt)], a vector of centroids, a Cluster of pts, compute the closest centroid for each pt and add them each to the list 
        let doOneCluster :: [(Int, a)] -> Cluster a -> [(Int, a)] 
            doOneCluster soFar (Cluster as) = FL.fold (FL.Fold (\l x -> (nearest centroids x, x) : l) soFar id) as
            repackage :: [(Int, a)] -> Clusters a -- put things into clusters by index
            repackage = Clusters . V.accum (\(Cluster l) x -> Cluster (x : l)) (V.replicate k emptyCluster) -- x ~ a
        in FL.fold (FL.Fold doOneCluster [] repackage) oldCs
      newCentroids :: Clusters a -> Centroids
      newCentroids (Clusters cs) = Centroids $ (V.mapMaybe (fmap fst . centroid weighted . members) cs)
      go n oldClusters newClusters = do
--        let centroids' = newCentroids newClusters
--        SL.log SL.Info $ T.pack $ "centroids=" ++ show centroids'
        case (oldClusters == newClusters) of
          True -> return $ (newClusters, n)
          False -> go (n+1) newClusters $ updateClusters (newCentroids newClusters)  newClusters
--  SL.log SL.Info $ T.pack $ "centroids0=" ++ show initial        
  go 0 clusters0 (updateClusters initial clusters0)

  
