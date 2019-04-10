import           Criterion.Main

-- moved to map-reduce-folds package
--import           Bench.MapReduce                ( benchMapReduceIO )

benchesIO = sequence [] -- should have whatever bench routines come from imported bench modules

main = benchesIO >>= defaultMain

