import           Criterion.Main

import           Bench.MapReduce                ( benchMapReduce )

benches :: [Benchmark]
benches = [benchMapReduce]

main = do
  defaultMain benches

