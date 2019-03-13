import           Criterion.Main

import           Bench.MapReduce                ( benchMapReduceIO )

benchesIO :: IO [Benchmark]
benchesIO = sequence [benchMapReduceIO]

main = do
  benches <- benchesIO
  defaultMain benches

