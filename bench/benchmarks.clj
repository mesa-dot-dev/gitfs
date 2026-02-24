(ns bench.benchmarks
  (:require [bench.dsl :as dsl]))

(defn run-sample
  "Sample benchmark to verify the DSL works."
  []
  (dsl/hyperfine {:warmup 1 :runs 5}
                 "echo hello"))
