(ns bench.dsl
  (:require [babashka.fs :as fs]))

(defn- fn->script-file
  "Serialize a quoted Clojure form to a temp .clj file in `dir`.
   Returns the absolute path string. The file starts with a bb shebang
   so hyperfine can execute it directly."
  [dir form]
  (let [f (fs/file dir (str (gensym "hook-") ".clj"))]
    (spit (str f)
          (str "#!/usr/bin/env bb\n"
               (pr-str form)
               "\n"))
    (fs/set-posix-file-permissions f "rwxr-xr-x")
    (str f)))

(defn- hook-arg
  "If `v` is a list/seq, serialize to a script file in `dir` and return
   the path. If it's a string, return as-is."
  [dir v]
  (if (or (list? v) (seq? v))
    (fn->script-file dir v)
    (str v)))

(def ^:private flag-map
  {:warmup   "-w"
   :min-runs "-m"
   :runs     "-r"
   :shell    "-S"})

(def ^:private hook-flags
  {:setup    "-s"
   :prepare  "-p"
   :conclude "-C"
   :cleanup  "-c"})

(defn- build-args
  "Translate an option map + commands into a hyperfine argv vector.
   `dir` is the temp directory for script files.
   `json-path` is where --export-json will write."
  [dir json-path opts commands]
  (-> ["hyperfine" "--export-json" (str json-path)]
      (into (mapcat (fn [[k flag]]
                      (when-let [v (get opts k)]
                        [flag (str v)]))
                    flag-map))
      (into (mapcat (fn [[k flag]]
                      (when-let [v (get opts k)]
                        [flag (hook-arg dir v)]))
                    hook-flags))
      (cond->
        (:parameter-list opts)
        (into (let [[var-name values] (:parameter-list opts)]
                ["-L" (str var-name) (str values)]))

        (:no-shell opts)
        (conj "-N"))
      (into (map str commands))))
