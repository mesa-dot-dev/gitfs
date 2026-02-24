(ns bench.dsl
  (:require [babashka.fs :as fs]
            [babashka.process :as proc]
            [cheshire.core :as json]))

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
  (let [args (transient ["hyperfine" "--export-json" (str json-path)])]
    ;; simple k/v flags
    (doseq [[k flag] flag-map]
      (when-let [v (get opts k)]
        (conj! args flag)
        (conj! args (str v))))
    ;; hook flags (fn-or-string)
    (doseq [[k flag] hook-flags]
      (when-let [v (get opts k)]
        (conj! args flag)
        (conj! args (hook-arg dir v))))
    ;; -L parameter-list: expects [var-name values-string]
    (when-let [[var-name values] (:parameter-list opts)]
      (conj! args "-L")
      (conj! args (str var-name))
      (conj! args (str values)))
    ;; -N no-shell (bare flag)
    (when (:no-shell opts)
      (conj! args "-N"))
    ;; append the commands to benchmark
    (doseq [cmd commands]
      (conj! args (str cmd)))
    (persistent! args)))
