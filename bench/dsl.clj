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
