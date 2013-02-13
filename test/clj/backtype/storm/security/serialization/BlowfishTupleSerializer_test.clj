(ns backtype.storm.security.serialization.BlowfishTupleSerializer-test
  (:use [
         clojure test])
  (:import [backtype.storm.security.serialization BlowfishTupleSerializer]
           [backtype.storm.utils ListDelegate]
           [com.esotericsoftware.kryo Kryo]
           [com.esotericsoftware.kryo.io Input Output]
  )
)

; Exceptions are getting wrapped in RuntimeException.  This might be due to
; CLJ-855.
(defn- unpack-runtime-exception [expression]
  (try (eval expression)
    nil
    (catch java.lang.RuntimeException gripe
      (throw (.getCause gripe)))
  )
)

(deftest test-constructor-throws-on-null-key
  (let [conf {}]
    (is (thrown? java.lang.RuntimeException
      (unpack-runtime-exception 
        '(new BlowfishTupleSerializer nil conf)))
      "Throws RuntimeException when no encryption key is given."
    )
  )
)

(use '[clojure.string :only (join split)])
(deftest test-encrypts-and-decrypts-message

  (let [
        test-text (str
"Tetraodontidae is a family of primarily marine and estuarine fish of the order"
" Tetraodontiformes. The family includes many familiar species, which are"
" variously called pufferfish, puffers, balloonfish, blowfish, bubblefish,"
" globefish, swellfish, toadfish, toadies, honey toads, sugar toads, and sea"
" squab.[1] They are morphologically similar to the closely related"
" porcupinefish, which have large external spines (unlike the thinner, hidden"
" spines of Tetraodontidae, which are only visible when the fish has puffed up)."
" The scientific name refers to the four large teeth, fused into an upper and"
" lower plate, which are used for crushing the shells of crustaceans and"
" mollusks, their natural prey."
)
        kryo (new Kryo)
        arbitrary-key "7dd6fb3203878381b08f9c89d25ed105"
        storm_conf {"topology.tuple.serializer.blowfish.key" arbitrary-key}
        writer-bts (new BlowfishTupleSerializer kryo storm_conf)
        reader-bts (new BlowfishTupleSerializer kryo storm_conf)
        buf-size 1024
        output (new Output buf-size buf-size)
        input (new Input buf-size)
        strlist (split test-text #" ")
        delegate (new ListDelegate)
       ]
    (-> delegate (.addAll strlist))
    (-> writer-bts (.write kryo output delegate))
    (-> input (.setBuffer (-> output (.getBuffer))))
    (is 
      (=
        test-text
        (join " " (map (fn [e] (str e)) 
          (-> reader-bts (.read kryo input ListDelegate) (.toArray))))
      )
      "Reads a string encrypted by another instance with a shared key"
    )
  )
)
