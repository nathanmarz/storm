(ns backtype.storm.ui-test
  (:use [backtype.storm.ui core])
  (:use [clojure test])
  (:use [hiccup.core :only (html)])
)

(deftest test-javascript-escaped-in-action-buttons
  (let [expected-s "confirmAction('XX\\\"XX\\'XX\\\\XX\\/XX\\nXX', 'XX\\\"XX\\'XX\\\\XX\\/XX\\nXX', 'activate', false, 0)"
        malicious-js "XX\"XX'XX\\XX/XX
XX"
        result (topology-action-button malicious-js malicious-js
                                "Activate" "activate" false 0 true)
        onclick (:onclick (second result))]

    (is (= expected-s onclick)
        "Escapes quotes, slashes, back-slashes, and new-lines.")
  )
)

(deftest test-topology-link-escapes-content-html
  (let [topo-name "BOGUSTOPO"]
    (is (= (str "<a href=\"/topology/" topo-name "\">&lt;BLINK&gt;foobar</a>")
           (html (topology-link topo-name "<BLINK>foobar"))))
  )
)

(deftest test-component-link-escapes-content-html
  (let [topo-name "BOGUSTOPO"]
    (is (= (str "<a href=\"/topology/" topo-name "/component/%3CBLINK%3Ecomp-id\">&lt;BLINK&gt;comp-id</a>")
           (html (component-link topo-name "<BLINK>comp-id"))))
  )
)

; main-topology-summary-table
; submit topo name like "<BLINK>foobar"
; Load / and visually confirm the 'id' column does not blink for the topo.

; topology-summary-table
; submit topo name like "<BLINK>foobar"
; Load / and visually confirm the 'id' column does not blink for the topo or the name

; topology-summary-table
; submit topo name like "<BLINK>foobar"
; Load / and visually confirm the 'id' column does not blink for the topo or the name

; component-page
; recompile a topology (such as the ExclamationTopology from storm-starter) and hardcode bolt/spout names with '<blink>' 
; Load the bolt or spout component page and visually confirm the 'id' column does not blink the component name.

; bolt-input-summary-table
; recompile a topology (such as the ExclamationTopology from storm-starter) and hardcode bolt/spout names with '<blink>' 
;
