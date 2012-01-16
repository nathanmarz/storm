/**
<h1>De-duplication Framework</h1>
<ul>
  <li><a href="#overview">Overview</a></li>
  <li><a href="#interface">Interface</a></li>
  <li><a href="#usage">How to Use</a></li>
  <li><a href="#implementation">Implementation</a></li>
</ul>

<h2><a href="#overview">Overview</a></h2>
<p>This package provides a framework for de-duplication in Storm.
</p>

<p>The core of the framework consist of
  <ul>
    <li>Simple to use Java API that is similar to original API, including
        IDedupSpout, IDedupBolt, IDedupContext and DedupTopologyBuilder.
    </li>
    <li>Easy to understand and extend de-duplication mechanism.
    </li>
  </ul>
</p>

<h2><a href="#interface">Interface</a></h2>

<p>The Java API
  <ul>
    <li>{@link storm.dedup.IDedupSpout} 
        User spout should implement this interface. 
        The main method is declareOutputFields and nextTuple.
    </li>
    <li>{@link storm.dedup.IDedupBolt} 
        User bolt should implement this interface. 
        The main method is declareOutputFields and execute.
    </li>
    <li>{@link storm.dedup.IDedupContext} 
        User spout/bolt can use it to perform the operations
      <ul>
        <li>get configuration through getConf()</li>
        <li>set or get persistent state through setState()/getState()</li>
        <li>emit tuple through emit()</li>
      </ul>
    </li>
    <li>{@link storm.dedup.IStateStore} 
        The storage to persistent user and system state. A default HBase
        implementation {@link storm.dedup.HBaseStateStore} is provided.
    </li>
  </ul>
</p>

<h2><a href="#usage">How to Use</a></h2>

<p>A example {@link storm.dedup.example.WordTopTopology} 
   is provided in the package. The key points to use the framework
  <ol>
    <li>implement IDedupSpout to create spout</li>
    <li>implement IDedupBolt to create bolt</li>
    <li>use IDedupContext in spout/bolt to emit tuple and store state</li>
    <li>use DedupTopologyBuilder to create a de-duplication topology</li>
    <li>use StormSubmitter.submitTopology() to submit topology</li>
  </ol>
</p>
  
<h2><a href="#implementation">Implementation</a></h2>

<p>The implementation details of the de-duplication mechanism:
  <ul>
    <li>DedupSpoutExecutor/DedupSpoutContext is the real Strom spout and 
        it wrap user spout that implementes IDedupSpout. 
        And DedupBoltExecutor/DedupBoltContext is the same.</li>
    <li>DedupSpoutExecutor/DedupBoltExecutor add a special field _TUPLE_ID_ to 
        each user stream. DedupSpoutContext/DedupBoltContext append 
        a unique tuple id to each user tuple.</li>
    <li>DedupSpoutExecutor add a special dedup stream _DEDUP_STREAM_ID_ to each 
        user spout's output stream. DedupTopologyBuilder set each bolt 
        allGrouping the special dedup stream _DEDUP_STREAM_ID_</li>
    <li><b>tuple processing</b>
        <ul>
          <li>DedupSpoutContext use (stormid+componentid+taskid+globalid) to be 
              the tuple id for user spout's output tuple. The globalid is a 
              long and increase from 0 and will be persistent in state store.
              DedupSpoutContext use tuple id as the message id which is used 
              by Storm message gurantting to track spout's output tuple. 
              DedupSpoutContext save (tupleid => tuple) mapping in the state 
              store for each spout's output tuple before emit the tuple to Storm. 
          </li>
          <li>DedupBoltContext use (input tuple id+stormid+componentid+taskid+
              output index) to be the tuple id for user bolt's output tuple. 
              Output index is start from 0 for each input tuple and increase
              for each output tuple. DedupBoltContext also save
              (tupleid => output) tuple mapping in the state store.
          </li>
        </ul>
        <b>ack/fail processing</b>
        <ul>
          <li>If the tuple is successfully processed, DedupSpoutExecutor.ack() 
              is invoked by Storm. Then DedupSpoutContext delete the 
              (tupleid => tuple mapping) and emit a NOTICE tuple just contain
              the tupleid to the special dedup stream. Then each bolt will
              receive the NOTICE tuple and DedupBoltContext just delete the
              (tupleid => tuple) mapping related to the tupleid.
              </li>
          <li>If the tuple is NOT successfully processed after timeout, 
              DedupSpoutExecutor.fail() is invoked by Storm. 
              Then DedupSpoutContext check the (tupleid => tuple) mapping to 
              get the original tuple and re-emit it to original stream.
              Then some bolt may receive duplicated tuple. DedupBoltContext 
              check its (tupleid => tuple) mapping to see whether the input 
              tuple is duplicated. If it's a duplicated tuple, DedupBoltContext
              just re-emit it. If it's NOT a duplicated tuple, DedupBoltContext
              invoke user bolt's to emit new output tuples.
              </li>
        </ul>
         </li>
    <li></li>
  </ul>
</p>

*/

package storm.dedup;
