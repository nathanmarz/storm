package backtype.storm.task;

/**
 * This output collector exposes the API for emitting tuples from an IRichBolt.
 * This is the core API for emitting tuples. For a simpler API, and a more restricted
 * form of stream processing, see IBasicBolt and BasicOutputCollector.
 */
public abstract class OutputCollector extends BoltEmitter implements IOutputCollector {

}
