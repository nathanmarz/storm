package storm.kafka.trident.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Date: 27/04/2013
 * Time: 09:56
 */
public class PrintFunction extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String currentString = tuple.getString(0);
		System.out.println(currentString);
		collector.emit(new Values(currentString));
	}

}
