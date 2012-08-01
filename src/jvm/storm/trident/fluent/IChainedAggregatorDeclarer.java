package storm.trident.fluent;

import storm.trident.Stream;

public interface IChainedAggregatorDeclarer {
    Stream chainEnd();
}
