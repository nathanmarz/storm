package storm.trident.operation;

import storm.trident.Stream;


public interface Assembly {
    Stream apply(Stream input);
}
