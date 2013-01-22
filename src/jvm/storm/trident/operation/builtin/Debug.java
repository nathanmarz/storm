package storm.trident.operation.builtin;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class Debug extends BaseFilter {
    private final String name;

    public Debug() {
        name = "DEBUG: ";
    }

    public Debug(String name) {
        this.name = "DEBUG(" + name + "): ";
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        System.out.println(name + tuple.toString());
        return true;
    }
}
