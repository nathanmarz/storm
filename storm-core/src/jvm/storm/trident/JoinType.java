package storm.trident;

import java.util.Arrays;
import java.util.List;

public enum JoinType {
    INNER,
    OUTER;
    
    public static List<JoinType> mixed(JoinType... types) {
        return Arrays.asList(types);
    }
}