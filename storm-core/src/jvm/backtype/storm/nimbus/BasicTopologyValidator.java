package backtype.storm.nimbus;

import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BasicTopologyValidator implements ITopologyValidator {
    @Override
    public void validate(String topologyName, Map topologyConf,
            StormTopology topology) throws InvalidTopologyException {
        validateConfigurationTypes(topologyConf);
    }

    protected Class getCommonAncestorClass(Iterable args) {
        Iterator it = args.iterator();
        if (! it.hasNext()) {
            return Object.class;
        }
        Class toRet = it.next().getClass();

        while (it.hasNext()) {
            while (! toRet.isInstance(it.next())) {
                toRet = toRet.getSuperclass();
            }
        }
        return toRet;
    }

    protected boolean areObjectsInstances(Class cls, Iterable<?> vals) {
        if (Object.class == cls) {
            return true;
        }
        for (Object val : vals) {
            if (cls.isInstance(val)) {
                continue;
            }
            return false;
        }
        return true;
    }

    protected boolean areObjectsInstances(Map<?, ?> defConf, Map<?, ?> conf, Object key) {
        final Object defVal = defConf.get(key);
        final Object confVal = conf.get(key);
        Class defClass = defVal.getClass();

        if (! defClass.isInstance(confVal)) {
            return false;
        }

        if (defVal instanceof Map) {
            return areObjectsInstances(getCommonAncestorClass(((Map)defVal).keySet()),
                    ((Map) confVal).keySet());
        }

        if (defVal instanceof Iterable) {
            return areObjectsInstances(getCommonAncestorClass((Iterable) defVal),
                    (Iterable) confVal);
        }

        return true;
    }

    protected void validateConfigurationTypes(Map<?, ?> conf) throws InvalidTopologyException {
        Map<?, ?> defaultConf = Utils.readDefaultConfig();

        for (Object key : conf.keySet()) {
            if (! defaultConf.containsKey(key)) {
                continue;
            }
            if (areObjectsInstances(defaultConf, conf, key)) {
                continue;
            }

            // The type did not match.
            String defaultClassName = defaultConf.getClass().getName();
            String confClassName = conf.getClass().getName();
            throw new InvalidTopologyException("Key '" + key + "' is expected to be a '"
                    + defaultClassName + "' but was a '" + confClassName + "'.");
        }
    }
}
