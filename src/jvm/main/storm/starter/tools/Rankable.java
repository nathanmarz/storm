package storm.starter.tools;

public interface Rankable extends Comparable<Rankable> {

    Object getObject();

    long getCount();

}
