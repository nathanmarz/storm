package storm.starter.tools;

public interface Rankable extends Comparable<Rankable> {

  Object getObject();

  long getCount();

  /**
   * Note: We do not defensively copy the object wrapped by the Rankable.  It is passed as is.
   *
   * @return a defensive copy
   */
  Rankable copy();
}
