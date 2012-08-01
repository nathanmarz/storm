package storm.trident.util;

import java.io.Serializable;


public class IndexedEdge<T> implements Comparable, Serializable {
    public T source;
    public T target;
    public int index;
    
    public IndexedEdge(T source, T target, int index) {
        this.source = source;
        this.target = target;
        this.index = index;
    }

    @Override
    public int hashCode() {
        return 13* source.hashCode() + 7 * target.hashCode() + index;
    }

    @Override
    public boolean equals(Object o) {
        IndexedEdge other = (IndexedEdge) o;
        return source.equals(other.source) && target.equals(other.target) && index == other.index;
    }

    @Override
    public int compareTo(Object t) {
        IndexedEdge other = (IndexedEdge) t;
        return index - other.index;
    }
}
