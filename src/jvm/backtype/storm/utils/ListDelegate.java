package backtype.storm.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class ListDelegate implements List<Object> {
    private List<Object> _delegate;
    
    public void setDelegate(List<Object> delegate) {
        _delegate = delegate;
    }
    
    @Override
    public int size() {
        return _delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return _delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return _delegate.contains(o);
    }

    @Override
    public Iterator<Object> iterator() {
        return _delegate.iterator();
    }

    @Override
    public Object[] toArray() {
        return _delegate.toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        return _delegate.toArray(ts);
    }

    @Override
    public boolean add(Object e) {
        return _delegate.add(e);
    }

    @Override
    public boolean remove(Object o) {
        return _delegate.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> clctn) {
        return _delegate.containsAll(clctn);
    }

    @Override
    public boolean addAll(Collection<? extends Object> clctn) {
        return _delegate.addAll(clctn);
    }

    @Override
    public boolean addAll(int i, Collection<? extends Object> clctn) {
        return _delegate.addAll(i, clctn);
    }

    @Override
    public boolean removeAll(Collection<?> clctn) {
        return _delegate.removeAll(clctn);
    }

    @Override
    public boolean retainAll(Collection<?> clctn) {
        return _delegate.retainAll(clctn);
    }

    @Override
    public void clear() {
        _delegate.clear();
    }

    @Override
    public Object get(int i) {
        return _delegate.get(i);
    }

    @Override
    public Object set(int i, Object e) {
        return _delegate.set(i, e);
    }

    @Override
    public void add(int i, Object e) {
        _delegate.add(i, e);
    }

    @Override
    public Object remove(int i) {
        return _delegate.remove(i);
    }

    @Override
    public int indexOf(Object o) {
        return _delegate.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return _delegate.lastIndexOf(o);
    }

    @Override
    public ListIterator<Object> listIterator() {
        return _delegate.listIterator();
    }

    @Override
    public ListIterator<Object> listIterator(int i) {
        return _delegate.listIterator(i);
    }

    @Override
    public List<Object> subList(int i, int i1) {
        return _delegate.subList(i, i1);
    }
    
}
