package backtype.storm.utils;

public class WindowedTimeThrottler {
    long _windowMillis;
    int _maxAmt;
    long _windowStartTime;
    int _windowEvents = 0;
    
    public WindowedTimeThrottler(Number windowMillis, Number maxAmt) {
        _windowMillis = windowMillis.longValue();
        _maxAmt = maxAmt.intValue();
        _windowStartTime = System.currentTimeMillis();
    }
    
    public boolean isThrottled() {
        resetIfNecessary();
        return _windowEvents >= _maxAmt;
    }
    
    //returns void if the event should continue, false if the event should not be done
    public void markEvent() {
        resetIfNecessary();
        _windowEvents++;
        
    }
    
    private void resetIfNecessary() {
        long now = System.currentTimeMillis();
        if(now - _windowStartTime > _windowMillis) {
            _windowStartTime = now;
            _windowEvents = 0;
        }
    }
}
