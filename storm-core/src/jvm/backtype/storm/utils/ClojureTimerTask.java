package backtype.storm.utils;

import clojure.lang.IFn;
import java.util.TimerTask;

public class ClojureTimerTask extends TimerTask {
    IFn _afn;
    
    public ClojureTimerTask(IFn afn) {
        super();
        _afn = afn;
    }
    
    @Override
    public void run() {
        _afn.run();
    }    
}
