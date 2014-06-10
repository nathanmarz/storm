package backtype.storm.messaging.netty;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

public class NettyRenameThreadFactory  implements ThreadFactory {
    
    static {
      //Rename Netty threads
      ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT);
    }
  
    final ThreadGroup group;
    final AtomicInteger index = new AtomicInteger(1);
    final String name;

    NettyRenameThreadFactory(String name) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null)? s.getThreadGroup() :
                             Thread.currentThread().getThreadGroup();
        this.name = name;
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, name + "-" + index.getAndIncrement(), 0);
        if (t.isDaemon())
            t.setDaemon(false);
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }
}