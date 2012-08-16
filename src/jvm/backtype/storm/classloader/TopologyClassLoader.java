package backtype.storm.classloader;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

public class TopologyClassLoader extends URLClassLoader {
    public TopologyClassLoader(ClassLoader parent){
    	// load the class from the path specified by java property: topology.classpath
        super(string2Url(System.getProperty("topology.classpath").split(File.pathSeparator)), parent);
        System.out.println("TopologyClassLoader: java.system.class.loader: " + System.getProperty("java.system.class.loader"));
        System.out.println("TopologyClassLoader: topology.classpath: " + System.getProperty("topology.classpath"));
    }
    
	@Override
	public synchronized Class<?> loadClass(String name)
			throws ClassNotFoundException {
		System.out.println("CLASSNAME: " + name);
		Class<?> clazz = this.findLoadedClass(name);

		if (clazz == null) {
			// first find the class from the topology's dependencies
			try {
				clazz = this.findClass(name);
			} catch (ClassNotFoundException e) {
				// ignore it.
			}
			
			if (clazz == null) {
				// then delegate to parent loader to load
				clazz = this.getParent().loadClass(name);
			}
		}

		return clazz;
	}
    
    public Enumeration<URL> getResources(String name) throws IOException {
        Enumeration<URL> ret = this.findResources(name);
        
        if (ret == null || !ret.hasMoreElements()) {
            ret = this.getParent().getResources(name);
        }
        
        return ret;
    }
    
    protected static URL[] string2Url(String[] urls) {
        URL[] urlArr = new URL[urls.length];
        int idx = 0;
        for (String url : urls) {
            try {
                urlArr[idx] = new File(url).toURI().toURL();
                idx++;
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
        return urlArr;
    }
}
