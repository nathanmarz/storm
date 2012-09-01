package backtype.storm.testing;

import backtype.storm.testing.Bar;


public class TestClassLoaderTopology {
	public static void main(String[] args) {
		System.out.println("=======================================================================");
		Bar.foo();
		System.out.println("classloader_test::backtype.storm.testing.Foo loaded by " + Foo.class.getClassLoader());
	}
}
