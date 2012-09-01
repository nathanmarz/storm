package backtype.storm.testing;

import backtype.storm.testing.Bar;


public class TestClassLoaderTopology {
	public static void main(String[] args) {
		System.out.println("=======================================================================");
		// Bar.foo will load the backtype.storm.testing.Foo from Storm core
		Bar.foo();
		// The following loads the backtype.storm.testing.Foo from classloader_test.
		System.out.println("classloader_test::backtype.storm.testing.Foo loaded by " + Foo.class.getClassLoader());
	}
}
