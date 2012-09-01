package backtype.storm.testing;

public class Bar {
	public static void foo() {
		System.out.println("Storm::backtype.storm.testing.Foo loaded by " + Foo.class.getClassLoader());
	}
}
