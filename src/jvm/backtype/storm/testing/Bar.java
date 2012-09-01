package backtype.storm.testing;

public class Bar {
	public static String foo() {
		return "Storm::backtype.storm.testing.Foo loaded by " + Foo.class.getClassLoader();
	}
}
