package backtype.storm.testing;

public class Bar {
	public static String foo() {
		return Foo.class.getClassLoader().toString();
	}
}
