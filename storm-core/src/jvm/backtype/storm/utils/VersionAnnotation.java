package backtype.storm.utils;
import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PACKAGE)
public @interface VersionAnnotation {
  String version();

  String user();

  String date();

  String url();

  String revision();

  String branch();

  String srcChecksum();
}
