---
layout: documentation
---
To develop topologies, you'll need the Storm jars on your classpath. You should either include the unpacked jars in the classpath for your project or use Maven to include Storm as a development dependency. Storm is hosted on Maven Central. To include Storm in your project as a development dependency, add the following to your pom.xml:


```xml
<dependency>
  <groupId>org.apache.storm</groupId>
  <artifactId>storm-core</artifactId>
  <version>0.9.3</version>
  <scope>provided</scope>
</dependency>
```

[Here's an example](https://github.com/apache/storm/blob/master/examples/storm-starter/pom.xml) of a pom.xml for a Storm project.

### Developing Storm

Please refer to [DEVELOPER.md](https://github.com/apache/storm/blog/master/DEVELOPER.md) for more details.