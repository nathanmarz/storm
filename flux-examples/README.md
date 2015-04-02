# Flux Examples
A collection of examples illustrating various capabilities.

## Building From Source and Running

Checkout the projects source and perform a top level Maven build (i.e. from the `flux` directory):

```bash
git clone https://github.com/ptgoetz/flux.git
cd flux
mvn install
```

This will create a shaded (i.e. "fat" or "uber") jar in the `flux-examples/target` directory that can run/deployed with
the `storm` command:

```bash
cd flux-examples
storm jar ./target/flux-examples-0.2.3-SNAPSHOT.jar org.apache.storm.flux.Flux --local ./src/main/resources/simple_wordcount.yaml
```

The example YAML files are also packaged in the examples jar, so they can also be referenced with Flux's `--resource`
command line switch:

```bash
storm jar ./target/flux-examples-0.2.3-SNAPSHOT.jar org.apache.storm.flux.Flux --local --resource /sime_wordcount.yaml
```

