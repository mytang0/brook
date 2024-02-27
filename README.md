# Brook

Brook is an orchestration engine, supports microservices and in-app logic (embedded use) orchestration. With the embedded mode, users can effortlessly build their own workflow orchestration engine.

## Getting started

In order to maximize the lightweight nature of the Brook engine, a deliberate separation is made between its core components (which depend solely on a few essential toolkits) and the middleware extensions using SPI (Service Provider Interface). Consequently, regardless of the application implementation framework, one can seamlessly rely on the engine JAR and initialize the relevant instances.

### Maven dependency

Specify the version appropriate for the project.
```xml
<properties>
    <dubbo.version>...</dubbo.version>
</properties>
```

#### Not using Spring

```xml
<dependencies>
    <dependency>
        <groupId>xyz.mytang0.brook</groupId>
        <artifactId>>brook-engine</artifactId>
        <version>${brook.version}</version>
    </dependency>
</dependencies>
```

#### Springboot (recommend)

```xml
<dependencies>
    <dependency>
        <groupId>xyz.mytang0.brook</groupId>
        <artifactId>>brook-spring-boot-starter</artifactId>
        <version>${brook.version}</version>
    </dependency>
</dependencies>
```

### Maven archetype

For expedited testing.

```shell
mvn archetype:generate                                  \
  -DarchetypeGroupId=xyz.mytang0.brook.demo             \
  -DarchetypeArtifactId=brook-demo-spring-archetype     \
  -DarchetypeVersion=1.0.0-SNAPSHOT                     \
  -DgroupId=<my.groupid>                                \
  -DartifactId=<my-artifactId>                          \
  -DinteractiveMode=false                               \
  -DarchetypeCatalog=internal
```

The definition of the testing process is located at 'resources/META-INF/flows'.

## Contributing

Welcomes anyone that wants to help out in any way, whether that includes reporting problems, helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features. You can report problems to request features in the [GitHub Issues](https://github.com/mytang0/brook/issues).