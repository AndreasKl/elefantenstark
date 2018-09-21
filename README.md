# elefantenstark

[![Build Status](https://travis-ci.org/AndreasKl/elefantenstark.svg?branch=master)](https://travis-ci.org/AndreasKl/elefantenstark)
[![codecov](https://codecov.io/gh/AndreasKl/elefantenstark/branch/master/graph/badge.svg)](https://codecov.io/gh/AndreasKl/elefantenstark)

**Elefantenstark** is a **PostgreSQL** powered worker queue for Java 8. It uses
Postgres **advisory locks** features to lock work while being worked on.

**The lock can be tweaked** to solve requirements
regarding the ordering of how work has to be processed. E.g. all work items sharing 
the same key must be processed one after another.   

#### Initializing the database tables

The `Initializer` creates the default **queue** table and indexes.

```java
withPostgres(
    connection -> {
      new Initializer().build(connection);
      validateTableIsAvailable(connection);
    });
```

#### Producing work

`Producer` is thread safe and can be used to produce a
 `WorkItem` to the **queue**. 

```java

withPostgresAndSchema(
    connection -> {
      WorkItem workItem = WorkItem.groupedOnKey("_test_key_", "_test_value_", 0);
      new Producer().produce(connection, workItem);

      WorkItem queuedWorkItem = queryForWorkItem(connection);

      assertEquals(workItem, queuedWorkItem);
    });
```

#### Consuming work

A `Consumer` can be `sessionScoped` (tied to the current connection) or `transactionScoped` (creates a transaction). 
The session scoped can modify state that is not rolled back when the
consuming function fails, however can cause unreleased locks when the application is killed. These are cleaned up when
the OS decides to close the network connection.

```java
withPostgresAndSchema(
    connection -> {
      Producer producer = new Producer();
      Consumer consumer = Consumer.sessionScoped();
      producer.produce(connection, WorkItem.groupedOnKey("a", "b", 23));

      Optional<String> next = consumer.next(
          connection,
          workItemContext -> {
            capturedWork.set(workItemContext);
            return "OK+";
          });
    });
```

#### Builds

Snapshot builds are available on [Sonatype Snapshots](https://oss.sonatype.org/content/repositories/snapshots/net/andreaskluth/elefantenstark/).

```xml
<dependencies>
  <dependency>
    <groupId>net.andreaskluth</groupId>
    <artifactId>elefantenstark</artifactId>
    <version>0.1-SNAPSHOT</version>
  </dependency>
</dependencies>

<repositories>
  <repository>
    <id>snapshots-repo</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    <releases>
      <enabled>false</enabled>
    </releases>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
  </repository>
</repositories>
```