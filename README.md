# jspark
Simple jdbc client for Apache Spark

## Assembling

You need to have sbt to build and assembly jspark. Follow instructions at sbt website to
install sbt on [Mac OS](http://www.scala-sbt.org/1.0/docs/Installing-sbt-on-Mac.html),
[Linux](http://www.scala-sbt.org/1.0/docs/Installing-sbt-on-Linux.html) or
[Windows](http://www.scala-sbt.org/1.0/docs/Installing-sbt-on-Windows.html).

The following command creates fat jar with jspark:
```
$ sbt assembly
[info] Packaging /Users/maximgekk/proj/jspark/target/scala-2.12/jspark.jar ...
```

The assembled jar is available in BinTray - [jspark.jar](https://bintray.com/maxgekk/generic/download_file?file_path=jspark.jar)

## Config

JSpark accepts command line arguments and configuration files. Command arguments have priority
over configuration parameters and overwrite them. Default config file is included in jspark.jar.
You can find it in the resource folder - [application.conf](https://github.com/MaxGekk/jspark/blob/master/src/main/resources/application.conf).

JSpark can be run with custom config file like [mycluster.conf](https://github.com/MaxGekk/jspark/blob/master/src/main/resources/mycluster.conf)

```
$ java -Dconfig.file=mycluster.conf -jar jspark.jar
```

Configuration file may have at least the following sections:

* **credentials** contains username/password or token. For example:
```
credentials = {
  user = "foo@bar.com"
  password = "12345678"
}
```

* **jdbc** settings like **url** which must have the prefix **jdbc:hive2://**. For example:

```
jdbc = {
  url = "jdbc:hive2://shardname.cloud.databricks.com:443/default;transportMode=http;ssl=true;httpPath=sql/protocolv1/o/0/clustername"
}
```

* **sql query** that will be executed by default:

```
sql = {
  query = "select * from table limit 10"
}
```

* output related settings like **format**. Current versions of jspark supports the following formats: 
_simple_ (by default), csv, json, html, xml.

```
output = {
  format = "json"
  to = "file.json"
}
```

## Command line options

The options overwrite settings from the config file. JSpark support the following options:

```
$ java -jar jspark.jar --help
```

```
JSpark 1.2
Usage: Simple Jdbc client for Apache Spark [options]

  -u, --url string       jdbc url with the prefix: jdbc:hive2://
  -q, --query string     sql query like SHOW TABLES
  -n, --name string      
  -p, --password string  
  -o, --output string    stdout or file name
  -f, --format string    supported: json, xml, cvs, html or simple
  --help                 
  --version   
```
For example:

```
$ java -Dconfig.file=mycluster.conf -jar jspark.jar -q "select id, type, priority, status from tickets limit 5"
```

it outputs result in simple format by default:

```
+----+--------+--------+------+
|  id|type    |priority|status|
+----+--------+--------+------+
|9120|problem |urgent  |closed|
|9121|question|normal  |hold  |
|9122|incident|normal  |closed|
|9123|question|normal  |open  |
|9124|incident|normal  |solved|
+----+--------+--------+------+
```

or in json format:

```
$ java -Dconfig.file=mycluster.conf -jar jspark.jar -q "select id, status from tickets" -f json

{"fields":[{"name":"id","type":"BIGINT"},{"name":"status","type":"OTHER"}],"records":[[9120,"closed"],[9121,"hold"],[9122,"closed"],[9123,"open"],[9124,"solved"]]}
```