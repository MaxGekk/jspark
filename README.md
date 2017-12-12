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

JSpark can be run with custom config file:

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
  query = "show tables"
}
```

* output related settings like **format**. Current versions of jspark supports the following formats: 
_simple_ (by default), csv, json, html, xml.

```
output = {
  format = "simple"
}
```


