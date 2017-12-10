import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.hive.jdbc.HiveDriver
import org.apache.hive.jdbc.Utils
import org.jooq.impl.DSL

case class Config(url: String,
                  query: String,
                  user: String, password: String,
                  format: String)

object JSpark {
  val argsParser = new scopt.OptionParser[Config]("Simple Jdbc client for Apache Spark") {
    head("JSpark", "1.0")

    opt[String]('u', "url").valueName(Utils.URL_PREFIX + "...").action((x, c) =>
      c.copy(url = x)
    )
    opt[String]('q', "query").action((x, c) => c.copy(query = x))

    opt[String]('n', "name").action((x, c) => c.copy(user = x))
    opt[String]('p', "password").action((x, c) => c.copy(password = x))

    opt[String]('f', "format").action((x, c) => c.copy(format = x))
  }

  def run(config: Config): Unit = {
    val driver = new HiveDriver
    val props = new Properties()

    props.setProperty("user", config.user)
    props.setProperty("password", config.password)

    val conn = driver.connect(config.url, props)

    if (conn != null) {
      try {
        val stmt = conn.createStatement()
        val resultSet = stmt.executeQuery(config.query)
        val res = DSL.using(conn).fetch(resultSet)

        val resStr: String = config.format.toLowerCase match {
          case "json" => res.formatJSON()
          case "xml" => res.formatXML()
          case "csv" => res.formatCSV()
          case "html" => res.formatHTML()
          case "simple" => res.format()
          case unknown => throw new IllegalArgumentException(s"Not supported output format $unknown")
        }
        System.out.println(resStr)
      } finally {
        conn.close()
      }
    } else {
      throw new IllegalArgumentException(
        s"""
           | Connection ref is null. Probably, url has wrong format.
           | The url is ${config.url}
           | It must have the prefix: ${Utils.URL_PREFIX}
         """.stripMargin)
    }
  }

  def appConf: Config = {
    val conf = ConfigFactory.load()

    Config(
      url = conf.getString("jdbc.url"),
      query = conf.getString("sql.query"),
      user = conf.getString("credentials.user"),
      password = conf.getString("credentials.password"),
      format = conf.getString("output.format")
    )
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, appConf) match {
      case Some(config) => run(config)
      case _ => System.err.println("Fix params and try again")
    }
  }
}
