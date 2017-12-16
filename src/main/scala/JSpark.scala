import java.util.Properties
import java.io.{FileWriter, PrintWriter, Writer}

import com.typesafe.config.ConfigFactory
import org.apache.hive.jdbc.HiveDriver
import org.apache.hive.jdbc.Utils
import org.jooq.impl.DSL

case class Config(url: String,
                  query: String,
                  user: String, password: String,
                  output: String, format: String)

object JSpark {
  val argsParser = new scopt.OptionParser[Config]("Simple Jdbc client for Apache Spark") {
    head("JSpark", "1.1")

    opt[String]('u', "url")
      .text("jdbc url with the prefix: jdbc:hive2://")
      .valueName(Utils.URL_PREFIX + "...").action((x, c) => c.copy(url = x))

    opt[String]('q', "query")
      .text("sql query like SHOW TABLES")
      .action((x, c) => c.copy(query = x))

    opt[String]('n', "name").action((x, c) => c.copy(user = x))
    opt[String]('p', "password").action((x, c) => c.copy(password = x))

    opt[String]('o', "output")
      .text("stdout or file name")
      .action((x, c) => c.copy(output = x))
    opt[String]('f', "format")
      .text("output format: json, xml, cvs, html or simple")
      .action((x, c) => c.copy(format = x))

    help("help")
    version("version")
  }

  def run(config: Config): Unit = {
    val driver = new HiveDriver
    val props = new Properties()

    props.setProperty("user", config.user)
    props.setProperty("password", config.password)

    val conn = driver.connect(config.url, props)

    if (conn != null) {
      try {
        val writer = getWriter(config.output)
        val ctx = DSL.using(conn)
        val res = ctx.resultQuery(config.query).fetch()

        config.format.toLowerCase match {
          case "json" => res.formatJSON(writer)
          case "xml" => res.formatXML(writer)
          case "csv" => res.formatCSV(writer)
          case "html" => res.formatHTML(writer)
          case "simple" => res.format(writer, Int.MaxValue)
          case unknown => throw new IllegalArgumentException(s"Not supported output format $unknown")
        }
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
      output = conf.getString("output.to"),
      format = conf.getString("output.format")
    )
  }

  def getWriter(it: String): Writer = it match {
    case "stdout" => new PrintWriter(System.out)
    case fileName => new FileWriter(fileName)
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, appConf) match {
      case Some(config) => run(config)
      case _ => System.err.println("Fix params and try again")
    }
  }
}
