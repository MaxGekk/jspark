import java.util.Properties
import java.io.{FileWriter, PrintWriter, Writer}
import com.typesafe.config.ConfigFactory
import org.apache.hive.jdbc.HiveDriver
import org.apache.hive.jdbc.Utils
import org.jooq.impl.DSL
import scala.util.Try

case class Config(url: Option[String],
                  query: Option[String],
                  user: Option[String], password: Option[String],
                  output: Option[String], format: Option[String])

object JSpark {
  val argsParser = new scopt.OptionParser[Config]("Simple Jdbc client for Apache Spark") {
    head("JSpark", "1.2")

    opt[String]('u', "url")
      .text("jdbc url with the prefix: jdbc:hive2://")
      .valueName("string")
      .action((x, c) => c.copy(url = Some(x)))

    opt[String]('q', "query")
      .text("sql query like SHOW TABLES")
      .valueName("string")
      .action((x, c) => c.copy(query = Some(x)))

    opt[String]('n', "name")
      .valueName("string")
      .action((x, c) => c.copy(user = Some(x)))
    opt[String]('p', "password")
      .valueName("string")
      .action((x, c) => c.copy(password = Some(x)))

    opt[String]('o', "output")
      .text("stdout or file name")
      .valueName("string")
      .action((x, c) => c.copy(output = Some(x)))
    opt[String]('f', "format")
      .text("supported: json, xml, cvs, html or simple")
      .valueName("string")
      .action((x, c) => c.copy(format = Some(x)))

    help("help")
    version("version")
  }

  def run(config: Config): Unit = {
    val driver = new HiveDriver
    val props = new Properties()

    props.setProperty("user", config.user.get)
    props.setProperty("password", config.password.get)

    val conn = driver.connect(config.url.get, props)

    if (conn != null) {
      try {
        val writer = getWriter(config.output.getOrElse("stdout"))
        val ctx = DSL.using(conn)
        val res = ctx.resultQuery(config.query.get).fetch()

        config.format.getOrElse("simple").toLowerCase match {
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
      url = Try { conf.getString("jdbc.url") }.toOption,
      query = Try { conf.getString("sql.query") }.toOption,
      user = Try { conf.getString("credentials.user") }.toOption,
      password = Try { conf.getString("credentials.password") }.toOption,
      output = Try { conf.getString("output.to") }.toOption,
      format = Try { conf.getString("output.format") }.toOption
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
