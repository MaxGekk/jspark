import java.util.Properties
import org.apache.hive.jdbc.HiveDriver
import org.apache.hive.jdbc.Utils

case class Config(url: String = "jdbc:hive2://???",
                  user: String = "???",
                  password: String = "???"
                 )

object JSpark {
  val argsParser = new scopt.OptionParser[Config]("Simple Jdbc client for Apache Spark") {
    head("JSpark", "1.0")

    opt[String]('u', "url").valueName(Utils.URL_PREFIX + "...").action((x, c) =>
      c.copy(url = x)
    )

    opt[String]('n', "name").action((x, c) => c.copy(user = x))
    opt[String]('p', "password").action((x, c) => c.copy(password = x))
  }

  def run(config: Config): Unit = {
    val driver = new HiveDriver
    val props = new Properties()

    props.setProperty("user", config.user)
    props.setProperty("password", config.password)
    println(s"config: $config")
    val conn = driver.connect(config.url, props)

    if (conn != null) {
      val stmt = conn.createStatement()
      val query = "SHOW TABLES"
      val rs = stmt.executeQuery(query)

      println(query)
      println("---")
      while (rs.next) {
        val database = rs.getString("database")
        val tableName = rs.getString("tableName")
        println(s"$database.$tableName")
      }

      conn.close()
    } else {
      System.err.println(
        s"""
           |\ERROR: connection is null. Probably, url has wrong format.
           | url must have the prefix: ${Utils.URL_PREFIX}
         """.stripMargin)
    }
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Config()) match {
      case Some(config) => run(config)
      case _ => System.err.println("Fix params and try again")
    }
  }
}
