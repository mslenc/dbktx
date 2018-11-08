package schema1

import com.github.mslenc.asyncdb.DbConfig
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.DbConnectorImpl
import com.github.mslenc.dbktx.conn.TimeProviderFromClock
import com.github.mslenc.dbktx.util.vertxLaunch
import io.vertx.core.Vertx
import java.time.Clock

fun main(args: Array<String>) {
    val vertx = Vertx.vertx()
    val server = vertx.createHttpServer()
    println("${TestSchema.numberOfTables} tables initialized")

    val dbConfig =
        DbConfig.newBuilder(DbConfig.DbType.MYSQL).
            setHost("127.0.0.1", 3306).
            setDefaultCredentials("eteam", "eteam").
            setDefaultDatabase("eteam").
            setEventLoopGroup(vertx.nettyEventLoopGroup()).
        build()

    val mySqlClient = dbConfig.makeDataSource()

    val dbConnector = DbConnectorImpl(mySqlClient, timeProvider = TimeProviderFromClock(Clock.systemDefaultZone()))

    server.requestHandler { request ->
        vertxLaunch {
            val start = System.currentTimeMillis()
            var response = "!!!"

            try {
                dbConnector.connect { db: DbConn ->
                    val sb = StringBuilder()

                    val mitja = db.loadById(TestSchema.PEOPLE, 1)
                    val irena = db.loadById(TestSchema.PEOPLE, 2)

                    for (person in arrayOf(mitja, irena)) {
                        sb.append(person.firstName + " " + person.lastName + "\n")
                    }

                    response = sb.toString()
                }
            } catch (t: Throwable) {
                t.printStackTrace()
                response = "Error: $t"
            }

            val res = request.response()
            res.putHeader("content-type", "text/plain; charset=UTF-8")
            res.end(response + "Hello World!")

            println("Finished in ${System.currentTimeMillis() - start} ms")
        }
    }

    server.listen(8888)
}
