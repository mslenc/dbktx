package schema1

import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.conn.DbConnectorImpl
import com.xs0.dbktx.conn.TimeProviderFromClock
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.asyncsql.MySQLClient
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.launch
import java.time.Clock

fun main(args: Array<String>) {
    val vertx = Vertx.vertx()
    val server = vertx.createHttpServer()
    println("${TestSchema.numberOfTables} tables initialized")

    val mySQLClientConfig =
            JsonObject(mapOf(
                "host" to "127.0.0.1",
                "port" to 3306,
                "username" to "eteam",
                "password" to "eteam",
                "database" to "eteam"
            ))

    val mySqlClient = MySQLClient.createShared(vertx, mySQLClientConfig, "test")

    val dbConnector = DbConnectorImpl(mySqlClient, timeProvider = TimeProviderFromClock(Clock.systemDefaultZone()))

    server.requestHandler { request ->
        launch(Unconfined) {
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
