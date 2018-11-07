package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.schemas.test2.*
import com.github.mslenc.dbktx.util.defer
import com.github.mslenc.dbktx.util.testing.DelayedExec
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import com.github.mslenc.dbktx.util.testing.MockResultSet
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean

@RunWith(VertxUnitRunner::class)
class AggregateTest {
    @Rule
    @JvmField
    var rule = RunTestOnContext()

    @Before
    fun initSchema() {
        TestSchema2.numberOfTables
    }

    @Test
    fun testSimpleQuery() {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun executeQuery(sql: String, values: MutableList<Any?>): CompletableFuture<DbResultSet> {
                theSql = sql
                theParams = values
                called.set(true)

                // W.name, AVG(CE.score), MIN(CE.score), LN.name, LN2.name, AVG(CR.place), W.id_weight, CE.id_country

                return CompletableFuture.completedFuture(
                    MockResultSet.Builder(
                           "W.name",  "AVG(CE.score)", "MIN(CE.score)", "LN.name",  "LN2.name",          "AVG(CR.place)", "W.id_weight", "CE.id_country"
                    ).
                    addRow("12-34kg", 12.5,            6.2,             "Slovenia", "Nagrada Ljubljane", 6.2,             1,             2).
                    addRow("13-55kg", 12.1,            5.1,             "Austria",  "Državno Avstrije",  4.3,             1,             2).
                    build())
            }
        }
        val delayedExec = DelayedExec()
        val db = DbLoaderImpl(connection, delayedExec, RequestTime.forTesting())

        lateinit var weightName: BoundAggregateExpr<String>

        val query =
            Weight.aggregateQuery(db) {
                weightName = +Weight.NAME

                innerJoin(Weight.ENTRIES_SET) {
                    filter {
                        CompEntry.ID_COUNTRY eq 123
                    }

                    +CompEntry.FINAL_SCORE.avg()
                    +CompEntry.FINAL_SCORE.min()

                    filter {
                        CompEntry.REF_COMPETITION.has {
                            Competition.ENTRIES_SET.contains {
                                CompEntry.ID_WEIGHT oneOf setOf(12, 13, 14)
                            }
                        }
                    }

                    innerJoin(CompEntry.REF_COUNTRY) {
                        innerJoin(Country.REF_LOCALNAME("en")) {
                            +LocalName.NAME

                            filter {
                                LocalName.NAME.contains("a")
                            }
                        }
                    }

                    innerJoin(CompEntry.REF_COMPETITION) {
                        innerJoin(Competition.REF_LOCALNAME("sl")) {
                            +LocalName.NAME

                            filter {
                                LocalName.NAME.contains("prix")
                            }
                        }
                    }

                    innerJoin(CompEntry.REF_RESULT) {
                        +CompResult.PLACE.avg()
                    }
                }
            }

        query.orderBy(Weight.NAME)

        query.expand {
            +Weight.ID_WEIGHT
            innerJoin(Weight.ENTRIES_SET) {
                +CompEntry.ID_COUNTRY
            }
        }

        val deferred = defer { query.run() }

        Assert.assertFalse(called.get())
        delayedExec.executePending()
        Assert.assertTrue(called.get())

        val result = runBlocking { deferred.await() }

        assertEquals(
            "SELECT W.name, AVG(CE.score), MIN(CE.score), LN.name, LN2.name, AVG(CR.place), W.id_weight, CE.id_country " +
            "FROM weights AS W " +
            "INNER JOIN comp_entries AS CE ON W.id_weight = CE.weight_id " +
            "INNER JOIN competitions AS C ON C.id_competition = CE.id_comp " +
            "INNER JOIN local_names AS LN2 ON LN2.entity_id = C.id_competition AND LN2.lang_code = ? AND LN2.prop_name = ? " +
            "INNER JOIN countries AS C2 ON C2.id_country = CE.id_country " +
            "INNER JOIN local_names AS LN ON LN.entity_id = C2.id_country AND LN.lang_code = ? AND LN.prop_name = ? " +
            "INNER JOIN comp_results AS CR ON CR.id_person = CE.id_person AND CR.id_country = CE.id_country AND CR.id_weight = CE.weight_id " +
            "WHERE (CE.id_country = 123) " +
            "AND (" +
                "(C.id_competition IN " +
                    "(SELECT CE2.id_comp FROM comp_entries AS CE2 " +
                    "WHERE CE2.weight_id IN (12, 13, 14))" +
                ")" +
            ") AND (LN.name LIKE ? ESCAPE '|') " +
              "AND (LN2.name LIKE ? ESCAPE '|') " +
            "GROUP BY W.name, LN.name, LN2.name, W.id_weight, CE.id_country " +
            "ORDER BY W.name", theSql)

        assertEquals(6, theParams.size)

        assertEquals("sl", theParams[0])
        assertEquals("competition.name", theParams[1])
        assertEquals("en", theParams[2])
        assertEquals("country.name", theParams[3])
        assertEquals("%a%", theParams[4])
        assertEquals("%prix%", theParams[5])

        assertEquals(2, result.size)

        val row0 = result[0]
        assertEquals("12-34kg", row0[0].unwrap())
        assertEquals("12-34kg", row0[Weight.NAME])
        assertEquals("12-34kg", row0[weightName])
        assertEquals("12-34kg", weightName(row0))

        assertEquals(12.5, row0[1].unwrap())
        assertEquals(12.5, row0.getValue(CompEntry.FINAL_SCORE.avg()).asDouble(), 0.0)

        assertEquals(6.2, row0[2].unwrap())
        assertEquals("Slovenia", row0[3].unwrap())
        assertEquals("Nagrada Ljubljane", row0[4].unwrap())
        assertEquals(6.2, row0[5].unwrap())
        assertEquals(1, row0[6].unwrap())
        assertEquals(2, row0[7].unwrap())

        // TODO: check other columns (and maybe second row)?
        // TODO: fix types of AVG and SUM - they don't stay the same as the original (workaround in the meantime: use getValue())
        // TODO: disable AVG, SUM for non-numeric columns, as it doesn't make much sense..

        // addRow("12-34kg", 12.5,            6.2,             "Slovenia", "Nagrada Ljubljane", 6.2,             1,             2).
        // addRow("13-55kg", 12.1,            5.1,             "Austria",  "Državno Avstrije",  4.3,             1,             2).


    }
}