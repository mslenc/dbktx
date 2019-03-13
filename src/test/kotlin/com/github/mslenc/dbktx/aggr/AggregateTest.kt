package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbQueryResultObserver
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.schemas.test2.*
import com.github.mslenc.dbktx.schemas.test3.Invoice
import com.github.mslenc.dbktx.schemas.test3.InvoiceDailyTimeItem
import com.github.mslenc.dbktx.schemas.test3.InvoiceTimeItem
import com.github.mslenc.dbktx.schemas.test3.TestSchema3
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import com.github.mslenc.dbktx.util.testing.MockResultSet
import com.github.mslenc.dbktx.util.testing.toLD
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Test
import java.math.BigDecimal
import java.time.LocalDate
import java.util.concurrent.atomic.AtomicBoolean

class AggregateTest {
    init {
        TestSchema2.numberOfTables // init
        TestSchema3.numberOfTables
    }

    class CompAggr {
        lateinit var weightName: String
        var averageScore: Double? = null
        var minScore: Int? = null
        lateinit var countryName: String
        lateinit var compName: String
        var entityCount: Long = 0L
        var entityDistinctCount: Long = 0L
        var avgPlace: Double? = null
        var idWeight: Int = 0
        var idCountry: Int = 0
    }

    @Test
    fun testComplexQuery() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun streamQuery(sql: String, observer: DbQueryResultObserver, args: List<Any?>) {
                theSql = sql
                theParams = args
                called.set(true)

                MockResultSet.Builder(
                        "W.name", "AVG(CE.score)", "MIN(CE.score)", "LN.name", "LN2.name", "COUNT(LN2.entity_id)", "COUNT(DISTINCT LN2.entity_id)", "AVG(CR.place)", "W.id_weight", "CE.id_country"
                ).
                addRow("12-34kg", 12.5,            6,            "Slovenia", "Nagrada Ljubljane",  9,                   5,                               2.4,            11,             25).
                addRow("13-55kg", 12.1,            5,            "Austria",  "Državno Avstrije",   4,                   3,                               2.6,            18,             133).
                streamInto(observer)
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        val query =
            Weight.makeAggregateListQuery(db, ::CompAggr) {
                CompAggr::weightName becomes Weight.NAME

                innerJoin(Weight.ENTRIES_SET) {
                    filter {
                        CompEntry.ID_COUNTRY eq 123
                    }

                    CompAggr::averageScore becomes average { +CompEntry.FINAL_SCORE }
                    CompAggr::minScore becomes min { +CompEntry.FINAL_SCORE }

                    filter {
                        CompEntry.REF_COMPETITION.has {
                            Competition.ENTRIES_SET.contains {
                                CompEntry.ID_WEIGHT oneOf setOf(12, 13, 14)
                            }
                        }
                    }

                    innerJoin(CompEntry.REF_COUNTRY) {
                        innerJoin(Country.REF_LOCALNAME("en")) {
                            CompAggr::countryName becomes LocalName.NAME

                            filter {
                                LocalName.NAME.contains("a")
                            }
                        }
                    }

                    innerJoin(CompEntry.REF_COMPETITION) {
                        innerJoin(Competition.REF_LOCALNAME("sl")) {
                            CompAggr::compName becomes LocalName.NAME
                            CompAggr::entityCount becomes count { +LocalName.ENTITY_ID }
                            CompAggr::entityDistinctCount becomes countDistinct { +LocalName.ENTITY_ID }

                            filter {
                                LocalName.NAME.contains("prix")
                            }
                        }
                    }

                    innerJoin(CompEntry.REF_RESULT) {
                        CompAggr::avgPlace becomes average { +CompResult.PLACE }
                    }
                }
            }

        query.orderBy(Weight.NAME)

        query.expand {
            CompAggr::idWeight becomes Weight.ID_WEIGHT
            innerJoin(Weight.ENTRIES_SET) {
                CompAggr::idCountry becomes CompEntry.ID_COUNTRY
            }
        }

        val deferred = async { query.run() }

        Assert.assertFalse(called.get())

        val result = deferred.await()

        Assert.assertTrue(called.get())


        assertEquals(
            "SELECT W.\"name\", AVG(CE.\"score\"), MIN(CE.\"score\"), LN.\"name\", LN2.\"name\", COUNT(LN2.\"entity_id\"), COUNT(DISTINCT LN2.\"entity_id\"), AVG(CR.\"place\"), W.\"id_weight\", CE.\"id_country\" " +
            "FROM \"weights\" AS W " +
            "INNER JOIN \"comp_entries\" AS CE ON W.\"id_weight\" = CE.\"weight_id\" " +
            "INNER JOIN \"competitions\" AS C ON C.\"id_competition\" = CE.\"id_comp\" " +
            "INNER JOIN \"local_names\" AS LN2 ON LN2.\"entity_id\" = C.\"id_competition\" AND LN2.\"lang_code\" = ? AND LN2.\"prop_name\" = ? " +
            "INNER JOIN \"countries\" AS C2 ON C2.\"id_country\" = CE.\"id_country\" " +
            "INNER JOIN \"local_names\" AS LN ON LN.\"entity_id\" = C2.\"id_country\" AND LN.\"lang_code\" = ? AND LN.\"prop_name\" = ? " +
            "INNER JOIN \"comp_results\" AS CR ON CR.\"id_person\" = CE.\"id_person\" AND CR.\"id_country\" = CE.\"id_country\" AND CR.\"id_weight\" = CE.\"weight_id\" " +
            "WHERE (CE.\"id_country\" = 123) " +
            "AND (" +
                "(C.\"id_competition\" IN " +
                    "(SELECT CE2.\"id_comp\" FROM \"comp_entries\" AS CE2 " +
                    "WHERE CE2.\"weight_id\" IN (12, 13, 14))" +
                ")" +
            ") AND (LN.\"name\" LIKE ? ESCAPE '|') " +
              "AND (LN2.\"name\" LIKE ? ESCAPE '|') " +
            "GROUP BY W.\"name\", LN.\"name\", LN2.\"name\", W.\"id_weight\", CE.\"id_country\" " +
            "ORDER BY W.\"name\"", theSql)

        assertEquals(6, theParams.size)

        assertEquals("sl", theParams[0])
        assertEquals("competition.name", theParams[1])
        assertEquals("en", theParams[2])
        assertEquals("country.name", theParams[3])
        assertEquals("%a%", theParams[4])
        assertEquals("%prix%", theParams[5])

        assertEquals(2, result.size)

        val row0 = result[0]
        assertEquals("12-34kg", row0.weightName)
        assertEquals(12.5, row0.averageScore)
        assertEquals(6, row0.minScore)
        assertEquals("Slovenia", row0.countryName)
        assertEquals("Nagrada Ljubljane", row0.compName)
        assertEquals(9, row0.entityCount)
        assertEquals(5, row0.entityDistinctCount)
        assertEquals(2.4, row0.avgPlace)
        assertEquals(11, row0.idWeight)
        assertEquals(25, row0.idCountry)

        // TODO: check other columns (and maybe second row)?
        // TODO: fix types of AVG and SUM - they don't stay the same as the original (workaround in the meantime: use getValue())
        // TODO: disable AVG, SUM for non-numeric columns, as it doesn't make much sense..

        // addRow("12-34kg", 12.5,            6.2,             "Slovenia", "Nagrada Ljubljane", 6.2,             1,             2).
        // addRow("13-55kg", 12.1,            5.1,             "Austria",  "Državno Avstrije",  4.3,             1,             2).


    }

    class InvoiceSummary {
        var numInvoices: Long = 0
        var hours: BigDecimal? = null
        var totalFee: BigDecimal? = null
        var firstDate: LocalDate? = null
        var lastDate: LocalDate? = null
    }

    @Test
    fun testSimpleQuery() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun streamQuery(sql: String, observer: DbQueryResultObserver, args: List<Any?>) {
                theSql = sql
                theParams = args
                called.set(true)

                MockResultSet.Builder(
                        "COUNT(DISTINCT I.id)", "SUM(IDTI.hours)", "SUM(ITI.hourly_rate * IDTI.hours)", "MIN(IDTI.date_worked)", "MAX(IDTI.date_worked)"
                ).
                addRow(     12L,                     42.5,             1521.6,                            "2019-02-01".toLD(),   "2019-02-27".toLD()).
                streamInto(observer)
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        val query =
            Invoice.makeAggregateListQuery(db, ::InvoiceSummary) {
                InvoiceSummary::numInvoices becomes countDistinct { +Invoice.ID }

                InvoiceSummary::hours becomes sum { Invoice.TIME_ITEMS_SET..InvoiceTimeItem.DAILY_ITEMS_SET..InvoiceDailyTimeItem.HOURS }

                innerJoin(Invoice.TIME_ITEMS_SET) {
                    InvoiceSummary::totalFee becomes sum { InvoiceTimeItem.HOURLY_RATE * (InvoiceTimeItem.DAILY_ITEMS_SET..InvoiceDailyTimeItem.HOURS) }

                    innerJoin(InvoiceTimeItem.DAILY_ITEMS_SET) {
                        InvoiceSummary::firstDate becomes min { +InvoiceDailyTimeItem.DATE_WORKED }
                    }
                }

                InvoiceSummary::lastDate becomes max { Invoice.TIME_ITEMS_SET..InvoiceTimeItem.DAILY_ITEMS_SET..InvoiceDailyTimeItem.DATE_WORKED }
            }

        val result = query.run().first()

        assertEquals(
                "SELECT COUNT(DISTINCT I.\"id\"), " +
                       "SUM(IDTI.\"hours\"), " +
                       "SUM(ITI.\"hourly_rate\" * IDTI.\"hours\"), " +
                       "MIN(IDTI.\"date_worked\"), " +
                       "MAX(IDTI.\"date_worked\") " +
                "FROM \"invoice\" AS I " +
                "INNER JOIN \"invoice_time_item\" AS ITI ON I.\"id\" = ITI.\"invoice_id\" " +
                "INNER JOIN \"invoice_daily_time_item\" AS IDTI ON ITI.\"id\" = IDTI.\"invoice_time_item_id\"",
                theSql)

        assertEquals(0, theParams.size)

        assertEquals(12L, result.numInvoices)
        assertEquals(42.5.toBigDecimal(), result.hours)
        assertEquals(1521.6.toBigDecimal(), result.totalFee)
        assertEquals("2019-02-01".toLD(), result.firstDate)
        assertEquals("2019-02-27".toLD(), result.lastDate)
    }
}