package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbExecResult
import com.github.mslenc.asyncdb.DbQueryResultObserver
import com.github.mslenc.asyncdb.impl.DbQueryResultImpl
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.crud.filter
import com.github.mslenc.dbktx.schemas.initSchemas
import com.github.mslenc.dbktx.schemas.test2.*
import com.github.mslenc.dbktx.schemas.test3.*
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
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean

class AggregateTest {
    init {
        initSchemas()
    }

    data class CompAggr(
        val weightName: String,
        val averageScore: Double?,
        val minScore: Int?,
        val countryName: String,
        val compName: String,
        val entityCount: Long,
        val entityDistinctCount: Long,
        val avgPlace: Double?,
        val idWeight: Int,
        val idCountry: Int
    )

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

        var weightName: String = ""
        var averageScore: Double? = null
        var minScore: Int? = null
        var countryName: String = ""
        var compName: String = ""
        var entityCount: Long = 0L
        var entityDistinctCount: Long = 0L
        var avgPlace: Double? = null
        var idWeight: Int = 0
        var idCountry: Int = 0

        val result = ArrayList<CompAggr>()

        val query =
            Weight.makeAggregateStreamQuery(db) {
                Weight.NAME into { weightName = it }

                innerJoin(Weight.ENTRIES_SET) {
                    filter {
                        CompEntry.ID_COUNTRY eq 123
                    }

                    average { +CompEntry.FINAL_SCORE } into { averageScore = it }
                    min { +CompEntry.FINAL_SCORE } into { minScore = it }

                    filter {
                        CompEntry.REF_COMPETITION.has {
                            Competition.ENTRIES_SET.contains {
                                CompEntry.ID_WEIGHT oneOf setOf(12, 13, 14)
                            }
                        }
                    }

                    innerJoin(CompEntry.REF_COUNTRY) {
                        innerJoin(Country.REF_LOCALNAME("en")) {
                            LocalName.NAME into { countryName = it }

                            filter {
                                LocalName.NAME.contains("a")
                            }
                        }
                    }

                    innerJoin(CompEntry.REF_COMPETITION) {
                        innerJoin(Competition.REF_LOCALNAME("sl")) {
                            LocalName.NAME into { compName = it }
                            count { +LocalName.ENTITY_ID } into { entityCount = it }
                            countDistinct { +LocalName.ENTITY_ID } into { entityDistinctCount = it }

                            filter {
                                LocalName.NAME.contains("prix")
                            }
                        }
                    }

                    innerJoin(CompEntry.REF_RESULT) {
                        average { +CompResult.PLACE } into { avgPlace = it }
                    }
                }
            }

        query.orderBy(Weight.NAME)

        query.expand {
            Weight.ID_WEIGHT into { idWeight = it }
            innerJoin(Weight.ENTRIES_SET) {
                CompEntry.ID_COUNTRY into { idCountry = it }
            }
        }

        query.onRowEnd {
            result += CompAggr(weightName, averageScore, minScore, countryName, compName, entityCount, entityDistinctCount, avgPlace, idWeight, idCountry)
        }

        val deferred = async { query.execute() }

        Assert.assertFalse(called.get())

        deferred.await()

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
                    "(SELECT DISTINCT CE2.\"id_comp\" FROM \"comp_entries\" AS CE2 " +
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

        var numInvoices: Long = 0
        var hours: BigDecimal? = null
        var totalFee: BigDecimal? = null
        var firstDate: LocalDate? = null
        var lastDate: LocalDate? = null

        val query = Invoice.makeAggregateStreamQuery(db) {
                countDistinct { +Invoice.ID } into { numInvoices = it }

                sum { Invoice.TIME_ITEMS_SET..InvoiceTimeItem.DAILY_ITEMS_SET..InvoiceDailyTimeItem.HOURS } into { hours = it }

                innerJoin(Invoice.TIME_ITEMS_SET) {
                    sum { InvoiceTimeItem.HOURLY_RATE * (InvoiceTimeItem.DAILY_ITEMS_SET..InvoiceDailyTimeItem.HOURS) } into { totalFee = it }

                    innerJoin(InvoiceTimeItem.DAILY_ITEMS_SET) {
                        min { +InvoiceDailyTimeItem.DATE_WORKED } into { firstDate = it }
                    }
                }

                max { Invoice.TIME_ITEMS_SET..InvoiceTimeItem.DAILY_ITEMS_SET..InvoiceDailyTimeItem.DATE_WORKED } into { lastDate = it }
            }

        query.execute()

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

        assertEquals(12L, numInvoices)
        assertEquals(42.5.toBigDecimal(), hours)
        assertEquals(1521.6.toBigDecimal(), totalFee)
        assertEquals("2019-02-01".toLD(), firstDate)
        assertEquals("2019-02-27".toLD(), lastDate)
    }

    @Test
    fun testFormulaQuery() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun streamQuery(sql: String, observer: DbQueryResultObserver, args: List<Any?>) {
                theSql = sql
                theParams = args
                called.set(true)

                println(sql)

                MockResultSet.Builder(
                        "COUNT(DISTINCT I.id)", "COUNT(DISTINCT IEI.id)", "SUM(IDEI.amount)", "SUM((IDEI.amount * (1 + (COALESCE(IDEI.markup_perc, 0) / 100))) + COALESCE(IDEI.markup_amount, 0))"
                ).
                addRow(     3L,                     25L,                      541.6,                            851.44).
                streamInto(observer)
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        var numInvoices: Long = 0
        var numExpenses: Long = 0
        var baseAmount: BigDecimal? = null
        var totalAmount: BigDecimal? = null

        val query =
            Invoice.makeAggregateStreamQuery(db) {
                countDistinct { +Invoice.ID } into { numInvoices = it }
                countDistinct { Invoice.EXPENSE_ITEMS_SET..InvoiceExpenseItem.ID } into { numExpenses = it }

                innerJoin(Invoice.EXPENSE_ITEMS_SET) {
                    filter { InvoiceExpenseItem.BILLABLE eq true }

                    innerJoin(InvoiceExpenseItem.DAILY_ITEMS_SET) {
                        sum { +InvoiceDailyExpenseItem.AMOUNT } into { baseAmount = it }

                        sum { InvoiceDailyExpenseItem.AMOUNT * (1.0.toBigDecimal() + InvoiceDailyExpenseItem.MARKUP_PERC.orZero() / 100.toBigDecimal()) + InvoiceDailyExpenseItem.MARKUP_AMOUNT.orZero() } into { totalAmount = it }
                    }
                }
            }

        query.execute()

        assertEquals(
                "SELECT COUNT(DISTINCT I.\"id\"), " +
                       "COUNT(DISTINCT IEI.\"id\"), " +
                       "SUM(IDEI.\"amount\"), " +
                       "SUM((IDEI.\"amount\" * (1.0 + (COALESCE(IDEI.\"markup_perc\", 0) / 100))) + COALESCE(IDEI.\"markup_amount\", 0)) " +
                "FROM \"invoice\" AS I " +
                "INNER JOIN \"invoice_expense_item\" AS IEI ON I.\"id\" = IEI.\"invoice_id\" " +
                "INNER JOIN \"invoice_daily_expense_item\" AS IDEI ON IEI.\"id\" = IDEI.\"invoice_expense_item_id\" " +
                "WHERE IEI.\"billable\" = TRUE",
                theSql)

        assertEquals(0, theParams.size)

        assertEquals(3L, numInvoices)
        assertEquals(25L, numExpenses)
        assertEquals(541.6.toBigDecimal(), baseAmount)
        assertEquals(851.44.toBigDecimal(), totalAmount)
    }

    @Test
    fun testInsertSelect() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                theSql = sql
                theParams = args
                called.set(true)

                println(sql)

                val result = DbQueryResultImpl(1L, null, null, null)
                return CompletableFuture.completedFuture(result)
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        with (db) {
            InvoiceDailyTimeItem.insertSelect(DailyTimeItem) {
                filter { DailyTimeItem.DATE_WORKED.between(LocalDate.parse("2019-11-01"), LocalDate.parse("2019-11-30")) }

                InvoiceDailyTimeItem.DATE_WORKED becomes DailyTimeItem.DATE_WORKED

                InvoiceDailyTimeItem.HOURS becomes sum { +DailyTimeItem.HOURS }
                InvoiceDailyTimeItem.NB_HOURS becomes BigDecimal("15.55")
                InvoiceDailyTimeItem.TOTAL_HOURS becomes sum { DailyTimeItem.HOURS + DailyTimeItem.NB_HOURS }

                innerJoin(DailyTimeItem.TIME_ITEM_REF) {
                    filter { TimeItem.BILLABLE eq true }
                    filter { TimeItem.TASK_ID eq 554L }

                    InvoiceDailyTimeItem.INVOICE_TIME_ITEM_ID becomes TimeItem.EMPLOYEE_ID // yes, this doesn't really make sense :)
                }
            }
        }

        assertEquals(
            "INSERT INTO \"invoice_daily_time_item\"(\"date_worked\", \"hours\", \"nb_hours\", \"total_hours\", \"invoice_time_item_id\") " +
                     "SELECT DTI.\"date_worked\", SUM(DTI.\"hours\"), 15.55, SUM(DTI.\"hours\" + DTI.\"nb_hours\"), TI.\"employee_id\" " +
                     "FROM \"daily_time_item\" AS DTI " +
                       "INNER JOIN \"time_item\" AS TI ON TI.\"id\" = DTI.\"time_item_id\" " +
                     "WHERE (DTI.\"date_worked\" BETWEEN '2019-11-01' AND '2019-11-30') " +
                       "AND (TI.\"billable\" = TRUE) " +
                       "AND (TI.\"task_id\" = 554) " +
                     "GROUP BY DTI.\"date_worked\", TI.\"employee_id\"",
            theSql)

        assertEquals(0, theParams.size)
    }
}