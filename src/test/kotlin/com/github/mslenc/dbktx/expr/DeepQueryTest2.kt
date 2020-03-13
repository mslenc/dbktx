package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.queryAll
import com.github.mslenc.dbktx.runMysqlTest
import com.github.mslenc.dbktx.schemas.initSchemas
import com.github.mslenc.dbktx.schemas.test4.*
import com.github.mslenc.dbktx.util.BatchingLoader
import com.github.mslenc.utils.CachedAsync
import com.github.mslenc.utils.getLogger
import kotlinx.coroutines.*
import org.junit.Test
import java.math.BigDecimal
import java.time.LocalDate

val FOREVER = LocalDate.of(2199, 12, 31)
val log = getLogger<DeepQueryTest2>()

class DeepQueryTest2 {
    init {
        initSchemas()
    }

    // this s

    @Test
    fun testDeepQuery2() = runMysqlTest { db -> supervisorScope {
        val ctx = Ctx(db, 17)

        val request = db.loadById(DbScheduleRequest, 1825)

        listOf(
            async(start = CoroutineStart.UNDISPATCHED) { getMayApprove(request, ctx) },
            async(start = CoroutineStart.UNDISPATCHED) { getExpectedApprover(request, ctx) }
        ).awaitAll()
    } }

    @Test
    fun testDeepQuery2Delayed() = runMysqlTest { db -> supervisorScope {
        val ctx = Ctx(db, 17)

        val request = db.loadById(DbScheduleRequest, 1825)

        listOf(
            async { getMayApprove(request, ctx) },
            async { getExpectedApprover(request, ctx) }
        ).awaitAll()
    } }

    suspend fun getMayApprove(request: DbScheduleRequest, ctx: Ctx): Boolean {
        log.info("Starting getMayApprove")
        try {
            return when {
                ctx.getViewerLevel() >= 100 -> true
                ctx.viewerId == getExpectedApprover(request, ctx)?.id -> true
                else -> false
            }
        } finally {
            log.info("Ending getMayApprove")
        }
    }

    suspend fun getExpectedApprover(request: DbScheduleRequest, ctx: Ctx): DbUser? {
        val nonbillable = request.scheduleTime()

        val taskId = ctx.db.load(TaskBatchLoader(ctx), TaskBatchKey(nonbillable.userId, nonbillable.timeTypeId, request.firstDate))
        val taskApprover = taskId?.let { ctx.db.loadById(DbTask, it) }?.approver()
        taskApprover?.let { return it }

        val manager = request.scheduleTime().user().reportsTo()
        manager?.let { return it }

        return null
    }


    class Ctx(val db: DbConn, val viewerId: Long) {
        private val _payRateLookup = CachedAsync { PayRateLookup.loadEverything(this) }

        suspend fun payRateLookup() = _payRateLookup.get()

        suspend fun getViewerLevel() = db.loadById(DbUser, viewerId).roleLevel()
    }

    data class PayRateInfo(
        val hourlyRate: BigDecimal,
        val payRateId: Long,
        val employmentProfileId: Long
    )

    data class PayRateDateRange(
        val startDate: LocalDate,
        val endDate: LocalDate,
        val payRate: PayRateInfo
    )

    class EmployeePayRateLookup(rates: List<DbPayRate>, profiles: Map<Long, DbEmploymentProfile>) {
        val dateRanges: List<PayRateDateRange>

        init {
            val sortedRates = ArrayList(rates)
            sortedRates.sortWith(compareBy({ it.dateEffective.toEpochDay() }, { it.id }))

            val ranges = ArrayList<PayRateDateRange>()

            for (i in sortedRates.indices) {
                val rate = sortedRates[i]
                val startDate = rate.dateEffective
                val endDate = sortedRates.getOrNull(i + 1)?.dateEffective?.minusDays(1) ?: FOREVER
                val profile = profiles.getValue(rate.profileId)
                val hourlyRate = rate.hourlyRate

                ranges.add(
                    PayRateDateRange(
                        startDate = startDate,
                        endDate = endDate,
                        payRate = PayRateInfo(
                            hourlyRate = hourlyRate,
                            payRateId = rate.id,
                            employmentProfileId = profile.id
                        )
                    ))
            }

            this.dateRanges = ranges.apply { reverse() }
        }

        fun getBasePayRate(date: LocalDate): PayRateInfo? {
            return dateRanges.firstOrNull { date >= it.startDate && date <= it.endDate }?.payRate
        }
    }

    class PayRateLookup private constructor(private val lookups: Map<Long, EmployeePayRateLookup>) {
        fun getEmploymentProfileId(employeeId: Long, date: LocalDate): Long? {
            return lookups[employeeId]?.getBasePayRate(date)?.employmentProfileId
        }

        companion object {
            suspend fun loadEverything(ctx: Ctx): PayRateLookup {
                val payRates = DbPayRate.queryAll(ctx.db).groupBy { it.userId }
                val profiles = DbEmploymentProfile.queryAll(ctx.db).associateBy { it.id }
                val employees = ctx.db.loadByIds(DbUser, payRates.keys)

                val lookups = employees.mapValues { (empId, _) ->
                    EmployeePayRateLookup(payRates[empId] ?: emptyList(), profiles)
                }

                return PayRateLookup(lookups)
            }
        }
    }

    data class TaskBatchKey(val employeeId: Long, val timeTypeId: Long, val date: LocalDate)

    data class TaskBatchLoader(val ctx: Ctx) : BatchingLoader<TaskBatchKey, Long?> {
        override suspend fun loadNow(keys: Set<TaskBatchKey>, db: DbConn): Map<TaskBatchKey, Long?> {
            val payRateLookup = ctx.payRateLookup()

            val mappings = HashMap<Pair<Long, Long>, Long>()
            val dbMappings = DbScheduleRequestMapping.queryAll(ctx.db)

            dbMappings.forEach { mapping ->
                mappings[Pair(mapping.employmentProfileId, mapping.timeTypeId)] = mapping.internalTaskId
            }

            return keys.associateWith { key ->
                payRateLookup.getEmploymentProfileId(key.employeeId, key.date)?.let { profileId ->
                    mappings[Pair(profileId, key.timeTypeId)]
                }
            }
        }

        override fun nullResult() = null
    }
}