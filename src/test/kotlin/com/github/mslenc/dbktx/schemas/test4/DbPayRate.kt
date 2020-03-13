package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.DATE
import com.github.mslenc.dbktx.fieldprops.DECIMAL
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.math.BigDecimal
import java.time.LocalDate

class DbPayRate(db: DbConn, id: Long, row: DbRow)
    : DbEntity<DbPayRate, Long>(db, id, row) {

    override val metainfo get() = DbPayRate

    val userId: Long get() = USER_ID(row)
    val dateEffective: LocalDate get() = DATE_EFFECTIVE(row)
    val hourlyRate: BigDecimal get() = HOURLY_RATE(row)
    val profileId: Long get() = PROFILE_ID(row)

    suspend fun user() = USER_REF(this)
    suspend fun profile() = PROFILE_REF(this)

    companion object : DbTable<DbPayRate, Long>(TestSchema4, "pay_rate", DbPayRate::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbPayRate::id, primaryKey = true, autoIncrement = true)
        val USER_ID = b.nonNullLong("user_id", BIGINT(), DbPayRate::userId)
        val DATE_EFFECTIVE = b.nonNullDate("date_effective", DATE(), DbPayRate::dateEffective)
        val HOURLY_RATE = b.nonNullDecimal("hourly_rate", DECIMAL(9, 2), DbPayRate::hourlyRate)
        val PROFILE_ID = b.nonNullLong("profile_id", BIGINT(), DbPayRate::profileId)

        val USER_REF = b.relToOne(USER_ID, DbUser::class)
        val PROFILE_REF = b.relToOne(PROFILE_ID, DbEmploymentProfile::class)

        init {
            b.build(::DbPayRate)
        }
    }
}