package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable

class DbScheduleTime(id: Long, val row: DbRow) : DbEntity<DbScheduleTime, Long>(id) {

    override val metainfo get() = DbScheduleTime

    val userId: Long get() = USER_ID(row)
    val timeTypeId: Long get() = TIME_TYPE_ID(row)

    suspend fun user() = USER_REF(this)!!
    suspend fun timeType() = TIME_TYPE_REF(this)!!

    companion object : DbTable<DbScheduleTime, Long>(TestSchema4, "schedule_time", DbScheduleTime::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbScheduleTime::id, primaryKey = true, autoIncrement = true)
        val USER_ID = b.nonNullLong("user_id", BIGINT(), DbScheduleTime::userId)
        val TIME_TYPE_ID = b.nonNullLong("time_type_id", BIGINT(), DbScheduleTime::timeTypeId)

        val USER_REF = b.relToOne(USER_ID, DbUser::class)
        val TIME_TYPE_REF = b.relToOne(TIME_TYPE_ID, DbTimeType::class)

        init {
            b.build(::DbScheduleTime)
        }
    }
}