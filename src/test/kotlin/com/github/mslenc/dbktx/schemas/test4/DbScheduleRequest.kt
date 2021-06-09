package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.*
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.time.LocalDate

class DbScheduleRequest(id: Long, val row: DbRow) : DbEntity<DbScheduleRequest, Long>(id) {

    override val metainfo = TABLE

    val scheduleTimeId: Long get() = SCHEDULE_TIME_ID(row)
    val firstDate: LocalDate get() = FIRST_DATE(row)
    val comment: String? get() = COMMENT(row)

    suspend fun scheduleTime() = SCHEDULE_TIME_REF(this)!!

    companion object TABLE : DbTable<DbScheduleRequest, Long>(TestSchema4, "schedule_request", DbScheduleRequest::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbScheduleRequest::id, primaryKey=true, autoIncrement=true)

        val SCHEDULE_TIME_ID = b.nonNullLong("schedule_time_id", BIGINT(), DbScheduleRequest::scheduleTimeId)
        val FIRST_DATE = b.nonNullDate("first_date", DATE(), DbScheduleRequest::firstDate)
        val COMMENT = b.nullableString("comment", TEXT(), DbScheduleRequest::comment)

        val SCHEDULE_TIME_REF = b.relToOne(SCHEDULE_TIME_ID, DbScheduleTime::class)

        init {
            b.build(::DbScheduleRequest)
        }
    }
}