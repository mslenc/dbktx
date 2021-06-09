package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable

class DbScheduleRequestMapping(id: Long, val row: DbRow) : DbEntity<DbScheduleRequestMapping, Long>(id) {

    override val metainfo get() = TABLE

    val timeTypeId: Long get() = TIME_TYPE_ID(row)
    val employmentProfileId: Long get() = EMPLOYMENT_PROFILE_ID(row)
    val internalTaskId: Long get() = INTERNAL_TASK_ID(row)

    suspend fun timeType() = TIME_TYPE_REF(this)!!
    suspend fun employmentProfile() = EMPLOYMENT_PROFILE_REF(this)!!
    suspend fun internalTask() = INTERNAL_TASK_REF(this)!!

    companion object TABLE : DbTable<DbScheduleRequestMapping, Long>(TestSchema4, "schedule_request_mapping", DbScheduleRequestMapping::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbScheduleRequestMapping::id, primaryKey=true, autoIncrement=true)

        val TIME_TYPE_ID = b.nonNullLong("time_type_id", BIGINT(), DbScheduleRequestMapping::timeTypeId)
        val EMPLOYMENT_PROFILE_ID = b.nonNullLong("employment_profile_id", BIGINT(), DbScheduleRequestMapping::employmentProfileId)
        val INTERNAL_TASK_ID = b.nonNullLong("internal_task_id", BIGINT(), DbScheduleRequestMapping::internalTaskId)

        val TIME_TYPE_REF = b.relToOne(TIME_TYPE_ID, DbTimeType::class)
        val EMPLOYMENT_PROFILE_REF = b.relToOne(EMPLOYMENT_PROFILE_ID, DbEmploymentProfile::class)
        val INTERNAL_TASK_REF = b.relToOne(INTERNAL_TASK_ID, DbTask::class) // NonClientTask


        init {
            b.build(::DbScheduleRequestMapping)
        }
    }

}