package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable

class DbTask(db: DbConn, id: Long, row: DbRow)
    : DbEntity<DbTask, Long>(db, id, row) {

    override val metainfo get() = DbTask

    val name: String get() = NAME(row)
    val approverId: Long? get() = APPROVER_ID(row)

    suspend fun approver() = APPROVER_REF(this)

    companion object : DbTable<DbTask, Long>(TestSchema4, "task", DbTask::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbTask::id, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("name", VARCHAR(30), DbTask::name)
        val APPROVER_ID = b.nullableLong("approver_id", BIGINT(), DbTask::approverId)

        val APPROVER_REF = b.relToOne(APPROVER_ID, DbUser::class)

        init {
            b.build(::DbTask)
        }
    }
}