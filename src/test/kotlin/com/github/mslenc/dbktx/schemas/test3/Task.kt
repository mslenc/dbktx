package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable


class Task(id: Long, val row: DbRow) : DbEntity<Task, Long>(id) {
    override val metainfo get() = Task

    val name: String get() = NAME(row)

    companion object : DbTable<Task, Long>(TestSchema3, "task", Task::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), Task::id, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("first_name", VARCHAR(255), Task::name)

        init {
            b.build(::Task)
        }
    }
}