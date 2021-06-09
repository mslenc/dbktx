package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.INT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable

class DbRole(id: Long, val row: DbRow) : DbEntity<DbRole, Long>(id) {

    override val metainfo get() = DbRole

    val name: String get() = NAME(row)
    val level: Int get() = LEVEL(row)

    companion object : DbTable<DbRole, Long>(TestSchema4, "role", DbRole::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbRole::id, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("name", VARCHAR(30), DbRole::name)
        val LEVEL = b.nonNullInt("level", INT(), DbRole::level)

        init {
            b.build(::DbRole)
        }
    }
}