package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable

class DbTimeType(db: DbConn, id: Long, row: DbRow)
    : DbEntity<DbTimeType, Long>(db, id, row) {

    override val metainfo get() = DbTimeType

    val name: String get() = NAME(row)

    companion object : DbTable<DbTimeType, Long>(TestSchema4, "time_type", DbTimeType::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbTimeType::id, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("name", VARCHAR(30), DbTimeType::name)

        init {
            b.build(::DbTimeType)
        }
    }
}