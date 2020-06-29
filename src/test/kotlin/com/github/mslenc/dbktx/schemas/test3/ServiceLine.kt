package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.INT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable


class ServiceLine(db: DbConn, id: Long, row: DbRow) : DbEntity<ServiceLine, Long>(db, id, row) {
    override val metainfo get() = ServiceLine

    val name: String get() = NAME(row)
    val sortIndex: Int? get() = SORT_INDEX(row)
    val parentId: Long? get() = PARENT_ID(row)

    suspend fun parent(): ServiceLine? = PARENT_REF(this)

    companion object : DbTable<ServiceLine, Long>(TestSchema3, "service_line", ServiceLine::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), ServiceLine::id, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("name", VARCHAR(255), ServiceLine::name)
        val SORT_INDEX = b.nullableInt("sort_index", INT(), ServiceLine::sortIndex)
        val PARENT_ID = b.nullableLong("parent_id", BIGINT(), ServiceLine::parentId)

        val PARENT_REF = b.relToOne(PARENT_ID, ServiceLine::class)

        init {
            b.build(::ServiceLine)
        }
    }
}