package com.github.mslenc.dbktx.schema

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.DbUpdate

abstract class DbEntity<E : DbEntity<E, ID>, ID: Any>(
    val db: DbConn,
    val id: ID,
    val row: DbRow
) {

    abstract val metainfo: DbTable<E, ID>

    fun createUpdate(): DbUpdate<E> {
        return metainfo.updateById(db, id)
    }

    suspend fun executeUpdate(modifier: (DbUpdate<E>) -> Unit): Boolean {
        val update = createUpdate()
        modifier(update)
        return update.execute() > 0
    }

    override fun toString(): String {
        val sb = StringBuilder()
        sb.append(metainfo.dbName).append('(')
        var first = true
        for (column in metainfo.columns) {
            if (first) {
                first = false
            } else {
                sb.append(", ")
            }

            val value = column.invoke(row).toString()

            val shortValue = if (value.length <= 40) {
                value
            } else {
                value.substring(0..17) + "[...]" + value.substring(value.length - 18)
            }

            sb.append(column.fieldName).append('=').append(shortValue)
        }
        sb.append(')')
        return sb.toString()
    }
}
