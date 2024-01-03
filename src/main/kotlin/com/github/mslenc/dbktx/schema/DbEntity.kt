package com.github.mslenc.dbktx.schema

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.EntityQuery
import com.github.mslenc.dbktx.crud.filter
import com.github.mslenc.dbktx.util.getContextDb
import com.github.mslenc.utils.smap

abstract class DbEntity<E : DbEntity<E, ID>, ID: Any>(
    val id: ID,
) {

    abstract val metainfo: DbTable<E, ID>

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

            @Suppress("UNCHECKED_CAST")
            val value = column.invoke(this as E).toString()

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

    suspend fun hasIncomingRefs(db: DbConn = getContextDb()): Boolean {
        return metainfo._incomingRefs.any {
            countInverse(it, db) > 0
        }
    }

    suspend fun findIncomingRefs(nonZeroOnly: Boolean = true, db: DbConn = getContextDb()): List<IncomingRefInfo<E, *>> {
        val infos = metainfo._incomingRefs.smap { makeIncomingRefInfo(it, db) }

        return when (nonZeroOnly) {
            true -> infos.filter { it.count > 0 }
            else -> infos
        }
    }

    private suspend fun <MANY: DbEntity<MANY, *>> makeIncomingRefInfo(ref: RelToOneImpl<MANY, E, *>, db: DbConn): IncomingRefInfo<E, MANY> {
        @Suppress("UNCHECKED_CAST")
        val query = makeInverseQuery(ref, this as E, db)
        return IncomingRefInfoImpl(query.countAll(), ref, query)
    }

    private suspend fun <MANY: DbEntity<MANY, *>> countInverse(ref: RelToOneImpl<MANY, E, *>, db: DbConn): Long {
        @Suppress("UNCHECKED_CAST")
        return makeInverseQuery(ref, this as E, db).countAll()
    }
}

private fun <ONE: DbEntity<ONE, *>, MANY: DbEntity<MANY, *>> makeInverseQuery(ref: RelToOneImpl<MANY, ONE, *>, target: ONE, db: DbConn): EntityQuery<MANY> {
    val query = ref.info.manyTable.newEntityQuery(db)
    query.filter { ref eq target }
    return query
}


interface IncomingRefInfo<ONE: DbEntity<ONE, *>, MANY: DbEntity<MANY, *>> {
    val count: Long
    val ref: RelToOne<MANY, ONE>
    suspend fun getRefs(): List<MANY>
}

class IncomingRefInfoImpl<ONE: DbEntity<ONE, *>, MANY: DbEntity<MANY, *>>(
    override val count: Long,
    override val ref: RelToOne<MANY, ONE>,
    internal val query: EntityQuery<MANY>
) : IncomingRefInfo<ONE, MANY> {
    override suspend fun getRefs(): List<MANY> {
        return query.execute()
    }
}