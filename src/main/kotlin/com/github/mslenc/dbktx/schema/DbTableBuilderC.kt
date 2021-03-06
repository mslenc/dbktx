package com.github.mslenc.dbktx.schema

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.composite.CompositeId
import kotlin.reflect.KClass

class DbTableBuilderC<E : DbEntity<E, ID>, ID : CompositeId<E, ID>> internal constructor(table: DbTable<E, ID>) : DbTableBuilder<E, ID>(table) {
    fun primaryKey(constructor: (DbRow)->ID): MultiColumnKeyDef<E, ID> {
        val id = constructor(dummyRow())

        checkColumns(id)

        val result: MultiColumnKeyDef<E, ID> = MultiColumnKeyDef(table, 0, constructor, DbEntity<E, ID>::id, id, true)
        val idClass: KClass<out ID> = id::class

        setPrimaryKey(result, idClass)

        return result
    }

    private fun checkColumns(id: CompositeId<*, *>) {
        for (i in 1 .. id.numColumns) {
            if (id.getColumn(i).table !== table) {
                throw AssertionError("Table mismatch")
            }

            for (j in i+1 .. id.numColumns) {
                if (id.getColumn(i) === id.getColumn(j)) {
                    throw AssertionError("Column " + id.getColumn(i) + " repeats")
                }
            }
        }
    }
}