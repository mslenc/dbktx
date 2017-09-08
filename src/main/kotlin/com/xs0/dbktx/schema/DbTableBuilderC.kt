package com.xs0.dbktx.schema

import com.xs0.dbktx.composite.CompositeId
import kotlin.reflect.KClass

class DbTableBuilderC<E : DbEntity<E, ID>, ID : CompositeId<E, ID>> internal constructor(table: DbTable<E, ID>) : DbTableBuilder<E, ID>(table) {

    fun compositeId(constructor: (List<Any?>)->ID): MultiColumn<E, ID> {
        val id = constructor(dummyRow())

        checkColumns(id)

        val result: MultiColumn<E, ID> = MultiColumn(constructor, id)
        val idClass: KClass<out ID> = id::class

        setIdField(result, idClass)

        return result
    }

    private fun checkColumns(id: CompositeId<*, *>) {
        var i = 0
        val n = id.numColumns
        while (i < n) {
            if (id.getColumn(i).table !== table) {
                throw AssertionError("Table mismatch")
            }

            for (j in i + 1 until n) {
                if (id.getColumn(i) === id.getColumn(j)) {
                    throw AssertionError("Column " + id.getColumn(i) + " repeats")
                }
            }
            i++
        }
    }
}