package com.xs0.dbktx

import si.datastat.db.api.DbEntity
import si.datastat.db.api.MultiColumn
import si.datastat.db.api.util.CompositeId
import java.util.function.Function

class DbTableBuilderC<E : DbEntity<E, ID>, ID : CompositeId<E, ID>> internal constructor(table: DbTable<E, ID>) : DbTableBuilder<E, ID>(table) {

    fun compositeId(constructor: Function<List<Any>, ID>): MultiColumn<E, ID> {
        val id = constructor.apply(dummyRow())

        checkColumns(id)

        val result = MultiColumn(table, constructor, id)

        setIdField(result)

        return result
    }

    private fun checkColumns(id: CompositeId<*, *>) {
        var i = 0
        val n = id.getNumColumns()
        while (i < n) {
            if (id.getColumn(i).getTable() !== table) {
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