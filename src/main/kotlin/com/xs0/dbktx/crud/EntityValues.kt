package com.xs0.dbktx.crud

import com.xs0.dbktx.schema.Column
import com.xs0.dbktx.schema.DbEntity

class EntityValues<E : DbEntity<E, *>> : Iterable<Column<E, *>> {
    private val values: MutableMap<Column<E, *>, Any?> = LinkedHashMap()

    infix fun <T : Any> Column<E, T>.to(value: T?) {
        values[this] = value
    }

    fun isEmpty(): Boolean {
        return values.isEmpty()
    }

    override operator fun iterator(): MutableIterator<Column<E, *>> {
        return values.keys.iterator()
    }

    fun <T: Any> get(column: Column<E, T>): T? {
        @Suppress("UNCHECKED_CAST")
        return values[column] as T
    }
}