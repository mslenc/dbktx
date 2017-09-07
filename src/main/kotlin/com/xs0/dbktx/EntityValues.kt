package com.xs0.dbktx

class EntityValues<E : DbEntity<E, *>> {
    private val values: MutableMap<Column<E, *>, Any?> = LinkedHashMap()

    infix fun <T : Any> Column<E, T>.to(value: T?) {
        values[this] = value
    }

    fun isEmpty(): Boolean {
        return values.isEmpty()
    }

    operator fun iterator(): MutableIterator<Column<E, *>> {
        return values.keys.iterator()
    }

    fun <T: Any> get(column: Column<E, T>): T? {
        @Suppress("UNCHECKED_CAST")
        return values[column] as T
    }
}