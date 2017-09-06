package com.xs0.dbktx

class EntityValues<E : DbEntity<E, *>> {
    private val values: MutableMap<Column<E, *>, Expr<in E, *>> = LinkedHashMap()

    infix fun <T : Any> Column<E, T>.to(value: Expr<in E, T>) {
        values[this] = value
    }

    fun contains(column: Column<E, *>): Boolean {
        return values.contains(column)
    }
}