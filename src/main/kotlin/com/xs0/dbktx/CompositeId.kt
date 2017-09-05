package com.xs0.dbktx

abstract class CompositeId<E : DbEntity<E, ID>, ID : CompositeId<E, ID>> : CompositeExpr<Any, ID> {
    abstract override fun equals(other: Any?): Boolean

    abstract override fun hashCode(): Int

    abstract operator fun <X: Any> get(field: Column<E, X>): X

    abstract val numColumns: Int
    abstract fun getColumn(index: Int): Column<E, *>

    abstract val tableMetainfo: DbTable<E, ID>

    override fun getPart(index: Int): Expr<Any, *> {
        return doGetPart(getColumn(index))
    }

    override val numParts: Int
        get() = numColumns

    private fun <T: Any> doGetPart(column: Column<E, T>): Expr<Any, T> {
        return column.makeLiteral(get(column))
    }

    override fun toString(): String {
        val sb = StringBuilder()
        sb.append("[")
        for (i in 0 until numColumns) {
            if (i > 0)
                sb.append(", ")
            sb.append(get(getColumn(i)))
        }
        return sb.append("]").toString()
    }
}
