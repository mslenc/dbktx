package com.xs0.dbktx

private fun buildFieldTuple(id: CompositeId<*, *>): String {
    val sb = StringBuilder()
    for (i in 0 until id.numColumns) {
        sb.append(if (i == 0) "(" else ", ")
        sb.append(id.getColumn(i).dbName)
    }
    return sb.append(")").toString()
}

class MultiColumn<E : DbEntity<E, ID>, ID : CompositeId<E, ID>>(
        table: DbTable<E, ID>,
        private val constructor: (List<Any?>) -> ID,
        private val prototype: ID) : RowProp<E, ID>(table, buildFieldTuple(prototype)), CompositeExpr<E, ID> {

    override val numParts: Int
        get() = prototype.numColumns

    override fun getPart(index: Int): Column<E, *> {
        return prototype.getColumn(index)
    }

    override fun eq(value: ID): ExprBoolean<E> {
        return ExprEquals.equals(this, value)
    }

    override fun neq(value: ID): ExprBoolean<E> {
        return ExprEquals.notEquals(this, value)
    }

    override fun oneOf(values: Set<ID>): ExprBoolean<E> {
        return ExprOneOf.oneOf(this, ArrayList(values))
    }

    override fun from(row: List<Any?>): ID {
        return constructor(row)
    }

    override val isAutoGenerated: Boolean
        get() = false

    override fun isSet(values: Map<Column<E, *>, Expr<in E, *>>): Boolean {
        var i = 0
        val n = numParts
        while (i < n) {
            if (!getPart(i).isSet(values))
                return false
            i++
        }

        return true
    }
}