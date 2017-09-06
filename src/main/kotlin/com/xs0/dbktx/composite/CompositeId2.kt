package com.xs0.dbktx.composite

import com.xs0.dbktx.DbEntity
import com.xs0.dbktx.NonNullSimpleColumn
import com.xs0.dbktx.SqlBuilder

abstract class CompositeId2<E : DbEntity<E, ID>, A: Any, B: Any, ID : CompositeId2<E, A, B, ID>>
    private constructor()
    : CompositeId<E, ID>() {
    abstract val columnA: NonNullSimpleColumn<E, A>
    abstract val columnB: NonNullSimpleColumn<E, B>

    lateinit var partA: A
    lateinit var partB: B

    protected constructor(row: List<Any?>) : this() {
        this.partA = columnA.from(row)
        this.partB = columnB.from(row)
    }

    protected constructor(partA: A, partB: B) : this() {
        this.partA = partA
        this.partB = partB
    }

    @Suppress("UNCHECKED_CAST")
    operator fun <X: Any> get(field: NonNullSimpleColumn<E, X>): X {
        if (field === columnA) return partA as X
        if (field === columnB) return partB as X
        throw IllegalArgumentException()
    }

    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("(")
        columnA.sqlType.toSql(partA, sb, true)
        sb.sql(", ")
        columnB.sqlType.toSql(partB, sb, true)
        sb.sql(")")
    }

    override val numColumns: Int
        get() = 2

    override fun getColumn(index: Int): NonNullSimpleColumn<E, *> {
        when (index) {
            0 -> return columnA
            1 -> return columnB

            else -> throw IllegalArgumentException(index.toString() + " is not a valid index")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CompositeId2<*, *, *, *>

        if (partA != other.partA) return false
        if (partB != other.partB) return false

        return true
    }

    override fun hashCode(): Int {
        var result = 0
        result = 31 * result + partA.hashCode()
        result = 31 * result + partB.hashCode()
        return result
    }
}
