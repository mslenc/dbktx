package com.github.mslenc.dbktx.composite

import com.github.mslenc.asyncdb.common.RowData
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.NonNullColumn
import com.github.mslenc.dbktx.util.Sql

abstract class CompositeId2<E : DbEntity<E, ID>, T1: Any, T2: Any, ID : CompositeId2<E, T1, T2, ID>>
    private constructor()
    : CompositeId<E, ID>() {
    abstract val column1: NonNullColumn<E, T1>
    abstract val column2: NonNullColumn<E, T2>

    lateinit var component1: T1 private set
    lateinit var component2: T2 private set

    protected constructor(row: RowData) : this() {
        this.component1 = column1(row)
        this.component2 = column2(row)
    }

    protected constructor(val1: T1, val2: T2) : this() {
        this.component1 = val1
        this.component2 = val2
    }

    @Suppress("UNCHECKED_CAST")
    override operator fun <X: Any> get(column: NonNullColumn<E, X>): X {
        return when {
            column === column1 -> component1 as X
            column === column2 -> component2 as X
            else -> throw IllegalArgumentException()
        }
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.paren {
            column1.sqlType.toSql(component1, this)
            +", "
            column2.sqlType.toSql(component2, this)
        }
    }

    override val numColumns: Int
        get() = 2

    override fun getColumn(index: Int): NonNullColumn<E, *> {
        return when (index) {
            1 -> column1
            2 -> column2

            else -> throw IllegalArgumentException(index.toString() + " is not a valid index")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other === null) return false
        if (this::class != other::class) return false

        other as CompositeId2<*, *, *, *>

        if (component1 != other.component1) return false
        if (component2 != other.component2) return false

        return true
    }

    override fun hashCode(): Int {
        var result = 0
        result = 31 * result + component1.hashCode()
        result = 31 * result + component2.hashCode()
        return result
    }
}
