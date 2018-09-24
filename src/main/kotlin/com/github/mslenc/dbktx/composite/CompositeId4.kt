package com.github.mslenc.dbktx.composite

import com.github.mslenc.asyncdb.common.RowData
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.NonNullColumn
import com.github.mslenc.dbktx.util.Sql

abstract class CompositeId4<E : DbEntity<E, ID>, T1: Any, T2: Any, T3: Any, T4: Any, ID : CompositeId4<E, T1, T2, T3, T4, ID>>
    private constructor()
    : CompositeId<E, ID>() {
    abstract val column1: NonNullColumn<E, T1>
    abstract val column2: NonNullColumn<E, T2>
    abstract val column3: NonNullColumn<E, T3>
    abstract val column4: NonNullColumn<E, T4>

    lateinit var component1: T1 private set
    lateinit var component2: T2 private set
    lateinit var component3: T3 private set
    lateinit var component4: T4 private set

    protected constructor(row: RowData) : this() {
        this.component1 = column1(row)
        this.component2 = column2(row)
        this.component3 = column3(row)
        this.component4 = column4(row)
    }

    protected constructor(val1: T1, val2: T2, val3: T3, val4: T4) : this() {
        this.component1 = val1
        this.component2 = val2
        this.component3 = val3
        this.component4 = val4
    }

    @Suppress("UNCHECKED_CAST")
    override operator fun <X: Any> get(column: NonNullColumn<E, X>): X {
        return when {
            column === column1 -> component1 as X
            column === column2 -> component2 as X
            column === column3 -> component3 as X
            column === column4 -> component4 as X
            else -> throw IllegalArgumentException()
        }
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.paren {
            column1.sqlType.toSql(component1, this)
            +", "
            column2.sqlType.toSql(component2, this)
            +", "
            column3.sqlType.toSql(component3, this)
            +", "
            column4.sqlType.toSql(component4, this)
        }
    }

    override val numColumns: Int
        get() = 4

    override fun getColumn(index: Int): NonNullColumn<E, *> {
        return when (index) {
            1 -> column1
            2 -> column2
            3 -> column3
            4 -> column4

            else -> throw IllegalArgumentException(index.toString() + " is not a valid index")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other === null) return false
        if (this::class != other::class) return false

        other as CompositeId4<*, *, *, *, *, *>

        if (component1 != other.component1) return false
        if (component2 != other.component2) return false
        if (component3 != other.component3) return false
        if (component4 != other.component4) return false

        return true
    }

    override fun hashCode(): Int {
        var result = 0
        result = 31 * result + component1.hashCode()
        result = 31 * result + component2.hashCode()
        result = 31 * result + component3.hashCode()
        result = 31 * result + component4.hashCode()
        return result
    }
}