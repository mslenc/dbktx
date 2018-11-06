package com.github.mslenc.dbktx.aggr

import com.github.mslenc.dbktx.crud.BoundColumnForSelect
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.SqlEmitter
import com.github.mslenc.dbktx.schema.Column
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql

interface AggregateExpr<E : DbEntity<E, *>, T : Any> {
    fun bind(tableInQuery: TableInQuery<E>, indexInRow: Int): BoundAggregateExpr<T>
}

interface BoundAggregateExpr<T : Any>: SqlEmitter {
    operator fun invoke(row: AggregateRow): T?
}

class BoundColumnExpr<T: Any>(val boundColumn: BoundColumnForSelect<*, T>, val indexInRow: Int) : BoundAggregateExpr<T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        boundColumn.toSql(sql, topLevel)
    }

    override fun invoke(row: AggregateRow): T? {
        val dbValue = row[indexInRow]
        val sqlType = boundColumn.column.sqlType
        if (dbValue.isNull || sqlType.isNullDbValue(dbValue))
            return null

        return sqlType.parseDbValue(dbValue)
    }
}

class BoundColumnAggrExpr<T: Any>(val boundColumn: Expr<*, T>, val sqlType: SqlType<T>, val indexInRow: Int, val type: ColumnAggrExpr.Type) : BoundAggregateExpr<T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw(type.name).raw("(")
        boundColumn.toSql(sql, true)
        sql.raw(")")
    }

    override fun invoke(row: AggregateRow): T? {
        val dbValue = row[indexInRow]
        if (dbValue.isNull || sqlType.isNullDbValue(dbValue))
            return null

        return sqlType.parseDbValue(dbValue)
    }
}

class ColumnAggrExpr<E : DbEntity<E, *>, T : Any>(val column: Column<E, T>, val type: Type) : AggregateExpr<E, T> {
    enum class Type {
        MIN,
        MAX,
        AVG,
        SUM
    }

    override fun bind(tableInQuery: TableInQuery<E>, indexInRow: Int): BoundAggregateExpr<T> {
        val boundColumn = column.bindForSelect(tableInQuery)

        return BoundColumnAggrExpr(boundColumn, column.sqlType, indexInRow, type)
    }
}

class BoundCountColumnExpr(val boundColumn: Expr<*, *>, val indexInRow: Int, val type: CountColumnExpr.Type) : BoundAggregateExpr<Long> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw(when(type) {
            CountColumnExpr.Type.COUNT -> "COUNT ("
            CountColumnExpr.Type.COUNT_DISTINCT -> "COUNT (DISTINCT "
        })
        boundColumn.toSql(sql, true)
        sql.raw(")")
    }

    override fun invoke(row: AggregateRow): Long? {
        val dbValue = row[indexInRow]
        if (dbValue.isNull)
            return null

        return dbValue.asLong()
    }
}

class CountColumnExpr<E : DbEntity<E, *>, T : Any>(val column: Column<E, T>, val type: Type) : AggregateExpr<E, Long> {
    enum class Type {
        COUNT,
        COUNT_DISTINCT
    }

    override fun bind(tableInQuery: TableInQuery<E>, indexInRow: Int): BoundAggregateExpr<Long> {
        val boundColumn = column.bindForSelect(tableInQuery)

        return BoundCountColumnExpr(boundColumn, indexInRow, type)
    }
}