package com.xs0.dbktx.crud

import com.xs0.dbktx.composite.CompositeId
import com.xs0.dbktx.expr.CompositeExpr
import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.schema.*
import com.xs0.dbktx.util.Sql

enum class JoinType {
    LEFT_JOIN,
    INNER_JOIN
}

data class Join(val joinType: JoinType, val relToOne: RelToOne<*, *>)

internal fun generateAliasTo(query: QueryImpl, table: DbTable<*, *>): String {
    var counter = 1
    var name = table.aliasPrefix

    while (true) {
        if (query.isTableAliasTaken(name)) {
            name = table.aliasPrefix + ++counter
        } else {
            return name
        }
    }
}

sealed class TableInQuery<E : DbEntity<E, *>>(val query: QueryImpl, val tableAlias: String, val table: DbTable<E, *>) {
    val joins = LinkedHashMap<Join, TableInQuery<*>>()

    fun <R: DbEntity<R, *>> leftJoin(rel: RelToOne<E, R>): TableInQuery<R> {
        return join(JoinType.LEFT_JOIN, rel)
    }

    fun <R: DbEntity<R, *>> innerJoin(rel: RelToOne<E, R>): TableInQuery<R> {
        return join(JoinType.INNER_JOIN, rel)
    }

    fun <R: DbEntity<R, *>>
        join(joinType: JoinType, rel: RelToOne<E, R>): TableInQuery<R> {

        val join = Join(joinType, rel)
        val existing = joins[join]

        @Suppress("UNCHECKED_CAST")
        if (existing != null) {
            return existing as TableInQuery<R>
        }

        val alias = generateAliasTo(query, rel.targetTable)
        val joinedTable = JoinedTableInQuery(query, alias, rel.targetTable, this, join)
        query.registerTableInQuery(joinedTable)
        return joinedTable
    }
}

internal class JoinedTableInQuery<E: DbEntity<E, *>>(query: QueryImpl, tableAlias: String, table: DbTable<E, *>,
                                                     val prevTable: TableInQuery<*>, val join: Join)
    : TableInQuery<E>(query, tableAlias, table)


internal class BaseTableInQuery<E: DbEntity<E, *>>(query: QueryImpl, table: DbTable<E, *>)
    : TableInQuery<E>(query, table.aliasPrefix, table)


class BoundColumnForSelect<E: DbEntity<E, *>, T : Any>(val column: Column<E, T>, val tableInQuery: TableInQuery<E>) : Expr<E, T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw(tableInQuery.tableAlias)
        sql.raw(".")
        sql.raw(column.quotedFieldName)
    }
}

class BoundMultiColumnForSelect<E : DbEntity<E, ID>, ID : CompositeId<E, ID>>(val multiColumn: MultiColumn<E, ID>, val tableInQuery: TableInQuery<E>) : CompositeExpr<E, ID> {
    override val numParts: Int
        get() = multiColumn.numColumns

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.paren {
            sql.tuple(1..numParts) { colIdx ->
                sql.raw(tableInQuery.tableAlias)
                sql.raw(".")
                sql.raw(multiColumn.getPart(colIdx).quotedFieldName)
            }
        }
    }

    override fun getPart(index: Int): Expr<E, *> {
        return BoundColumnForSelect(multiColumn.getPart(index), tableInQuery)
    }
}