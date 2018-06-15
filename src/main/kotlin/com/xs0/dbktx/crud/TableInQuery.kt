package com.xs0.dbktx.crud

import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable
import com.xs0.dbktx.schema.RelToOne

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
