package com.xs0.dbktx.crud

import com.xs0.dbktx.composite.CompositeId
import com.xs0.dbktx.expr.CompositeExpr
import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.schema.*
import com.xs0.dbktx.util.Sql
import java.util.*

// the policy over joins is this:
// - SUB_QUERY is only used for referenced filters on to-one relations, like REF_PERSON.has { GIVEN_NAME
//   startsWith "John" }, which becomes ... WHERE id_person IN (SELECT id_person FROM persons WHERE
//   given_name LIKE 'John%').
//   We normally prefer that over joins, because it's faster and allows more indices to be used; however,
//   if we're doing a join anyway, then there's no benefit and we filter on the joined table instead
//   (so SUB_QUERY becomes INNER_JOIN)
// - LEFT_JOIN is used when we're ordering by a referenced table (because doing a sub-query in that case
//   is like a thousand times slower); we use a left join because we don't want ordering to alter the
//   filtering, like it happens with an inner join. However, if we're doing an inner join anyway for other
//   reasons, the filtering is already in place and so LEFT_JOIN becomes INNER_JOIN instead


enum class JoinType {
    INNER_JOIN,
    LEFT_JOIN,
    SUB_QUERY
}

data class Join(var joinType: JoinType, val relToOne: RelToOne<*, *>) {
    fun combineWithJoinType(joinType: JoinType) {
        if (this.joinType == joinType)
            return

        // all three possible pairs combine into INNER_JOIN..
        this.joinType = JoinType.INNER_JOIN
    }
}

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
    private val joins = LinkedList<JoinedTableInQuery<*>>()

    fun <R: DbEntity<R, *>> forcedSubQuery(rel: RelToMany<E, R>): TableInQuery<R> {
        val alias = generateAliasTo(query, rel.targetTable)
        val joinedTable = SubTableInQuery(query, alias, rel.targetTable, this)
        query.registerTableInQuery(joinedTable)
        return joinedTable
    }

    fun <R: DbEntity<R, *>> leftJoin(rel: RelToOne<E, R>): TableInQuery<R> {
        return join(JoinType.LEFT_JOIN, rel)
    }

    fun <R: DbEntity<R, *>> innerJoin(rel: RelToOne<E, R>): TableInQuery<R> {
        return join(JoinType.INNER_JOIN, rel)
    }

    fun <R: DbEntity<R, *>> subQueryOrJoin(rel: RelToOne<E, R>): TableInQuery<R> {
        return join(JoinType.SUB_QUERY, rel)
    }


    private fun <R: DbEntity<R, *>> join(joinType: JoinType, rel: RelToOne<E, R>): TableInQuery<R> {

        val existing = joins.firstOrNull { it.join.relToOne === rel }

        @Suppress("UNCHECKED_CAST")
        if (existing != null) {
            existing.join.combineWithJoinType(joinType)
            return existing as TableInQuery<R>
        }

        val join = Join(joinType, rel)
        val alias = generateAliasTo(query, rel.targetTable)
        val joinedTable = JoinedTableInQuery(query, alias, rel.targetTable, this, join)
        query.registerTableInQuery(joinedTable)
        return joinedTable
    }
}

internal class JoinedTableInQuery<E: DbEntity<E, *>>(query: QueryImpl, tableAlias: String, table: DbTable<E, *>,
                                                     val prevTable: TableInQuery<*>, val join: Join)
    : TableInQuery<E>(query, tableAlias, table) {

}


internal class BaseTableInQuery<E: DbEntity<E, *>>(query: QueryImpl, table: DbTable<E, *>)
    : TableInQuery<E>(query, table.aliasPrefix, table)

internal class SubTableInQuery<E: DbEntity<E, *>>(query: QueryImpl, tableAlias: String, table: DbTable<E, *>, val prevTable: TableInQuery<*>)
    : TableInQuery<E>(query, tableAlias, table)


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