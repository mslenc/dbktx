package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.composite.CompositeId
import com.github.mslenc.dbktx.expr.CompositeExpr
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql
import java.lang.UnsupportedOperationException
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

sealed class Join(var joinType: JoinType) {
    abstract fun combineWithJoinType(joinType: JoinType)
}

class JoinToOne(joinType: JoinType, val relToOne: RelToOne<*, *>) : Join(joinType) {
    override fun combineWithJoinType(joinType: JoinType) {
        if (this.joinType == joinType)
            return

        // all three possible pairs combine into INNER_JOIN..
        this.joinType = JoinType.INNER_JOIN
    }
}

class JoinToMany(joinType: JoinType, val relToMany: RelToMany<*, *>): Join(joinType) {
    override fun combineWithJoinType(joinType: JoinType) {
        if (this.joinType == joinType)
            return

        // only one possible pair, which combines into INNER_JOIN..
        this.joinType = JoinType.INNER_JOIN
    }
}

class JoinToZeroOrOne(joinType: JoinType, val relToZeroOrOne: RelToZeroOrOne<*, *>): Join(joinType) {
    override fun combineWithJoinType(joinType: JoinType) {
        if (this.joinType == joinType)
            return

        // only one possible pair, which combines into INNER_JOIN..
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

sealed class TableInQuery<E : DbEntity<E, *>>(val query: QueryImpl, val tableAlias: String, val table: DbTable<E, *>, open val incomingJoin: Join?) {
    internal val joins = LinkedList<JoinedTableInQuery<*>>()

    fun <R: DbEntity<R, *>> forcedSubQuery(rel: RelToMany<E, R>): TableInQuery<R> {
        val alias = generateAliasTo(query, rel.targetTable)
        val joinedTable = SubTableInQuery(query, alias, rel.targetTable, this)
        query.registerTableInQuery(joinedTable)
        return joinedTable
    }

    fun <R: DbEntity<R, *>> forcedSubQuery(rel: RelToZeroOrOne<E, R>): TableInQuery<R> {
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

    fun <R: DbEntity<R, *>> leftJoin(rel: RelToMany<E, R>): TableInQuery<R> {
        return join(JoinType.LEFT_JOIN, rel)
    }

    fun <R: DbEntity<R, *>> innerJoin(rel: RelToMany<E, R>): TableInQuery<R> {
        return join(JoinType.INNER_JOIN, rel)
    }

    fun <R: DbEntity<R, *>> leftJoin(rel: RelToZeroOrOne<E, R>): TableInQuery<R> {
        return join(JoinType.LEFT_JOIN, rel)
    }

    fun <R: DbEntity<R, *>> innerJoin(rel: RelToZeroOrOne<E, R>): TableInQuery<R> {
        return join(JoinType.INNER_JOIN, rel)
    }

    fun <R: DbEntity<R, *>> leftJoin(rel: RelToSingle<E, R>): TableInQuery<R> {
        return when (rel) {
            is RelToOne -> join(JoinType.LEFT_JOIN, rel)
            is RelToZeroOrOne -> join(JoinType.LEFT_JOIN, rel)
            else -> throw UnsupportedOperationException("Unknown RelToSingle type")
        }
    }

    fun <R: DbEntity<R, *>> innerJoin(rel: RelToSingle<E, R>): TableInQuery<R> {
        return when (rel) {
            is RelToOne -> join(JoinType.INNER_JOIN, rel)
            is RelToZeroOrOne -> join(JoinType.INNER_JOIN, rel)
            else -> throw UnsupportedOperationException("Unknown RelToSingle type")
        }
    }

    fun <R: DbEntity<R, *>> subQueryOrJoin(rel: RelToOne<E, R>): TableInQuery<R> {
        return join(JoinType.SUB_QUERY, rel)
    }

    fun <R: DbEntity<R, *>> subQueryOrJoin(rel: RelToZeroOrOne<E, R>): TableInQuery<R> {
        return join(JoinType.SUB_QUERY, rel)
    }


    private fun <R: DbEntity<R, *>> join(joinType: JoinType, rel: RelToOne<E, R>): TableInQuery<R> {

        val existing = joins.firstOrNull { (it.incomingJoin as? JoinToOne)?.relToOne === rel }

        @Suppress("UNCHECKED_CAST")
        if (existing != null) {
            existing.incomingJoin.combineWithJoinType(joinType)
            return existing as TableInQuery<R>
        }

        val join = JoinToOne(joinType, rel)
        val alias = generateAliasTo(query, rel.targetTable)
        val joinedTable = JoinedTableInQuery(query, alias, rel.targetTable, this, join)
        query.registerTableInQuery(joinedTable)
        joins.add(joinedTable)
        return joinedTable
    }

    private fun <R: DbEntity<R, *>> join(joinType: JoinType, rel: RelToZeroOrOne<E, R>): TableInQuery<R> {

        val existing = joins.firstOrNull { (it.incomingJoin as? JoinToZeroOrOne)?.relToZeroOrOne === rel }

        @Suppress("UNCHECKED_CAST")
        if (existing != null) {
            existing.incomingJoin.combineWithJoinType(joinType)
            return existing as TableInQuery<R>
        }

        val join = JoinToZeroOrOne(joinType, rel)
        val alias = generateAliasTo(query, rel.targetTable)
        val joinedTable = JoinedTableInQuery(query, alias, rel.targetTable, this, join)
        query.registerTableInQuery(joinedTable)
        joins.add(joinedTable)
        return joinedTable
    }

    private fun <R: DbEntity<R, *>> join(joinType: JoinType, rel: RelToMany<E, R>): TableInQuery<R> {

        val existing = joins.firstOrNull { (it.incomingJoin as? JoinToMany)?.relToMany === rel }

        @Suppress("UNCHECKED_CAST")
        if (existing != null) {
            existing.incomingJoin.combineWithJoinType(joinType)
            return existing as TableInQuery<R>
        }

        val join = JoinToMany(joinType, rel)
        val alias = generateAliasTo(query, rel.targetTable)
        val joinedTable = JoinedTableInQuery(query, alias, rel.targetTable, this, join)
        query.registerTableInQuery(joinedTable)
        joins.add(joinedTable)
        return joinedTable
    }

    // this guy is here just because of type inference failure
    internal fun remap(tableRemapper: TableRemapper): TableInQuery<E> {
        return tableRemapper.remap(this)
    }

    internal fun toString(level: Int, sb: StringBuilder) {
        for (i in 0 until level)
            sb.append("    ")
        sb.append(table.dbName).append(" AS ").append(tableAlias).append(" (").append(incomingJoin?.joinType ?: "").append(")\n")
        for (join in joins) {
            join.toString(level + 1, sb)
        }
    }

    override fun toString(): String {
        val sb = StringBuilder()
        toString(0, sb)
        return sb.toString()
    }

    internal fun addRemappedJoin(newJoinedTable: JoinedTableInQuery<*>) {
        joins.add(newJoinedTable)
    }
}

internal class JoinedTableInQuery<E: DbEntity<E, *>>(query: QueryImpl, tableAlias: String, table: DbTable<E, *>,
                                                     val prevTable: TableInQuery<*>, incomingJoin: Join)
    : TableInQuery<E>(query, tableAlias, table, incomingJoin) {

    override val incomingJoin: Join get() = super.incomingJoin!!
}

internal class BaseTableInQuery<E: DbEntity<E, *>>(query: QueryImpl, table: DbTable<E, *>)
    : TableInQuery<E>(query, table.aliasPrefix, table, null)

internal class SubTableInQuery<E: DbEntity<E, *>>(query: QueryImpl, tableAlias: String, table: DbTable<E, *>, val prevTable: TableInQuery<*>)
    : TableInQuery<E>(query, tableAlias, table, null)

internal class BaseTableInUpdateQuery<E: DbEntity<E, *>>(query: QueryImpl, table: DbTable<E, *>)
    : TableInQuery<E>(query, "", table, null)



class BoundColumnForSelect<E: DbEntity<E, *>, T : Any>(val column: Column<E, T>, val tableInQuery: TableInQuery<E>) : Expr<T> {
    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        if (tableInQuery.tableAlias.isNotEmpty()) {
            sql.raw(tableInQuery.tableAlias)
            sql.raw(".")
        }
        sql.raw(column.quotedFieldName)
    }

    override val couldBeNull: Boolean
        get() = column.nullable

    override val sqlType: SqlType<T>
        get() = column.sqlType

    override val involvesAggregation: Boolean
        get() = false

    override fun remap(remapper: TableRemapper): Expr<T> {
        return BoundColumnForSelect(column, remapper.remap(tableInQuery))
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}

class BoundMultiColumnForSelect<E : DbEntity<E, *>, ID : CompositeId<E, ID>>(val multiColumn: MultiColumnKeyDef<E, ID>, val tableInQuery: TableInQuery<E>) : CompositeExpr<E, ID> {
    override val numParts: Int
        get() = multiColumn.numColumns

    override val couldBeNull: Boolean
        get() = false

    override val involvesAggregation: Boolean
        get() = false

    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        sql.paren {
            sql.tuple(1..numParts) { colIdx ->
                if (tableInQuery.tableAlias.isNotEmpty()) {
                    sql.raw(tableInQuery.tableAlias)
                    sql.raw(".")
                }
                sql.raw(multiColumn.getColumn(colIdx).quotedFieldName)
            }
        }
    }

    override fun getPart(index: Int): Expr<*> {
        return BoundColumnForSelect(multiColumn.getColumn(index), tableInQuery)
    }

    override fun remap(remapper: TableRemapper): Expr<ID> {
        return BoundMultiColumnForSelect(multiColumn, remapper.remap(tableInQuery))
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}