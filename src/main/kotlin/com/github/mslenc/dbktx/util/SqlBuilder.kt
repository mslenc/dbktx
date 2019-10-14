package com.github.mslenc.dbktx.util

import com.github.mslenc.asyncdb.DbType
import com.github.mslenc.asyncdb.util.ULong
import com.github.mslenc.dbktx.crud.*
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.expr.SqlEmitter
import com.github.mslenc.dbktx.filters.MatchAnything
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.sqltypes.toHexString
import java.math.BigDecimal
import java.time.*

class Sql(val dbType: DbType) {
    private val sql = StringBuilder()
    val params = ArrayList<Any>()

    fun getSql(): String {
        return sql.toString()
    }

    fun raw(sql: String): Sql {
        this.sql.append(sql)
        return this
    }

    operator fun invoke(value: String): Sql {
        this.sql.append("?")
        this.params.add(value)
        return this
    }

    operator fun invoke(value: Int): Sql {
        this.sql.append(value)
        return this
    }

    operator fun invoke(value: Long): Sql {
        this.sql.append(value)
        return this
    }

    operator fun invoke(value: ULong): Sql {
        this.sql.append(value)
        return this
    }

    operator fun invoke(value: Double): Sql {
        this.sql.append(value)
        return this
    }

    operator fun invoke(value: Float): Sql {
        this.sql.append(value)
        return this
    }

    operator fun invoke(value: BigDecimal): Sql {
        this.sql.append(value.toPlainString())
        return this
    }

    operator fun invoke(value: ByteArray): Sql {
        toHexString(value, "X'", "'", sql)
        return this
    }

    operator fun invoke(param: Instant): Sql {
        return quotedRaw(param.toString())
    }

    operator fun invoke(param: LocalDate): Sql {
        return quotedRaw(param.toString())
    }

    operator fun invoke(param: LocalDateTime): Sql {
        return quotedRaw(param.toString())
    }

    operator fun invoke(param: LocalTime): Sql {
        return quotedRaw(param.toString())
    }

    operator fun invoke(param: Duration): Sql {
        return quotedRaw(formatDuration(param))
    }

    fun columnForSelect(table: TableInQuery<*>, column: Column<*, *>): Sql {
        if (table.tableAlias.isNotEmpty())
            sql.append(table.tableAlias).append('.')
        sql.append(column.quotedFieldName)
        return this
    }

    fun columnForSelect(col: BoundColumnForSelect<*, *>): Sql {
        return columnForSelect(col.tableInQuery, col.column)
    }

    fun columnForUpdate(column: Column<*, *>): Sql {
        sql.append(column.quotedFieldName)
        return this
    }

    operator fun invoke(table: DbTable<*, *>): Sql {
        sql.append(table.quotedDbName)
        return this
    }

    operator fun invoke(sqlEmitter: SqlEmitter, topLevel: Boolean = false): Sql {
        sqlEmitter.toSql(this, topLevel)
        return this
    }

    private fun quotedRaw(text: String): Sql {
        sql.append("'").append(text).append("'")
        return this
    }

    companion object {
        fun quoteIdentifier(ident: String): String {
            val sb = StringBuilder(ident.length + 2)
            sb.append('"')
            for (i in 0 until ident.length) {
                ident[i].let {
                    if (it == '"') {
                        sb.append("\"\"")
                    } else {
                        sb.append(it)
                    }
                }
            }
            sb.append('"')
            return sb.toString()
        }

        fun formatDuration(value: Duration): String {
            val micros = value.nano / 1000
            var seconds = value.seconds
            var minutes = seconds / 60
            seconds %= 60
            val hours = minutes / 60
            minutes %= 60

            return if (micros > 0) {
                String.format("%02d:%02d:%02d.%06d", hours, minutes, seconds, micros)
            } else {
                String.format("%02d:%02d:%02d", hours, minutes, seconds)
            }
        }
    }

    fun SELECT(what: String): Sql {
        return raw("SELECT ").raw(what)
    }

    fun FROM(table: DbTable<*,*>, alias: String): Sql {
        return raw(" FROM ").raw(table.quotedDbName).raw(" AS ").raw(alias)
    }

    fun FROM(tiq: TableInQuery<*>): Sql {
        raw(" FROM ").raw(tiq.table.quotedDbName).raw(" AS ").raw(tiq.tableAlias)

        buildJoins(tiq)

        return this
    }

    private fun buildJoins(tiq: TableInQuery<*>) {
        for (joinedTable in tiq.joins) {
            val joinType = joinedTable.incomingJoin.joinType
            if (joinType == JoinType.SUB_QUERY)
                continue

            when (val join = joinedTable.incomingJoin) {
                is JoinToOne -> buildJoinToOne(joinedTable, join)
                is JoinToMany -> buildJoinToMany(joinedTable, join)
                is JoinToZeroOrOne -> buildJoinToZeroOrOne(joinedTable, join)
            }
        }
    }

    private fun buildJoinToOne(joinedTable: JoinedTableInQuery<*>, incomingJoin: JoinToOne) {
        val rel = incomingJoin.relToOne as RelToOneImpl<*,*,*>
        val sourceTable = joinedTable.prevTable
        val targetTable = rel.targetTable

        if (incomingJoin.joinType == JoinType.INNER_JOIN) {
            raw(" INNER JOIN ")
        } else {
            raw(" LEFT JOIN ")
        }

        raw(targetTable.quotedDbName).raw(" AS ").raw(joinedTable.tableAlias).raw(" ON ")

        tuple(rel.info.columnMappings, separator = " AND ") { colMap ->
            when (colMap.columnFromKind) {
                ColumnInMappingKind.COLUMN -> {
                    columnForSelect(SqlBuilderHelpers.forceBindColumnTo(colMap, joinedTable))
                    raw(" = ")
                    columnForSelect(SqlBuilderHelpers.forceBindColumnFrom(colMap, sourceTable))
                }

                ColumnInMappingKind.CONSTANT,
                ColumnInMappingKind.PARAMETER -> {
                    columnForSelect(SqlBuilderHelpers.forceBindColumnTo(colMap, joinedTable))
                    raw(" = ")
                    colMap.columnFromLiteral.toSql(this)
                }
            }
        }

        buildJoins(joinedTable)
    }

    private fun buildJoinToMany(joinedTable: JoinedTableInQuery<*>, incomingJoin: JoinToMany) {
        val rel = incomingJoin.relToMany as RelToManyImpl<*, *, *>
        val sourceTable = joinedTable.prevTable
        val targetTable = rel.targetTable

        if (incomingJoin.joinType == JoinType.INNER_JOIN) {
            raw(" INNER JOIN ")
        } else {
            raw(" LEFT JOIN ")
        }

        raw(targetTable.quotedDbName).raw(" AS ").raw(joinedTable.tableAlias).raw(" ON ")

        tuple(rel.info.columnMappings, separator = " AND ") { colMap ->
            when (colMap.columnFromKind) {
                ColumnInMappingKind.COLUMN -> {
                    columnForSelect(SqlBuilderHelpers.forceBindColumnTo(colMap, sourceTable))
                    raw(" = ")
                    columnForSelect(SqlBuilderHelpers.forceBindColumnFrom(colMap, joinedTable))
                }

                ColumnInMappingKind.CONSTANT,
                ColumnInMappingKind.PARAMETER -> {
                    columnForSelect(SqlBuilderHelpers.forceBindColumnTo(colMap, sourceTable))
                    raw(" = ")
                    colMap.columnFromLiteral.toSql(this)
                }
            }
        }

        buildJoins(joinedTable)
    }

    private fun buildJoinToZeroOrOne(joinedTable: JoinedTableInQuery<*>, incomingJoin: JoinToZeroOrOne) {
        val rel = incomingJoin.relToZeroOrOne as RelToZeroOrOneImpl<*, *, *>
        val sourceTable = joinedTable.prevTable
        val targetTable = rel.targetTable

        if (incomingJoin.joinType == JoinType.INNER_JOIN) {
            raw(" INNER JOIN ")
        } else {
            raw(" LEFT JOIN ")
        }

        raw(targetTable.quotedDbName).raw(" AS ").raw(joinedTable.tableAlias).raw(" ON ")

        tuple(rel.info.columnMappings, separator = " AND ") { colMap ->
            when (colMap.columnFromKind) {
                ColumnInMappingKind.COLUMN -> {
                    columnForSelect(SqlBuilderHelpers.forceBindColumnTo(colMap, sourceTable))
                    raw(" = ")
                    columnForSelect(SqlBuilderHelpers.forceBindColumnFrom(colMap, joinedTable))
                }

                ColumnInMappingKind.CONSTANT,
                ColumnInMappingKind.PARAMETER -> {
                    columnForSelect(SqlBuilderHelpers.forceBindColumnTo(colMap, sourceTable))
                    raw(" = ")
                    colMap.columnFromLiteral.toSql(this)
                }
            }
        }

        buildJoins(joinedTable)
    }

    fun WHERE(filter: FilterExpr): Sql {
        if (filter != MatchAnything) {
            raw(" WHERE ")
            filter.toSql(this, true)
        }
        return this
    }

    fun GROUP_BY(groupBy: List<SqlEmitter>) {
        if (groupBy.isEmpty())
            return

        raw(" GROUP BY ")
        for ((index, group) in groupBy.withIndex()) {
            if (index > 0)
                raw(", ")
            group.toSql(this, true)
        }
    }

    fun subQueryWrapper(negated: Boolean, needleCanBeNull: Boolean, block: Sql.(String)->Unit) {
        if (needleCanBeNull) {
            when (dbType) {
                DbType.MYSQL -> {
                    if (negated) {
                        raw("(NOT (")
                        this.block(" <=> SOME ")
                        raw("))")
                    } else {
                        this.block(" <=> SOME ")
                    }
                }
                DbType.POSTGRES -> {
                    if (negated) {
                        raw("(TRUE IS DISTINCT FROM (")
                        this.block(" IN ")
                        raw("))")
                    } else {
                        raw("(TRUE IS NOT DISTINCT FROM (")
                        this.block(" IN ")
                        raw("))")
                    }
                }
            }
        } else {
            if (negated) {
                this.block(" NOT IN ")
            } else {
                this.block(" IN ")
            }
        }
    }

    fun inLiteralSetWrapper(negated: Boolean, needleCanBeNull: Boolean, block: Sql.(String)->Unit) {
        if (needleCanBeNull) {
            when (dbType) {
                DbType.MYSQL -> {
                    if (negated) {
                        raw("NOT (TRUE <=> (")
                        this.block(" IN ")
                        raw("))")
                    } else {
                        raw("(TRUE <=> (")
                        this.block(" IN ")
                        raw("))")
                    }
                }
                DbType.POSTGRES -> {
                    if (negated) {
                        raw("(TRUE IS DISTINCT FROM (")
                        this.block(" IN ")
                        raw("))")
                    } else {
                        raw("(TRUE IS NOT DISTINCT FROM (")
                        this.block(" IN ")
                        raw("))")
                    }
                }
            }
        } else {
            if (negated) {
                this.block(" NOT IN ")
            } else {
                this.block(" IN ")
            }
        }
    }

    inline fun paren(showParens: Boolean = true, block: Sql.() -> Unit) {
        if (showParens) raw("(")
        this.block()
        if (showParens) raw(")")
    }

    inline fun expr(topLevel: Boolean, block: Sql.() -> Unit) {
        paren(!topLevel, block)
    }


    inline fun <T> tuple(elements: Iterable<T>, separator: String = ", ", block: (T) -> Unit) {
        var first = true

        for (el in elements) {
            if (first) {
                first = false
            } else {
                raw(separator)
            }

            block(el)
        }

        if (first)
            throw IllegalArgumentException("Empty tuple")
    }

    inline fun <T> tuple(elements: Array<T>, separator: String = ", ", block: (T) -> Unit) {
        var first = true

        for (el in elements) {
            if (first) {
                first = false
            } else {
                raw(separator)
            }

            block(el)
        }

        if (first)
            throw IllegalArgumentException("Empty tuple")
    }

    operator fun String.unaryPlus() {
        raw(this)
    }

    operator fun SqlEmitter.unaryPlus() {
        invoke(this, false)
    }
}
