package com.xs0.dbktx.util

import com.xs0.dbktx.crud.JoinType
import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.expr.SqlEmitter
import com.xs0.dbktx.schema.Column
import com.xs0.dbktx.schema.DbTable
import com.xs0.dbktx.schema.RelToOneImpl
import com.xs0.dbktx.sqltypes.toHexString
import io.vertx.core.json.JsonArray
import java.math.BigDecimal
import java.time.*
import java.util.*

class Sql {
    private val sql = StringBuilder()
    val params = JsonArray()
    private val tableContextStack = Stack<TableInQuery<*>>()

    fun withTable(table: TableInQuery<*>, block: Sql.()->Unit) {
        tableContextStack.push(table)
        try {
            this.block()
        } finally {
            tableContextStack.pop()
        }
    }

    fun currentTable(): TableInQuery<*> {
        return tableContextStack.peek() ?: throw IllegalStateException("No table in context stack")
    }

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
        sql.append(table.tableAlias).append('.').append(column.quotedFieldName)
        return this
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
            return if (ident.contains('`')) {
                ident.replace("`", "``")
            } else {
                ident
            }
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

            val rel = joinedTable.incomingJoin.relToOne as RelToOneImpl<*,*,*,*>
            val sourceAlias = joinedTable.prevTable.tableAlias
            val targetTable = rel.targetTable
            val targetAlias = joinedTable.tableAlias

            if (joinType == JoinType.INNER_JOIN) {
                raw(" INNER JOIN ")
            } else {
                raw(" LEFT JOIN ")
            }

            raw(targetTable.quotedDbName).raw(" AS ").raw(joinedTable.tableAlias).raw(" ON ")

            tuple(rel.info.columnMappings, separator = " AND ") { colMap ->
                raw(sourceAlias).raw(".").raw(colMap.columnFrom.quotedFieldName)
                raw(" = ")
                raw(targetAlias).raw(".").raw(colMap.columnTo.quotedFieldName)
            }
        }
    }

    fun WHERE(filter: ExprBoolean?): Sql {
        if (filter != null) {
            raw(" WHERE ")
            filter.toSql(this, true)
        }
        return this
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
