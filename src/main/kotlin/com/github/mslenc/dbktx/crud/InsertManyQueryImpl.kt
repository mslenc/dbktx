package com.github.mslenc.dbktx.crud

import com.github.mslenc.asyncdb.DbType
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.emitValue
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.ExprNull
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.util.Sql

class InsertManyQueryImpl<E: DbEntity<E, ID>, ID: Any, TABLE: DbTable<E, ID>>(val db: DbConn, val tableDef: TABLE) : InsertManyQuery<E, ID, TABLE>, QueryImpl() {
    private val rows = ArrayList<InsertManyRowImpl<E, ID>>()
    internal val table = BaseTableInUpdateQuery(this, tableDef)

    override val aggregatesAllowed: Boolean
        get() = false

    override fun buildRow(block: TABLE.(InsertManyRow<E, ID>) -> Unit) {
        tableDef.block(addRow())
    }

    override fun addRow(): InsertManyRow<E, ID> {
        return InsertManyRowImpl(this).also { rows += it }
    }

    override suspend fun execute() {
        if (rows.isNotEmpty()) {
            val sql = createInsertManyQuery(tableDef, rows.map { it.values }, db.dbType, false) // TODO: fetch ids?
            db.executeInsertMany(sql, tableDef)
        }
    }
}

private fun <E : DbEntity<E, ID>, ID: Any> createInsertManyQuery(table: DbTable<E, ID>, values: List<Map<Column<E,*>, Expr<*>>>, dbType: DbType, keyAutogenerates: Boolean): Sql {
    val allColumns = LinkedHashSet<Column<E, *>>()
    values.forEach { allColumns.addAll(it.keys) }

    return Sql(dbType).apply {
        +"INSERT INTO "
        raw(table.quotedDbName)
        paren {
            tuple(allColumns) {
                column -> raw(column.quotedFieldName)
            }
        }
        +" VALUES "
        tuple(values) { rowData ->
            paren {
                tuple(allColumns) {
                    emitValue(it, rowData, this)
                }
            }
        }

        if (keyAutogenerates && dbType == DbType.POSTGRES) {
            if (table.primaryKey.isAutoGenerated) {
                +" RETURNING "
                raw(table.primaryKey.getColumn(1).quotedFieldName)
            }
        }
    }
}

class InsertManyRowImpl<E: DbEntity<E, ID>, ID: Any>(private val owner: InsertManyQueryImpl<E, ID, *>) : InsertManyRow<E, ID> {
    internal val values = LinkedHashMap<Column<E, *>, Expr<*>>()

    override val table: TableInQuery<E>
        get() = owner.table

    override fun <T : Any> set(column: Column<E, T>, value: Expr<T>) {
        values[column] = value
    }

    override fun <T : Any> set(column: NonNullColumn<E, T>, value: T) {
        values[column] = column.makeLiteral(value)
    }

    override fun <T : Any> set(column: NullableColumn<E, T>, value: T?) {
        values[column] = when(value) {
            null -> ExprNull(column.sqlType)
            else -> column.makeLiteral(value)
        }
    }

    override fun <TARGET : DbEntity<TARGET, TID>, TID: Any>
    set(relation: RelToOne<E, TARGET>, target: TARGET) {
        @Suppress("UNCHECKED_CAST")
        relation as RelToOneImpl<E, TARGET, TID>

        for (colMap in relation.info.columnMappings) {
            doColMap(colMap, target)
        }
    }

    private fun <TARGET : DbEntity<TARGET, TID>, TID, VALTYPE: Any>
    doColMap(colMap: ColumnMapping<E, TARGET, VALTYPE>, target: TARGET) {
        if (colMap.columnFromKind != ColumnInMappingKind.COLUMN)
            return // TODO: check that constants and parameters match target?

        val colFrom = colMap.rawColumnFrom
        val colTo = colMap.rawColumnTo

        when (colFrom) {
            is NonNullColumn -> set(colFrom, colTo.invoke(target))
            is NullableColumn -> set(colFrom, colTo.invoke(target))
            else -> throw IllegalStateException("Column is neither nullable or nonNull")
        }
    }

    override fun copyUnsetValuesFrom(original: E) {
        val primaryKey = original.metainfo.primaryKey
        val avoidColumn = if (primaryKey.isAutoGenerated) primaryKey.getColumn(1) else null

        for (column in original.metainfo.columns) {
            if (column != avoidColumn && values[column] == null) {
                when (column) {
                    is NonNullColumn -> copyNonNull(column, original)
                    is NullableColumn -> copyNullable(column, original)
                }
            }
        }
    }

    private fun <T : Any> copyNonNull(column: NonNullColumn<E, T>, original: E) {
        set(column, column(original))
    }

    private fun <T : Any> copyNullable(column: NullableColumn<E, T>, original: E) {
        set(column, column(original))
    }
}