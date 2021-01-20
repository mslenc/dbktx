package com.github.mslenc.dbktx.crud

import com.github.mslenc.asyncdb.DbType
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.ExprNull
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.util.Sql

class BatchedUpdateQueryImpl<E: DbEntity<E, ID>, ID: Any, TABLE: DbTable<E, ID>>(val db: DbConn, val tableDef: TABLE) : BatchedUpdateQuery<E, ID, TABLE>, QueryImpl() {
    private val updates = HashMap<ID, BatchedUpdateRowImpl<E, ID>>()
    internal val table = BaseTableInUpdateQuery(this, tableDef)

    override val aggregatesAllowed: Boolean
        get() = false

    override fun buildUpdate(entity: E, block: TABLE.(BatchedUpdateRow<E, ID>) -> Unit) {
        tableDef.block(addUpdate(entity))
    }

    override fun addUpdate(entity: E): BatchedUpdateRow<E, ID> {
        return updates.getOrPut(entity.id) { BatchedUpdateRowImpl(entity, this) }
    }

    override suspend fun execute() {
        if (updates.isNotEmpty()) {
            val sql = createBatchedUpdateQuery(tableDef, table, updates.values, db.dbType) ?: return
            db.executeInsertMany(sql, tableDef)
        }
    }
}

private fun <E : DbEntity<E, ID>, ID: Any> createBatchedUpdateQuery(table: DbTable<E, ID>, tableInQuery: BaseTableInUpdateQuery<E>, values: Collection<BatchedUpdateRowImpl<E, ID>>, dbType: DbType): Sql? {
    val allColumns = LinkedHashSet<Column<E, *>>()
    val allIds = LinkedHashSet<ID>()
    val allIdsExprs = ArrayList<Expr<*>>()
    values.forEach {
        allColumns.addAll(it.exprs.keys)
    }

    if (allColumns.isEmpty())
        return null

    return Sql(dbType).apply {
        +"UPDATE "
        raw(table.quotedDbName)

        val boundPrimaryKey = table.primaryKey.bindForSelect(tableInQuery)

        var first = true
        for (column in allColumns) {
            val columnValues = HashMap<ID, Expr<*>>()

            for (row in values) {
                row.exprs[column]?.let {
                    columnValues[row.entity.id] = it
                    if (allIds.add(row.entity.id)) {
                        allIdsExprs.add(table.primaryKey.makeLiteral(row.entity.id))
                    }
                }
            }
            if (columnValues.isEmpty())
                continue

            if (first) {
                first = false
                +" SET "
            } else {
                +", "
            }

            columnForUpdate(column)
            +" = CASE "

            boundPrimaryKey.toSql(this, true)

            for ((id, value) in columnValues) {
                +" WHEN "
                table.primaryKey.makeLiteral(id).toSql(this, true)
                + " THEN "
                value.toSql(this, true)
            }

            +" ELSE "
            columnForUpdate(column)
            + " END"
        }

        if (first)
            return null

        +" WHERE "

        inLiteralSetWrapper(negated = false, needleCanBeNull = false) { IN ->
            +boundPrimaryKey
            +IN
            paren { tuple(allIdsExprs) { +it } }
        }
    }
}

private class BatchedUpdateRowImpl<E: DbEntity<E, ID>, ID: Any>(val entity: E, private val owner: BatchedUpdateQueryImpl<E, ID, *>) : BatchedUpdateRow<E, ID>, HasUpdatableColumns<E, ID> {
    val vals: MutableMap<Column<E, *>, Any?> = LinkedHashMap()
    val exprs: MutableMap<Column<E, *>, Expr<*>> = LinkedHashMap()

    override val table: BaseTableInUpdateQuery<E>
        get() = owner.table

    override fun <T : Any> set(column: NonNullColumn<E, T>, value: T) {
        if (column(entity) != value) {
            vals[column] = value
            exprs[column] = column.makeLiteral(value)
        } else {
            vals.remove(column)
            exprs.remove(column)
        }
    }

    override fun <T : Any> set(column: NullableColumn<E, T>, value: T?) {
        if (column(entity) != value) {
            vals[column] = value
            exprs[column] = if (value != null) column.makeLiteral(value) else ExprNull(column.sqlType)
        } else {
            vals.remove(column)
            exprs.remove(column)
        }
    }

    override fun <T : Any> set(column: Column<E, T>, value: Expr<T>) {
        exprs[column] = value
        vals.remove(column)
    }

    override fun <T : Any> get(column: Column<E, T>): DbColumnMutation<E, T> {
        return DbColumnMutationImpl(this, column)
    }

    override fun anyChangesSoFar(): Boolean {
        return exprs.isNotEmpty()
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
}