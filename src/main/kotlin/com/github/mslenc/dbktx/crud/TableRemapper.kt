package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.schema.DbEntity
import java.util.*

class TableRemapper(val newQuery: QueryImpl) {
    private val mappings = IdentityHashMap<TableInQuery<*>, TableInQuery<*>>()

    fun <T : DbEntity<T, *>> addExplicitMapping(original: TableInQuery<T>, mapped: TableInQuery<T>) {
        val previous = mappings.put(original, mapped)
        if (previous != null)
            throw IllegalStateException("A mapping already existed")
    }

    fun <T: DbEntity<T, *>> remap(original: TableInQuery<T>): TableInQuery<T> {
        val existing = mappings[original]
        if (existing != null) {
            @Suppress("UNCHECKED_CAST")
            return existing as TableInQuery<T>
        }

        val remapped: TableInQuery<T> = when (original) {
            is BaseTableInQuery,
            is BaseTableInUpdateQuery -> {
                throw IllegalStateException("Base table should have a pre-existing explicit mapping")
            }

            is JoinedTableInQuery -> {
                val newPrevTable = original.prevTable.remap(this)
                val newJoin = Join(original.incomingJoin.joinType, original.incomingJoin.relToOne)
                val newAlias = generateAliasTo(newQuery, original.table)
                val newJoinedTable = JoinedTableInQuery(newQuery, newAlias, original.table, newPrevTable, newJoin)
                newPrevTable.addRemappedJoin(newJoinedTable)
                newJoinedTable
            }

            is SubTableInQuery -> {
                val newAlias = generateAliasTo(newQuery, original.table)
                SubTableInQuery(newQuery, newAlias, original.table, original.prevTable.remap(this))
            }
        }

        newQuery.registerTableInQuery(remapped)
        mappings[original] = remapped
        return remapped
    }
}