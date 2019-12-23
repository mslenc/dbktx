package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.crud.FilterableQuery
import com.github.mslenc.dbktx.crud.OrderableQuery
import com.github.mslenc.dbktx.schema.DbEntity

interface AggrStreamQuery<E : DbEntity<E, *>> : FilterableQuery<E>, OrderableQuery<E> {
    /** Allows you to build/expand the query via DSL. */
    fun expand(block: AggrStreamTopLevelBuilder<E>.()->Unit)

    /** Registers an additional callback to be called before each row. */
    fun onRowStart(callback: (DbRow)->Unit)

    /** Registers an additional callback to be called after each row. */
    fun onRowEnd(callback: (DbRow)->Unit)

    /** Runs the query, calling the callbacks as specified via builders. Returns the number of rows processed. */
    suspend fun execute(): Long
}

interface AggrInsertSelectQuery<OUT: DbEntity<OUT, *>, ROOT: DbEntity<ROOT, *>> : FilterableQuery<ROOT>, OrderableQuery<ROOT> {
    /** Allows you to build/expand the query via DSL. */
    fun expand(block: AggrInsertSelectTopLevelBuilder<OUT, ROOT>.()->Unit)

    /** Runs the query, calling the callbacks as specified via builders. Returns the number of rows created. */
    suspend fun execute(): Long
}