package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.crud.FilterableQuery
import com.github.mslenc.dbktx.crud.OrderableQuery
import com.github.mslenc.dbktx.schema.DbEntity

interface AggrQuery<E: DbEntity<E, *>> : FilterableQuery<E>, OrderableQuery<E>

interface AggrListQuery<OUT: Any, E : DbEntity<E, *>> : AggrQuery<E> {
    /** Allows you to build/expand the query via DSL. Returns itself for chaining. */
    fun expand(block: AggrListBuilder<OUT, E>.()->Unit): AggrListQuery<OUT, E>

    /** Runs the query and returns the output objects, as specified via builders. */
    suspend fun run(): List<OUT>
}

interface AggrStreamQuery<E : DbEntity<E, *>> : AggrQuery<E> {
    /** Allows you to build/expand the query via DSL. Returns itself for chaining. */
    fun expand(block: AggrStreamTopLevelBuilder<E>.()->Unit): AggrStreamQuery<E>

    /** Registers an additional callback to be called before each row. */
    fun onRowStart(callback: (DbRow)->Unit)

    /** Registers an additional callback to be called after each row. */
    fun onRowEnd(callback: (DbRow)->Unit)

    /** Runs the query, calling the callbacks as specified via builders. Returns the number of rows processed. */
    suspend fun run(): Long
}
