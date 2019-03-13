package com.github.mslenc.dbktx.aggr

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.BaseTableInQuery
import com.github.mslenc.dbktx.crud.OrderableFilterableQueryImpl
import com.github.mslenc.dbktx.crud.OrderableQuery
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable

internal abstract class AggrQueryImpl<E : DbEntity<E, *>>(table: DbTable<E, *>, db: DbConn) : OrderableFilterableQueryImpl<E>(table, db), AggrQuery<E> {
    override fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E> {
        return BaseTableInQuery(this, table)
    }


}