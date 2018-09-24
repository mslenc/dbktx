package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.schema.DbEntity

internal class DbInsertImpl<E : DbEntity<E, ID>, ID: Any>(db: DbConn, table: BaseTableInUpdateQuery<E>)
    : DbMutationImpl<E, ID>(db, table), DbInsert<E, ID> {

    override suspend fun execute(): ID {
        return db.executeInsert(table, values)
    }
}