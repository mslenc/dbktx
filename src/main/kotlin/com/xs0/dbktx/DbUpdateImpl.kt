package com.xs0.dbktx

class DbUpdateImpl<E : DbEntity<E, ID>, ID: Any>(
        db: DbConn,
        table: DbTable<E, ID>,
        private val filter: ExprBoolean<E>?,
        private val specificIds: Set<ID>?)
    : DbMutationImpl<E, ID>(db, table), DbUpdate<E> {

    override suspend fun execute(): Long {
        return db.executeUpdate(table, filter, values, specificIds)
    }
}
