package com.xs0.dbktx

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler

import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.TransactionIsolation
import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.async

/**
 * A connection to the database, providing methods for querying, updating and
 * transaction management. A connection has a Handler&lt;AsyncResult&lt;T>> attached,
 * which is mainly there to be notified of any "out of band" errors that occur, but
 * can also receive the end result via [.complete].
 *
 * @param <CTX>
 */
interface DbConn<CTX> {
    val context: CTX

    suspend fun query(sqlBuilder: SqlBuilder): ResultSet

    fun queryAsync(sqlBuilder: SqlBuilder): Deferred<ResultSet> {
        return defer { query(sqlBuilder) }
    }


    suspend fun <E : DbEntity<E, ID>, ID>
            query(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): List<E>

    fun <E : DbEntity<E, ID>, ID>
            queryAsync(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): Deferred<List<E>> {
        return defer { query(table, filter) }
    }


    suspend fun <E : DbEntity<E, ID>, ID>
            queryAll(table: DbTable<E, ID>) : List<E>

    suspend fun <E : DbEntity<E, ID>, ID>
            queryAllAsync(table: DbTable<E, ID>) : Deferred<List<E>> {
        return defer { queryAll(table) }
    }


    suspend fun <E : DbEntity<E, ID>, ID>
            count(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): Long

    fun <E : DbEntity<E, ID>, ID>
            countAsync(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): Deferred<Long>


    suspend fun <E : DbEntity<E, ID>, ID>
            countAll(table: DbTable<E, ID>): Long

    fun <E : DbEntity<E, ID>, ID>
            countAllAsync(table: DbTable<E, ID>): Deferred<Long> {
        return defer { countAll(table) }
    }


    suspend fun <E : DbEntity<E, ID>, ID>
            load(table: DbTable<E, ID>, id: ID): E

    fun <E : DbEntity<E, ID>, ID>
            loadAsync(table: DbTable<E, ID>, id: ID): Deferred<E> {
        return defer { load(table, id) }
    }


    suspend fun <E : DbEntity<E, ID>, ID>
            find(table: DbTable<E, ID>, id: ID): E?

    fun <E : DbEntity<E, ID>, ID>
            findAsync(table: DbTable<E, ID>, id: ID): Deferred<E?> {
        return defer { load(table, id) }
    }


    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID>
            load(from: FROM, relation: RelToOne<FROM, TO>): TO

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID>
            loadAsync(from: FROM, relation: RelToOne<FROM, TO>): Deferred<TO>



    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID>
            load(from: FROM, relation: RelToMany<FROM, TO>): List<TO>

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID>
            loadAsync(from: FROM, relation: RelToMany<FROM, TO>): Deferred<List<TO>>


    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID>
            load(from: FROM, relation: RelToMany<FROM, TO>, filter: Expr<in TO, Boolean>): List<TO>

    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID>
            loadAsync(from: FROM, relation: RelToMany<FROM, TO>, filter: Expr<in TO, Boolean>): Deferred<List<TO>>


    suspend fun commit()

    fun commitAsync(): Deferred<Unit> {
        return defer { commit() }
    }


    suspend fun rollback()

    fun rollbackAsync(): Deferred<Unit> {
        return defer { rollback() }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeInsert(table: DbTable<E, ID>, values: EntityValues<E>): ID

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeUpdate(table: DbTable<E, ID>, filter: Expr<in E, Boolean>, values: EntityValues<E>, specificIds: Set<ID>): Long

    suspend fun setAutoCommit(autoCommit: Boolean)

    suspend fun setTransactionIsolation(isolation: TransactionIsolation)

    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    loadForAll(ref: RelToOne<FROM, TO>, sources: Collection<FROM>): Map<FROM, TO>

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    loadMany(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E>

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    delete(entity: E): Long

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    delete(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): Long

    fun <E : DbEntity<E, ID>, ID: Any>
    delete(table: DbTable<E, ID>, ids: Set<ID>): Long

    fun <E : DbEntity<E, ID>, ID: Any>
    delete(table: DbTable<E, ID>, id: ID): Long

    suspend fun execute(sql: String)
}
