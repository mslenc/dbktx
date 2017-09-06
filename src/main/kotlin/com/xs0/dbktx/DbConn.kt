package com.xs0.dbktx

import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.TransactionIsolation
import kotlinx.coroutines.experimental.Deferred

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


    suspend fun <E : DbEntity<E, ID>, ID: Any>
            query(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): List<E>

    fun <E : DbEntity<E, ID>, ID: Any>
            queryAsync(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): Deferred<List<E>> {
        return defer { query(table, filter) }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any>
            queryAll(table: DbTable<E, ID>) : List<E>

    suspend fun <E : DbEntity<E, ID>, ID: Any>
            queryAllAsync(table: DbTable<E, ID>) : Deferred<List<E>> {
        return defer { queryAll(table) }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any>
            count(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): Long

    fun <E : DbEntity<E, ID>, ID: Any>
            countAsync(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): Deferred<Long>


    suspend fun <E : DbEntity<E, ID>, ID: Any>
            countAll(table: DbTable<E, ID>): Long

    fun <E : DbEntity<E, ID>, ID: Any>
            countAllAsync(table: DbTable<E, ID>): Deferred<Long> {
        return defer { countAll(table) }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any>
            load(table: DbTable<E, ID>, id: ID): E

    fun <E : DbEntity<E, ID>, ID: Any>
            loadAsync(table: DbTable<E, ID>, id: ID): Deferred<E> {
        return defer { load(table, id) }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any>
            find(table: DbTable<E, ID>, id: ID?): E?

    fun <E : DbEntity<E, ID>, ID: Any>
            findAsync(table: DbTable<E, ID>, id: ID?): Deferred<E?> {
        return defer { find(table, id) }
    }


    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
            load(from: FROM, relation: RelToOne<FROM, TO>): TO

    fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
            loadAsync(from: FROM, relation: RelToOne<FROM, TO>): Deferred<TO> {
        return defer { load(from, relation) }
    }



    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID : Any, TO : DbEntity<TO, TOID>, TOID : Any>
            load(from: FROM, relation: RelToMany<FROM, TO>): List<TO>

    fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
            loadAsync(from: FROM, relation: RelToMany<FROM, TO>): Deferred<List<TO>> {
        return defer { load(from, relation) }
    }


    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
            load(from: FROM, relation: RelToMany<FROM, TO>, filter: Expr<in TO, Boolean>?): List<TO>

    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
            loadAsync(from: FROM, relation: RelToMany<FROM, TO>, filter: Expr<in TO, Boolean>): Deferred<List<TO>> {
        return defer { load(from, relation, filter) }
    }

    suspend fun setAutoCommit(autoCommit: Boolean)
    suspend fun setTransactionIsolation(isolation: TransactionIsolation)
    suspend fun commit()
    suspend fun rollback()


    suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeInsert(table: DbTable<E, ID>, values: EntityValues<E>): ID?

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeUpdate(table: DbTable<E, ID>, filter: Expr<in E, Boolean>, values: EntityValues<E>, specificIds: Set<ID>?): Long

    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    loadForAll(ref: RelToOne<FROM, TO>, sources: Collection<FROM>?): Map<FROM, TO>

    fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    loadForAllAsync(ref: RelToOne<FROM, TO>, sources: Collection<FROM>): Deferred<Map<FROM, TO>> {
        return defer { loadForAll(ref, sources) }
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    loadMany(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E>

    fun <E : DbEntity<E, ID>, ID: Any>
    loadManyAsync(table: DbTable<E, ID>, ids: Iterable<ID>): Deferred<Map<ID, E>> {
        return defer { loadMany(table, ids) }
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    delete(entity: E): Long

    fun <E : DbEntity<E, ID>, ID: Any>
    deleteAsync(entity: E): Deferred<Long> {
        return defer { delete(entity) }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any>
    delete(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): Long

    fun <E : DbEntity<E, ID>, ID: Any>
    deleteAsync(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): Deferred<Long> {
        return defer { delete(table, filter) }
    }


    fun <E : DbEntity<E, ID>, ID: Any>
    delete(table: DbTable<E, ID>, ids: Set<ID>): Long

    fun <E : DbEntity<E, ID>, ID: Any>
    deleteAsync(table: DbTable<E, ID>, ids: Set<ID>): Deferred<Long> {
        return defer { delete(table, ids) }
    }


    fun <E : DbEntity<E, ID>, ID: Any>
    delete(table: DbTable<E, ID>, id: ID): Long

    fun <E : DbEntity<E, ID>, ID: Any>
    deleteAsync(table: DbTable<E, ID>, id: ID): Deferred<Long> {
        return defer { delete(table, id) }
    }

    suspend fun execute(sql: String)
}
