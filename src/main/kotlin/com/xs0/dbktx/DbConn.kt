package com.xs0.dbktx

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler

import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.TransactionIsolation

/**
 * A connection to the database, providing methods for querying, updating and
 * transaction management. A connection has a Handler&lt;AsyncResult&lt;T>> attached,
 * which is mainly there to be notified of any "out of band" errors that occur, but
 * can also receive the end result via [.complete].
 *
 * @param <CTX>
 * @param <T>
</T></CTX> */
interface DbConn<CTX, T> {
    /**
     * Closes the connection, cancels any outstanding queries, and provides the result to
     * the attached result handler.
     *
     * @param result result
     */
    fun complete(result: T)

    /**
     * Closes the connection, cancels any outstanding queries, and provides the failure cause
     * to the result handler.
     *
     * @param cause failure cause
     */
    fun fail(cause: Throwable)

    var context: CTX


    fun query(sqlBuilder: SqlBuilder, handler: Handler<AsyncResult<ResultSet>>)

    fun <E : DbEntity<E, ID>, ID> query(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): Future<List<E>> {
        val future = Future.future<List<E>>()
        query(table, filter, future.completer())
        return future
    }


    fun <E : DbEntity<E, ID>, ID> query(table: DbTable<E, ID>, filter: Expr<in E, Boolean>, handler: Handler<AsyncResult<List<E>>>)

    fun <E : DbEntity<E, ID>, ID> queryCount(table: DbTable<E, ID>, filter: Expr<in E, Boolean>, handler: Handler<AsyncResult<Long>>)

    fun <E : DbEntity<E, ID>, ID> load(table: DbTable<E, ID>, id: ID, handler: Handler<AsyncResult<E>>)

    fun <E : DbEntity<E, ID>, ID> load(table: DbTable<E, ID>, id: ID): Future<E> {
        val future = Future.future<E>()
        load(table, id, future.completer())
        return future
    }

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID>
            load(from: FROM, relation: RelToOne<FROM, TO>, handler: Handler<AsyncResult<TO>>)

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> load(from: FROM, relation: RelToOne<FROM, TO>): Future<TO> {
        val future = Future.future<TO>()
        load(from, relation, future.completer())
        return future
    }

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> load(from: FROM, relation: RelToMany<FROM, TO>, handler: Handler<AsyncResult<List<TO>>>)

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> load(from: FROM, relation: RelToMany<FROM, TO>): Future<List<TO>> {
        val future = Future.future<List<TO>>()
        load(from, relation, future.completer())
        return future
    }

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> load(from: FROM, relation: RelToMany<FROM, TO>, filter: Expr<in TO, Boolean>, handler: Handler<AsyncResult<List<TO>>>)

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> load(from: FROM, relation: RelToMany<FROM, TO>, filter: Expr<in TO, Boolean>): Future<List<TO>> {
        val future = Future.future<List<TO>>()
        load(from, relation, filter, future.completer())
        return future
    }

    fun commit(handler: Handler<AsyncResult<Void>>)

    fun commitE(onSuccess: Handler<Void>) {
        commit(makeHandler(onSuccess, ???({ this.fail(it) })))
    }

    /**
     * Executes a roll back on the db connection.
     *
     * @param callback
     */
    fun rollback(callback: Handler<AsyncResult<Void>>)

    fun rollbackE(callback: Handler<Void>) {
        rollback(makeHandler(callback, ???({ this.fail(it) })))
    }

    fun <E : DbEntity<E, ID>, ID> executeInsert(table: DbTable<E, ID>, values: Map<Column<E, *>, Expr<in E, *>>, handler: Handler<AsyncResult<ID>>)

    fun <E : DbEntity<E, ID>, ID> executeInsertE(table: DbTable<E, ID>, values: Map<Column<E, *>, Expr<in E, *>>, onSuccess: Handler<ID>) {
        executeInsert(table, values, makeHandler(onSuccess, ???({ this.fail(it) })))
    }


    fun <E : DbEntity<E, ID>, ID> executeUpdate(table: DbTable<E, ID>, filter: Expr<in E, Boolean>, values: Map<Column<E, *>, Expr<in E, *>>, specificIds: Set<ID>, handler: Handler<AsyncResult<Long>>)

    fun <E : DbEntity<E, ID>, ID> executeUpdateE(table: DbTable<E, ID>, filter: Expr<in E, Boolean>, values: Map<Column<E, *>, Expr<in E, *>>, specificIds: Set<ID>, onSuccess: Handler<Long>) {
        executeUpdate(table, filter, values, specificIds, makeHandler(onSuccess, ???({ this.fail(it) })))
    }


    /**
     * Same as [.query], except errors cause fail to be called
     * automatically.
     *
     * @param sqlBuilder
     * @param handler
     */
    fun queryE(sqlBuilder: SqlBuilder, handler: Handler<ResultSet>) {
        query(sqlBuilder, makeHandler(handler, ???({ this.fail(it) })))
    }

    fun <E : DbEntity<E, ID>, ID> queryE(table: DbTable<E, ID>, filter: Expr<in E, Boolean>, handler: Handler<List<E>>) {
        query(table, filter, makeHandler(handler, ???({ this.fail(it) })))
    }

    fun <E : DbEntity<E, ID>, ID> queryCountE(table: DbTable<E, ID>, filter: Expr<in E, Boolean>, handler: Handler<Long>) {
        queryCount(table, filter, makeHandler(handler, ???({ this.fail(it) })))
    }

    fun <E : DbEntity<E, ID>, ID> loadE(table: DbTable<E, ID>, id: ID, handler: Handler<E>) {
        load(table, id, makeHandler(handler, ???({ this.fail(it) })))
    }

    fun <E : DbEntity<E, ID>, ID> loadE(table: DbTable<E, ID>, id: ID): Future<E> {
        val future = Future.future<E>()
        load(table, id, makeHandler(???({ future.complete() }), ???({ this.fail(it) })))
        return future
    }

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> loadE(from: FROM, relation: RelToOne<FROM, TO>, handler: Handler<TO>) {
        load(from, relation, makeHandler(handler, ???({ this.fail(it) })))
    }

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> loadE(from: FROM, relation: RelToMany<FROM, TO>, handler: Handler<List<TO>>) {
        load(from, relation, makeHandler(handler, ???({ this.fail(it) })))
    }

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> loadE(from: FROM, relation: RelToMany<FROM, TO>, filter: Expr<in TO, Boolean>, handler: Handler<List<TO>>) {
        load(from, relation, filter, makeHandler(handler, ???({ this.fail(it) })))
    }

    fun setAutoCommit(autoCommit: Boolean, handler: Handler<AsyncResult<Void>>)

    fun setAutoCommitE(autoCommit: Boolean, onSuccess: Handler<Void>) {
        setAutoCommit(autoCommit, makeHandler(onSuccess, ???({ this.fail(it) })))
    }

    fun setTransactionIsolation(isolation: TransactionIsolation, handler: Handler<AsyncResult<Void>>)

    fun setTransactionIsolationE(isolation: TransactionIsolation, onSuccess: Handler<Void>) {
        setTransactionIsolation(isolation, makeHandler(onSuccess, ???({ this.fail(it) })))
    }

    fun completer(): Handler<AsyncResult<T>>

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> loadForAll(ref: RelToOne<FROM, TO>, sources: Collection<FROM>, handler: Handler<AsyncResult<Map<FROM, TO>>>)

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> loadForAllE(ref: RelToOne<FROM, TO>, sources: Collection<FROM>, onSuccess: Handler<Map<FROM, TO>>) {
        loadForAll(ref, sources, makeHandler(onSuccess, ???({ this.fail(it) })))
    }

    fun <E : DbEntity<E, ID>, ID> loadMany(table: DbTable<E, ID>, ids: Iterable<ID>, handler: Handler<AsyncResult<Map<ID, E>>>)

    fun <E : DbEntity<E, ID>, ID> loadManyE(table: DbTable<E, ID>, ids: Iterable<ID>, onSuccess: Handler<Map<ID, E>>) {
        loadMany(table, ids, makeHandler(onSuccess, ???({ this.fail(it) })))
    }

    fun <E : DbEntity<E, ID>, ID> loadManyE(table: DbTable<E, ID>, ids: Iterable<ID>): Future<Map<ID, E>> {
        val future = Future.future<Map<ID, E>>()
        loadMany(table, ids, makeHandler(???({ future.complete() }), ???({ this.fail(it) })))
        return future
    }

    fun <E : DbEntity<E, ID>, ID> delete(entity: E, handler: Handler<AsyncResult<Long>>)

    fun <E : DbEntity<E, ID>, ID> deleteE(entity: E, onSuccess: Handler<Long>) {
        delete(entity, makeHandler(onSuccess, ???({ this.fail(it) })))
    }

    fun <E : DbEntity<E, ID>, ID> delete(table: DbTable<E, ID>, filter: Expr<in E, Boolean>, handler: Handler<AsyncResult<Long>>)

    fun <E : DbEntity<E, ID>, ID> deleteE(table: DbTable<E, ID>, filter: Expr<in E, Boolean>, onSuccess: Handler<Long>) {
        delete(table, filter, makeHandler(onSuccess, ???({ this.fail(it) })))
    }

    fun <E : DbEntity<E, ID>, ID> delete(table: DbTable<E, ID>, ids: Set<ID>, handler: Handler<AsyncResult<Long>>)

    fun <E : DbEntity<E, ID>, ID> deleteE(table: DbTable<E, ID>, ids: Set<ID>, onSuccess: Handler<Long>) {
        delete(table, ids, makeHandler(onSuccess, ???({ this.fail(it) })))
    }

    fun <E : DbEntity<E, ID>, ID> delete(table: DbTable<E, ID>, id: ID, handler: Handler<AsyncResult<Long>>)

    fun <E : DbEntity<E, ID>, ID> deleteE(table: DbTable<E, ID>, id: ID, onSuccess: (Long)->Unit) {
        delete(table, id, makeHandler(this::fail, onSuccess))
    }

    fun execute(sql: String, handler: Handler<AsyncResult<Void>>)

    fun executeE(sql: String, onSuccess: Handler<Void>) {
        execute(sql, makeHandler(onSuccess, ???({ this.fail(it) })))
    }
}
