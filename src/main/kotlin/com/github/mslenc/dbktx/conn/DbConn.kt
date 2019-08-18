package com.github.mslenc.dbktx.conn

import com.github.mslenc.asyncdb.*
import com.github.mslenc.dbktx.crud.*
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.util.BatchingLoader
import com.github.mslenc.dbktx.util.Sql
import kotlinx.coroutines.CoroutineScope

/**
 * A connection to the database, providing methods for querying, updating and
 * transaction management.
 */
interface DbConn {
    val requestTime: RequestTime
    val scope: CoroutineScope

    suspend fun startTransaction()
    suspend fun startTransaction(isolation: DbTxIsolation)
    suspend fun startTransaction(mode: DbTxMode)
    suspend fun startTransaction(isolation: DbTxIsolation, mode: DbTxMode)

    /**
     * Commits any pending changes
     */
    suspend fun commit()

    /**
     * Rolls back any pending changes
     */
    suspend fun rollback()

    /**
     * Commits any pending changes and starts a new transaction.
     */
    suspend fun commitAndChain()

    /**
     * Rolls back any pending changes and starts a new transaction.
     */
    suspend fun rollbackAndChain()

    /**
     * Executes arbitrary SQL (not SELECT)
     *
     * @see query for SELECT statements
     */
    suspend fun execute(sql: String)

    /**
     * Executes arbitrary SELECT SQL
     *
     * @see execute for non-SELECT statements
     */
    suspend fun query(sqlBuilder: Sql): DbResultSet

    /**
     * Executes arbitrary SELECT SQL in streaming mode. The function will return when streaming is finished.
     *
     * @see execute for non-SELECT statements
     * @return the number of rows processed
     */
    suspend fun streamQuery(sqlBuilder: Sql, receiver: (DbRow)->Unit): Long

    /**
     * INTERNAL FUNCTION, use [EntityQuery.run] instead.
     */
    suspend fun <E: DbEntity<E, *>>
    executeSelect(query: EntityQuery<E>, selectForUpdate: Boolean): List<E>

    /**
     * INTERNAL FUNCTION, use [EntityQuery.countAll] instead.
     */
    suspend fun <E: DbEntity<E, *>>
    executeCount(query: EntityQuery<E>): Long

    /**
     * INTERNAL FUNCTION, use [insert] or [newInsertion] instead.
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeInsert(table: TableInQuery<E>, values: EntityValues<E>): ID

    /**
     * INTERNAL FUNCTION, use [update] or [DbTable.update] instead.
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeUpdate(table: TableInQuery<E>, filters: FilterExpr, values: EntityValues<E>): Long

    /**
     * INTERNAL FUNCTION, use [delete] instead.
     */
    suspend fun <E : DbEntity<E, *>>
    executeDelete(deleteQuery: DeleteQuery<E>): Long


    /**
     * Follows a relation-to-one and throws if the target entity was not found.
     */
    suspend fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>
    load(from: FROM, relation: RelToOne<FROM, TO>): TO

    /**
     * Follows a relation-to-one and returns null if the target entity was not found.
     */
    suspend fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>
    find(from: FROM, relation: RelToOne<FROM, TO>): TO?

    /**
     * Follows a relation-to-many and applies additional filter to the result.
     */
    suspend fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>
    load(from: FROM, relation: RelToMany<FROM, TO>, filter: FilterBuilder<TO>.()->FilterExpr): List<TO>

    /**
     * Follows a relation-to-one for multiple source entities.
     */
    suspend fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>
    loadForAll(ref: RelToOne<FROM, TO>, sources: Collection<FROM>): Map<FROM, TO?>

    /**
     * Loads a row from the table by ID and throws if it is not found.
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any>
    loadById(table: DbTable<E, ID>, id: ID): E

    /**
     * Loads a row from the table by ID and returns null if it is not found.
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any>
    findById(table: DbTable<E, ID>, id: ID?): E?

    @Deprecated("Use the other findByKey", replaceWith = ReplaceWith("findByKey(keyDef, key)"))
    suspend fun <E: DbEntity<E, *>, KEY: Any>
    findByKey(table: DbTable<E, *>, keyDef: UniqueKeyDef<E, KEY>, key: KEY): E? {
        return findByKey(keyDef, key)
    }

    suspend fun <E: DbEntity<E, *>, KEY: Any>
    findByKey(keyDef: UniqueKeyDef<E, KEY>, key: KEY): E?

    /**
     * Loads multiple rows from the table by IDs and throws if all IDs are not actually found.
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any>
    loadByIds(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E>

    /**
     * Loads multiple rows from the table by IDs and returns null for all rows not present
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any>
    findByIds(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E?>

    /**
     * Loads a value using the specified batching loader.
     */
    suspend fun <KEY: Any, RESULT>
    load(loader: BatchingLoader<KEY, RESULT>, key: KEY): RESULT

    /**
     * Loads many values using the specified batching loader.
     */
    suspend fun <KEY: Any, RESULT>
    loadForAll(loader: BatchingLoader<KEY, RESULT>, keys: Collection<KEY>): Map<KEY, RESULT>

    /**
     * Shortcut for [loadById]. Use like this:
     * ```
     * val id = 1536
     * val competition = with(db) { COMPETITIONS(id) }
     * ```
     */
    suspend operator fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.invoke(id: ID): E {
        return loadById(this, id)
    }

//    suspend operator get() is not supported (yet).. when it becomes supported, uncomment this..
//
//    /**
//     * Shortcut for [findById]. Use like this:
//     * ```
//     * val id = 1536
//     * val competitionMaybe = with(db) { COMPETITIONS[id] }
//     * ```
//     */
//    suspend operator fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
//    Z.get(id: ID): E? {
//        return findById(this, id)
//    }


    // ease-of-use functions
    /**
     * Queries all the rows in the table.
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any>
    queryAll(table: DbTable<E, ID>) : List<E> {
        return table.newQuery(this).run()
    }

    /**
     * Counts all the rows in the table.
     */
    suspend fun <E : DbEntity<E, ID>, ID : Any>
    countAll(table: DbTable<E, ID>): Long {
        return table.newQuery(this).countAll()
    }

    /**
     * A shortcut for building and executing a count query. Use like this:
     * ```
     * val count = db.count(SOME_TABLE) {
     *     SOME_FIELD eq "foo"
     * }
     * ```
     */
    suspend fun <E : DbEntity<E, *>>
    count(table: DbTable<E, *>, filter: FilterBuilder<E>.() -> FilterExpr): Long {
        val query = table.newQuery(this)
        query.filter(filter)
        return query.countAll()
    }

    /**
     * Deletes the entity from DB.
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any>
    delete(entity: E): Boolean {
        return deleteByIds(entity.metainfo, setOf(entity.id)) > 0
    }

    /**
     * Deletes all rows matching the specified filter. Use like this:
     * ```
     * val aYearAgo = ...;
     * db.delete(COMPETITIONS) {
     *     COMP_DATE lt aYearAgo
     * }
     * ```
     */
    suspend fun <E : DbEntity<E, *>>
    deleteMany(table: DbTable<E, *>, filter: FilterBuilder<E>.() -> FilterExpr): Long {
        val query = table.newDeleteQuery(this)
        query.filter(filter)
        return query.deleteAllMatchingRows()
    }

    /**
     * Deletes multiple entities by their IDs.
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    deleteByIds(table: Z, ids: Set<ID>): Long {
        if (ids.isEmpty())
            return 0L

        val query = table.newDeleteQuery(this)
        query.filter { table.primaryKey oneOf ids }
        return query.deleteAllMatchingRows()
    }

    /**
     * Deletes a single entity by ID.
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.deleteById(id: ID): Boolean {
        return deleteByIds(this, setOf(id)) > 0
    }

    /**
     * Opposite of [DbTable.toJsonObject] - imports the serialized entity back into internal cache.
     */
    fun <E : DbEntity<E, ID>, ID: Any>
    importJson(table: DbTable<E, ID>, json: Map<String, Any?>): E

    /**
     * Shortcut for creating and executing a query. Use like this:
     * ```
     * val rows = with(db) { SOME_TABLE.query { SOME_FIELD eq value } }
     * ```
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.query(filter: FilterBuilder<E>.() -> FilterExpr): List<E> {
        val query = newQuery(this)
        query.filter(filter)
        return query.run()
    }

    /**
     * Shortcut for creating and executing a count query. Use like this:
     * ```
     * val numRows = with(db) { SOME_TABLE.countAll { SOME_FIELD eq value } }
     * ```
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
            Z.countAll(filter: FilterBuilder<E>.() -> FilterExpr): Long {
        val query = newQuery(this)
        query.filter(filter)
        return query.countAll()
    }

    /**
     * Shortcut for defining and executing an insertion. Use like this:
     * ```
     * val newId = with(db) { SOME_TABLE.insert {
     *     SOME_FIELD += "some field value"
     *     OTHER_FIELD += 15
     * } }
     * ```
     */
    suspend fun <E: DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.insert(builder: (DbInsert<E, ID>) -> Unit): ID {
        val insertion = insertion(this@DbConn)
        builder(insertion)
        return insertion.execute()
    }

    /**
     * Similar to [insert], but returns the insertion so that it may be further modified, before [DbInsert.execute] is called.
     */
    fun <E: DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.newInsertion(builder: (DbInsert<E, ID>) -> Unit): DbInsert<E, ID> {
        val insertion = insertion(this@DbConn)
        builder(insertion)
        return insertion
    }

    /**
     * Updates a single entity. Use like this:
     * ```
     * with(db) { SOME_TABLE.update(entity) {
     *     SOME_FIELD += "new field value"
     * } }
     * ```
     */
    suspend fun <E: DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.update(entity: E, builder: (DbUpdate<E>) -> Unit): Boolean {
        val update = update(this@DbConn, entity)
        builder(update)
        return update.execute() > 0
    }

    /**
     * Creates a new query. Use [EntityQuery.filter], [EntityQuery.orderBy], etc. to set it up,
     * then finish with [EntityQuery.run] and/or [EntityQuery.countAll].
     */
    fun <E : DbEntity<E, ID>, ID: Any>
    newQuery(table: DbTable<E, ID>) = table.newQuery(this)

    /**
     * Flushes all internal caches. Note that if there are any outstanding queries, they will all
     * be failed, so it's likely it only makes sense to call this is non-concurrent code.
     */
    fun
    flushAllCaches()

    /**
     * Flushes the cache for a specific table. Use this when you know the table contents changed
     * in some way DbConn doesn't see/recognize (e.g. if you run raw queries that do so, or if
     * the data is changed in some other way external to DbConn). Note that to-many caches
     * involving the table will be flushed as well. Also note that any related outstanding queries
     * will all be failed, so it's likely it only makes sense to call this in non-concurrent code.
     */
    fun <E : DbEntity<E, ID>, ID: Any>
    flushTableCache(table: DbTable<E, ID>)
}
