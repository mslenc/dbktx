package com.xs0.dbktx.conn

import com.xs0.dbktx.crud.*
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable
import com.xs0.dbktx.schema.RelToMany
import com.xs0.dbktx.schema.RelToOne
import com.xs0.dbktx.util.Sql
import io.vertx.core.json.JsonObject
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.TransactionIsolation

/**
 * A connection to the database, providing methods for querying, updating and
 * transaction management.
 */
interface DbConn {
    /**
     * Sets the autocommit mode for this connection
     */
    suspend fun setAutoCommit(autoCommit: Boolean)

    /**
     * Sets the transaction isolation mode for this connection
     */
    suspend fun setTransactionIsolation(isolation: TransactionIsolation)

    /**
     * Commits any pending changes
     */
    suspend fun commit()

    /**
     * Rolls back any pending changes
     */
    suspend fun rollback()

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
    suspend fun query(sqlBuilder: Sql): ResultSet

    /**
     * INTERNAL FUNCTION, use [EntityQuery.run] instead.
     */
    suspend fun <E: DbEntity<E, ID>, ID: Any>
    executeSelect(query: EntityQuery<E>): List<E>

    /**
     * INTERNAL FUNCTION, use [EntityQuery.countAll] instead.
     */
    suspend fun <E: DbEntity<E, ID>, ID: Any>
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
    executeUpdate(table: TableInQuery<E>, filters: ExprBoolean?, values: EntityValues<E>, specificIds: Set<ID>?): Long

    /**
     * INTERNAL FUNCTION, use [delete] instead.
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeDelete(deleteQuery: DeleteQuery<E>): Long


    /**
     * Follows a relation-to-one and throws if the target entity was not found.
     */
    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    load(from: FROM, relation: RelToOne<FROM, TO>): TO

    /**
     * Follows a relation-to-one and returns null if the target entity was not found.
     */
    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    find(from: FROM, relation: RelToOne<FROM, TO>): TO?

    /**
     * Follows a relation-to-many
     */
    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID : Any, TO : DbEntity<TO, TOID>, TOID : Any>
    load(from: FROM, relation: RelToMany<FROM, TO>): List<TO>

    /**
     * Follows a relation-to-many and applies additional filter to the result.
     */
    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    load(from: FROM, relation: RelToMany<FROM, TO>, filter: FilterBuilder<TO>.()->ExprBoolean): List<TO>

    /**
     * Follows a relation-to-one for multiple source entities.
     */
    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
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
    suspend fun <E : DbEntity<E, ID>, ID: Any>
    count(table: DbTable<E, ID>, filter: FilterBuilder<E>.() -> ExprBoolean): Long {
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
    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    deleteMany(table: Z, filter: FilterBuilder<E>.() -> ExprBoolean): Long {
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
        query.filter { table.idField oneOf ids }
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
    importJson(table: DbTable<E, ID>, json: JsonObject)

    /**
     * Shortcut for creating and executing a query. Use like this:
     * ```
     * val rows = with(db) { SOME_TABLE.query { SOME_FIELD eq value } }
     * ```
     */
    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.query(filter: FilterBuilder<E>.() -> ExprBoolean): List<E> {
        val query = newQuery(this)
        query.filter(filter)
        return query.run()
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
    Z.insert(builder: DbInsert<E, ID>.() -> Unit): ID {
        val insert = insertion(this@DbConn)
        insert.apply(builder)
        return insert.execute()
    }

    /**
     * Similar to [insert], but returns the insertion so that it may be further modified, before [DbInsert.execute] is called.
     */
    fun <E: DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.newInsertion(builder: DbInsert<E, ID>.() -> Unit): DbInsert<E, ID> {
        val insertion = insertion(this@DbConn)
        insertion.apply(builder)
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
    Z.update(entity: E, builder: DbUpdate<E>.() -> Unit): Boolean {
        val update = update(this@DbConn, entity)
        update.apply(builder)
        return update.execute() > 0
    }

    /**
     * Creates a new query. Use [EntityQuery.filter], [EntityQuery.orderBy], etc. to set it up,
     * then finish with [EntityQuery.run] and/or [EntityQuery.countAll].
     */
    fun <E : DbEntity<E, ID>, ID: Any>
    newQuery(table: DbTable<E, ID>) = table.newQuery(this)
}
