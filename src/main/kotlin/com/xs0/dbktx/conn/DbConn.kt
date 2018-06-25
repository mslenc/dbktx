package com.xs0.dbktx.conn

import com.xs0.dbktx.crud.*
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable
import com.xs0.dbktx.schema.RelToMany
import com.xs0.dbktx.schema.RelToOne
import com.xs0.dbktx.util.Sql
import com.xs0.dbktx.util.defer
import io.vertx.core.json.JsonObject
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.TransactionIsolation
import kotlinx.coroutines.experimental.Deferred

/**
 * A connection to the database, providing methods for querying, updating and
 * transaction management.
 */
interface DbConn {
    suspend fun query(sqlBuilder: Sql): ResultSet

    fun queryAsync(sqlBuilder: Sql): Deferred<ResultSet> {
        return defer { query(sqlBuilder) }
    }


    suspend fun <E: DbEntity<E, ID>, ID: Any>
    query(query: EntityQuery<E>): List<E>

    suspend fun <E: DbEntity<E, ID>, ID: Any>
    count(query: EntityQuery<E>): Long



    suspend fun <E : DbEntity<E, ID>, ID: Any>
    queryAll(table: DbTable<E, ID>) : List<E>

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    queryAllAsync(table: DbTable<E, ID>) : Deferred<List<E>> {
        return defer { queryAll(table) }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any>
    count(table: DbTable<E, ID>, filter: FilterBuilder<E>.() -> ExprBoolean): Long {
        val query = table.newQuery(this)
        query.filter(filter)
        return query.countAll()
    }

    fun <E : DbEntity<E, ID>, ID: Any>
    countAsync(table: DbTable<E, ID>, filter: FilterBuilder<E>.() -> ExprBoolean): Deferred<Long> {
        return defer { count(table, filter) }
    }


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

    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    find(from: FROM, relation: RelToOne<FROM, TO>): TO?

    fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    findAsync(from: FROM, relation: RelToOne<FROM, TO>): Deferred<TO?> {
        return defer { find(from, relation) }
    }



    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID : Any, TO : DbEntity<TO, TOID>, TOID : Any>
    load(from: FROM, relation: RelToMany<FROM, TO>): List<TO>

    fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    loadAsync(from: FROM, relation: RelToMany<FROM, TO>): Deferred<List<TO>> {
        return defer { load(from, relation) }
    }


    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    load(from: FROM, relation: RelToMany<FROM, TO>, filter: ExprBoolean): List<TO>

    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    loadAsync(from: FROM, relation: RelToMany<FROM, TO>, filter: ExprBoolean): Deferred<List<TO>> {
        return defer { load(from, relation, filter) }
    }

    suspend fun setAutoCommit(autoCommit: Boolean)
    suspend fun setTransactionIsolation(isolation: TransactionIsolation)
    suspend fun commit()
    suspend fun rollback()
    suspend fun execute(sql: String)


    suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeInsert(table: TableInQuery<E>, values: EntityValues<E>): ID

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeUpdate(table: TableInQuery<E>, filters: ExprBoolean?, values: EntityValues<E>, specificIds: Set<ID>?): Long

    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    loadForAll(ref: RelToOne<FROM, TO>, sources: Collection<FROM>?): Map<FROM, TO?>

    fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    loadForAllAsync(ref: RelToOne<FROM, TO>, sources: Collection<FROM>): Deferred<Map<FROM, TO?>> {
        return defer { loadForAll(ref, sources) }
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    loadMany(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E>

    fun <E : DbEntity<E, ID>, ID: Any>
    loadManyAsync(table: DbTable<E, ID>, ids: Iterable<ID>): Deferred<Map<ID, E>> {
        return defer { loadMany(table, ids) }
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    findMany(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E?>

    fun <E : DbEntity<E, ID>, ID: Any>
    findManyAsync(table: DbTable<E, ID>, ids: Iterable<ID>): Deferred<Map<ID, E?>> {
        return defer { findMany(table, ids) }
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    delete(entity: E): Boolean {
        return delete(entity.metainfo, setOf(entity.id)) > 0
    }

    fun <E : DbEntity<E, ID>, ID: Any>
    deleteAsync(entity: E): Deferred<Boolean> {
        return defer { delete(entity) }
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    delete(table: Z, filter: FilterBuilder<E>.() -> ExprBoolean): Long {
        val query = table.newDeleteQuery(this)
        query.filter(filter)
        return query.deleteAllMatchingRows()
    }

    fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    deleteAsync(table: Z, filter: FilterBuilder<E>.() -> ExprBoolean): Deferred<Long> {
        return defer { delete(table, filter) }
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    delete(deleteQuery: DeleteQuery<E>): Long

    fun <E : DbEntity<E, ID>, ID: Any>
    deleteAsync(deleteQuery: DeleteQuery<E>): Deferred<Long> {
        return defer { delete(deleteQuery) }
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    delete(table: Z, ids: Set<ID>): Long {
        if (ids.isEmpty())
            return 0L

        val query = table.newDeleteQuery(this)
        query.filter { table.idField oneOf ids }
        return query.deleteAllMatchingRows()
    }

    fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    deleteAsync(table: Z, ids: Set<ID>): Deferred<Long> {
        return defer { delete(table, ids) }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.delete(id: ID): Boolean {
        return delete(this, setOf(id)) > 0
    }

    fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.deleteAsync(id: ID): Deferred<Boolean> {
        return defer { delete(id) }
    }

    fun <E : DbEntity<E, ID>, ID: Any>
    importJson(table: DbTable<E, ID>, json: JsonObject)


    suspend operator fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.invoke(id: ID): E {
        return load(this, id)
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.get(id: ID): E? {
        return find(this, id)
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.query(filter: FilterBuilder<E>.() -> ExprBoolean): List<E> {
        val query = newQuery(this)
        query.filter(filter)
        return query.run()
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.queryAsync(filter: FilterBuilder<E>.() -> ExprBoolean): Deferred<List<E>> {
        return defer { query(filter) }
    }

    suspend fun <E: DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.insert(builder: DbInsert<E, ID>.() -> Unit): ID {
        val insert = insertion(this@DbConn)
        insert.apply(builder)
        return insert.execute()
    }

    fun <E: DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.insertion(builder: DbInsert<E, ID>.() -> Unit): DbInsert<E, ID> {
        val insertion = insertion(this@DbConn)
        insertion.apply(builder)
        return insertion
    }

    suspend fun <E: DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.update(entity: E, builder: DbUpdate<E>.() -> Unit): Boolean {
        val update = update(this@DbConn, entity)
        update.apply(builder)
        return update.execute() > 0
    }

    fun <E : DbEntity<E, ID>, ID: Any>
    newQuery(table: DbTable<E, ID>) = table.newQuery(this)
}
