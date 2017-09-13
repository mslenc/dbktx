package com.xs0.dbktx.conn

import com.xs0.dbktx.crud.DbInsert
import com.xs0.dbktx.crud.DbUpdate
import com.xs0.dbktx.crud.EntityQuery
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable
import com.xs0.dbktx.schema.RelToMany
import com.xs0.dbktx.schema.RelToOne
import com.xs0.dbktx.crud.EntityValues
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

    fun <E: DbEntity<E, ID>, ID: Any>
    queryAsync(query: EntityQuery<E>): Deferred<List<E>> {
        return defer { query(query) }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any>
    query(table: DbTable<E, ID>, filter: ExprBoolean<E>): List<E>

    fun <E : DbEntity<E, ID>, ID: Any>
    queryAsync(table: DbTable<E, ID>, filter: ExprBoolean<E>): Deferred<List<E>> {
        return defer { query(table, filter) }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any>
    queryAll(table: DbTable<E, ID>) : List<E>

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    queryAllAsync(table: DbTable<E, ID>) : Deferred<List<E>> {
        return defer { queryAll(table) }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.count(filter: Z.() -> ExprBoolean<E>): Long {
        return count(this, filter())
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    count(table: DbTable<E, ID>, filter: ExprBoolean<E>?): Long

    fun <E : DbEntity<E, ID>, ID: Any>
    countAsync(table: DbTable<E, ID>, filter: ExprBoolean<E>?): Deferred<Long> {
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
    load(from: FROM, relation: RelToMany<FROM, TO>, filter: ExprBoolean<TO>?): List<TO>

    suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    loadAsync(from: FROM, relation: RelToMany<FROM, TO>, filter: ExprBoolean<TO>): Deferred<List<TO>> {
        return defer { load(from, relation, filter) }
    }

    suspend fun setAutoCommit(autoCommit: Boolean)
    suspend fun setTransactionIsolation(isolation: TransactionIsolation)
    suspend fun commit()
    suspend fun rollback()
    suspend fun execute(sql: String)


    suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeInsert(table: DbTable<E, ID>, values: EntityValues<E>): ID

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeUpdate(table: DbTable<E, ID>, filter: ExprBoolean<E>?, values: EntityValues<E>, specificIds: Set<ID>?): Long

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
    findMany(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E?>

    fun <E : DbEntity<E, ID>, ID: Any>
    findManyAsync(table: DbTable<E, ID>, ids: Iterable<ID>): Deferred<Map<ID, E?>> {
        return defer { findMany(table, ids) }
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any>
    delete(entity: E): Boolean

    fun <E : DbEntity<E, ID>, ID: Any>
    deleteAsync(entity: E): Deferred<Boolean> {
        return defer { delete(entity) }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    delete(table: Z, filter: Z.() -> ExprBoolean<E>): Long

    fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    deleteAsync(table: Z, filter: Z.() -> ExprBoolean<E>): Deferred<Long> {
        return defer { delete(table, filter) }
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    delete(table: Z, ids: Set<ID>): Long

    fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    deleteAsync(table: Z, ids: Set<ID>): Deferred<Long> {
        return defer { delete(table, ids) }
    }


    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.delete(id: ID): Boolean

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
    Z.query(filter: Z.() -> ExprBoolean<E>): List<E> {
        return query(this, filter())
    }

    suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.queryAsync(filter: Z.() -> ExprBoolean<E>): Deferred<List<E>> {
        return queryAsync(this, filter())
    }

    suspend fun <E: DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    insert(table: Z, builder: DbInsert<E, ID>.() -> Unit): ID {
        val insert = table.insertion(this)
        insert.apply(builder)
        return insert.execute()
    }

    suspend fun <E: DbEntity<E, ID>, ID: Any>
    update(entity: E, builder: DbUpdate<E>.() -> Unit): Boolean {
        val update = entity.metainfo.update(this, entity)
        update.apply(builder)
        return update.execute() > 0
    }

    fun <E : DbEntity<E, ID>, ID: Any>
    newQuery(table: DbTable<E, ID>) = table.newQuery(this)
}
