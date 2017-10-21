package com.xs0.dbktx.conn

import com.xs0.dbktx.crud.EntityQuery
import com.xs0.dbktx.crud.EntityQueryImpl
import com.xs0.dbktx.schema.Column
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.schema.*
import com.xs0.dbktx.crud.EntityValues
import com.xs0.dbktx.util.*
import io.vertx.core.json.JsonObject
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLConnection
import io.vertx.ext.sql.TransactionIsolation
import io.vertx.ext.sql.UpdateResult
import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.sync.Mutex
import mu.KLogging

import java.util.*

import kotlin.collections.LinkedHashMap
import kotlin.coroutines.experimental.suspendCoroutine

class DbLoaderImpl(conn: SQLConnection, private val delayedExecScheduler: DelayedExecScheduler) : DbConn, AutoCloseable {

    private val _dbConn: SQLConnection // use via dbOp
    private val mutex = Mutex()
    private val masterIndex = MasterIndex()
    private var scheduled: Boolean = false // used when batching loads, true means that we're already scheduled (see scheduleDelayedExec())

    init {
        this._dbConn = MultiQueryConn(conn)
    }

    private suspend inline fun <T> dbOp(block: (SQLConnection) -> T): T {
        mutex.lock()
        try {
            return block(_dbConn)
        } finally {
            mutex.unlock()
        }
    }

    override fun close() {
        _dbConn.close()
    }

    private fun scheduleDelayedExec() {
        if (scheduled)
            return

        scheduled = true

        delayedExecScheduler.schedule({
            scheduled = false
            goLoadDelayed()
        })

    }

    override suspend fun
    query(sqlBuilder: Sql): ResultSet {
        val sql = sqlBuilder.getSql()
        logger.trace { "Starting query $sql" }
        val result: ResultSet = dbOp { conn -> vx { handler ->
            conn.queryWithParams(sqlBuilder.getSql(), sqlBuilder.params, handler)
        } }
        logger.trace { "Ended query $sql" }
        return result
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeInsert(table: DbTable<E, ID>, values: EntityValues<E>): ID {
        val explicitId: ID? = table.idField.extract(values)
        if (explicitId == null && !table.idField.isAutoGenerated) {
            throw IllegalArgumentException("Missing the ID")
        }

        val sqlBuilder = createInsertQuery(table, values)

        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        val updateResult = dbOp { conn -> vx<UpdateResult> { handler -> conn.updateWithParams(sql, params, handler) } }

        masterIndex.flushRelated(table)

        val keys = updateResult.keys
        if (keys != null && !keys.isEmpty && table.keyIsAutogenerated) {
            @Suppress("UNCHECKED_CAST")
            val idColumn = table.idField as Column<E, ID>
            return idColumn.sqlType.fromJson(keys.getValue(0))
        } else {
            return explicitId ?: throw IllegalStateException("ID missing")
        }
    }

    private fun <E : DbEntity<E, ID>, ID: Any> createInsertQuery(table: DbTable<E, ID>, values: EntityValues<E>): Sql {
        return Sql().apply {
            +"INSERT INTO "
            +table
            paren {
                tuple(values) {
                    column -> +column
                }
            }
            +" VALUES "
            paren {
                tuple(values) {
                    emitValue(it, values, this)
                }
            }
        }
    }

    private fun <E : DbEntity<E, *>, T: Any> emitValue(column: Column<E, T>, values: EntityValues<E>, sb: Sql) {
        val value = values.getExpr(column)
        if (value == null) {
            sb.raw("NULL")
        } else {
            value.toSql(sb, true)
        }
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeUpdate(table: DbTable<E, ID>, filter: ExprBoolean<E>?, values: EntityValues<E>, specificIds: Set<ID>?): Long {
        val sqlBuilder = createUpdateQuery(table, values, filter) ?: return 0
        return executeUpdateLikeQuery(sqlBuilder, table)
    }

    private fun <E : DbEntity<E, ID>, ID: Any>
    createUpdateQuery(table: DbTable<E, ID>, values: EntityValues<E>, filter: ExprBoolean<E>?): Sql? {
        if (values.isEmpty())
            return null

        return Sql().apply {
            +"UPDATE "
            +table
            +" SET "
            tuple(values) { column ->
                +column
                +"="
                emitValue(column, values, this)
            }
            WHERE(filter)
        }
    }

    override suspend fun <E : DbEntity<E, ID>, ID : Any>
    find(table: DbTable<E, ID>, id: ID?): E? {
        if (id == null)
            return null

        val entities = masterIndex[table]
        val entityInfo = entities[id]

        when (entityInfo.state) {
            EntityState.LOADED -> {
                logger.trace("Id {} of table {} was already loaded", id, entities.metainfo.dbName)
                return entityInfo.value
            }

            EntityState.LOADING -> {
                logger.trace("Adding id {} of table {} to waiting list")
                return suspendCoroutine(entityInfo::addReceiver)
            }

            EntityState.INITIAL -> {
                logger.trace("Adding id {} of table {} to list for loading and initiating load", id, entities.metainfo.dbName)
                scheduleDelayedExec()
                entities.addIdToLoad(id)
                return suspendCoroutine(entityInfo::startedLoading)
            }
        }
    }

    override suspend fun <E : DbEntity<E, ID>, ID : Any>
    load(table: DbTable<E, ID>, id: ID): E {
        return find(table, id) ?: throw NoSuchEntity("No ${table.dbName} with id $id")
    }

    override suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    loadForAll(ref: RelToOne<FROM, TO>, sources: Collection<FROM>?): Map<FROM, TO?> {
        if (sources == null || sources.isEmpty())
            return emptyMap()

        @Suppress("UNCHECKED_CAST")
        val rel = ref as RelToOneImpl<FROM, FROMID, TO, TOID>

        val futures = LinkedHashMap<FROM, Deferred<TO?>>()
        for (source in sources)
            futures.put(source, findAsync(source, rel))

        val result = LinkedHashMap<FROM, TO?>()
        for ((source, future) in futures)
            result[source] = future.await()

        return result
    }


    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    count(table: DbTable<E, ID>, filter: ExprBoolean<E>?): Long {
        val sb = Sql().SELECT("COUNT(*)").FROM(table).WHERE(filter)

        return query(sb).results[0].getLong(0)
    }

    override suspend fun <E : DbEntity<E, ID>, ID : Any>
    countAll(table: DbTable<E, ID>): Long {
        return count(table, null)
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    query(table: DbTable<E, ID>, filter: ExprBoolean<E>): List<E> {
        val entities = masterIndex[table]

        val sb = Sql().apply {
            SELECT(table.columnNames)
            FROM(table)
            WHERE(filter)
        }

        scheduleDelayedExec()
        return queryInternal(entities, sb)
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    queryAll(table: DbTable<E, ID>): List<E> {
        val entities = masterIndex[table]

        val sb = Sql().SELECT(table.columnNames).FROM(table)

        scheduleDelayedExec()
        return queryInternal(entities, sb)
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    query(query: EntityQuery<E>): List<E> {
        @Suppress("UNCHECKED_CAST")
        query as EntityQueryImpl<E, ID>

        val sb = Sql().apply {
            SELECT(query.table.columnNames)
            FROM(query.table)
            WHERE(query.filter)

            if (!query.orderBy.isEmpty()) {
                +" ORDER BY "
                tuple(query.orderBy) {
                    +it.expr
                    if (!it.isAscending)
                        +" DESC"
                }
            }

            if (query.offset != null || query.maxRowCount != null) {
                +" LIMIT "
                this(query.maxRowCount ?: Integer.MAX_VALUE)
                +" OFFSET "
                this(query.offset ?: 0)
            }
        }


        val result: List<E> = queryInternal(masterIndex[query.table], sb)
        scheduleDelayedExec()
        return result
    }

    override suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    find(from: FROM, relation: RelToOne<FROM, TO>): TO? {
        @Suppress("UNCHECKED_CAST")
        relation as RelToOneImpl<FROM, FROMID, TO, TOID>

        val toId: TOID? = relation.mapId(from)
        return if (toId != null) {
            load(relation.targetTable, toId)
        } else {
            null
        }
    }

    override suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    load(from: FROM, relation: RelToOne<FROM, TO>): TO {
        @Suppress("UNCHECKED_CAST")
        relation as RelToOneImpl<FROM, FROMID, TO, TOID>

        val toId: TOID = relation.mapId(from)
                ?: throw NoSuchEntity("${from.metainfo.dbName} ${from.id} has no reference to ${relation.targetTable.dbName}")

        return load(relation.targetTable, toId)
    }

    override suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    load(from: FROM, relation: RelToMany<FROM, TO>): List<TO> {
        val relInfo = masterIndex[relation]
        val loadState = relInfo[from.id]

        return when (loadState.state) {
            EntityState.LOADED -> {
                loadState.value
            }

            EntityState.LOADING -> {
                suspendCoroutine(loadState::addReceiver)
            }

            EntityState.INITIAL -> {
                scheduleDelayedExec()
                relInfo.addIdToLoad(from.id)
                suspendCoroutine(loadState::startedLoading)
            }
        }
    }

    override suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    load(from: FROM, relation: RelToMany<FROM, TO>, filter: ExprBoolean<TO>?): List<TO> {
        if (filter == null)
            return load(from, relation)

        val fromIds = setOf(from.id)

        @Suppress("UNCHECKED_CAST")
        val rel = relation as RelToManyImpl<FROM, FROMID, TO, TOID>

        val manyTable = rel.targetTable
        val condition = rel.createCondition(fromIds)

        return query(manyTable, condition.and(filter))
    }


    private suspend fun <E : DbEntity<E, ID>, ID: Any>
    queryInternal(entities: EntityIndex<E, ID>, sb: Sql): List<E> {
        val table = entities.metainfo

        val res = ArrayList<E>()
        for (json in query(sb).results) {
            val row = json.list

            val id = table.createId(row)
            val info = entities[id]

            val entity: E
            if (info.state == EntityState.LOADED) {
                val entityMaybe = info.value
                if (entityMaybe != null) {
                    entity = entityMaybe
                } else {
                    entity = table.create(this, id, row)
                    info.replaceResult(entity)
                }
            } else {
                entity = table.create(this, id, row)
                logger.trace { "Added entity with $id to ${table.dbName}" }
                info.handleResult(entity)
                if (entities.removeIdToLoad(id))
                    logger.trace { "Removed id to load $id" }
            }

            res.add(entity)
        }
        return res
    }

    private fun goLoadDelayed() {
        launch(Unconfined) {
            loadFirstDelayedEntity()
        }
    }

    private suspend fun loadFirstDelayedEntity() {
        for (index in masterIndex.allCachedToManyRels)
            if (index.loadNow(this)) // hoop because of type system :(
                return

        for (index in masterIndex.allCachedTables)
            if (index.loadNow(this))
                return
    }

    internal suspend fun <E : DbEntity<E, ID>, ID: Any>
    loadDelayedTable(index: EntityIndex<E, ID>): Boolean {
        val ids = index.getAndClearIdsToLoad() ?: return false

        // TODO: split large lists into chunks

        val table = index.metainfo
        val idField = table.idField

        logger.debug("Querying {} for ids {}", table.dbName, ids)

        val result: List<E>
        try {
            result = query(table, idField oneOf ids)
        } catch (e : Exception) {
            logger.error("Failed to query by IDs", e)
            index.reportError(ids, e)
            return true
        } finally {
            scheduleDelayedExec() // for next batch
        }

        for (entity: E in result)
            ids.remove(entity.id)

        if (!ids.isEmpty())
            index.reportNull(ids)

        return true
    }

    internal suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    loadDelayedToManyRel(index: ToManyIndex<FROM, FROMID, TO, TOID>): Boolean {
        val fromIds = index.getAndClearIdsToLoad() ?: return false

        // TODO: split large lists into chunks

        val rel = index.relation

        val manyTable = rel.targetTable
        val condition = rel.createCondition(fromIds)

        logger.debug { "Querying toMany ${rel.sourceTable.dbName} -> ${rel.targetTable.dbName} for source ids $fromIds" }

        val result = try {
            query(manyTable, condition)
        } catch (e: Throwable) {
            index.reportError(fromIds, e)
            return true
        } finally {
            scheduleDelayedExec() // for next batch
        }

        val mapped = LinkedHashMap<FROMID, ArrayList<TO>>()
        for (to in result) {
            val fromId = rel.reverseMap(to)!!
            mapped.computeIfAbsent(fromId, { _ -> ArrayList() }).add(to)
        }

        for ((key, value) in mapped) {
            index[key].handleResult(value)
        }

        fromIds.removeAll(mapped.keys)
        for (id in fromIds)
            index[id].handleResult(emptyList())

        return true
    }

    override suspend fun rollback() {
        dbOp { conn -> vx<Void> { conn.rollback(it) } }
    }

    override suspend fun commit() {
        dbOp { conn -> vx<Void> { conn.commit(it) } }
    }

    override suspend fun setAutoCommit(autoCommit: Boolean) {
        dbOp { conn -> vx<Void> { conn.setAutoCommit(autoCommit, it) } }
    }

    override suspend fun setTransactionIsolation(isolation: TransactionIsolation) {
        dbOp { conn -> vx<Void> { conn.setTransactionIsolation(isolation, it) } }
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    findMany(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E?> {
        @Suppress("NAME_SHADOWING")
        val ids = ids.toMutableSet()

        if (ids.isEmpty())
            return emptyMap()

        val result = LinkedHashMap<ID, E?>(ids.size)
        val futures = ArrayList<Deferred<E?>>(ids.size)

        for (id in ids) {
            result.put(id, null)
            futures.add(findAsync(table, id))
        }

        futures.mapNotNull { it.await() }
               .forEach { result.put(it.id, it) }

        return result
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    loadMany(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E> {
        @Suppress("NAME_SHADOWING")
        val ids = ids.toMutableSet()

        if (ids.isEmpty())
            return emptyMap()

        val result = LinkedHashMap<ID, E>(ids.size)
        val futures = ArrayList<Deferred<E>>(ids.size)

        for (id in ids)
            futures.add(loadAsync(table, id))

        futures.map { it.await() }
               .forEach { result.put(it.id, it) }

        return result
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    delete(entity: E): Boolean {
        return delete(entity.metainfo, setOf(entity.id)) > 0
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    delete(table: Z, filter: Z.() -> ExprBoolean<E>): Long {
        return executeUpdateLikeQuery(Sql().raw("DELETE").FROM(table).WHERE(table.filter()), table)
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    delete(table: Z, ids: Set<ID>): Long {
        if (ids.isEmpty())
            return 0L

        return executeUpdateLikeQuery(Sql().raw("DELETE").FROM(table).WHERE(table.idField oneOf ids), table)
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.delete(id: ID): Boolean {
        val table: Z = this // we're extension
        return executeUpdateLikeQuery(Sql().raw("DELETE").FROM(table).WHERE(table.idField eq id), table) > 0
    }

    private suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeUpdateLikeQuery(sqlBuilder: Sql, table: DbTable<E, ID>): Long {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        logger.debug("Executing update:\n{}\nparams: {}", sql, params)

        val updateResult = dbOp { conn -> vx<UpdateResult> { conn.updateWithParams(sql, params, it) } }

        // TODO: look at specific id, as to not destroy so much cache
        masterIndex.flushRelated(table)
        return updateResult.updated.toLong()
    }

    override suspend fun
    execute(sql: String) {
        logger.debug("Executing {}", sql)
        dbOp { conn -> vx<Void> { conn.execute(sql, it) } }
    }

    override fun <E : DbEntity<E, ID>, ID : Any> importJson(table: DbTable<E, ID>, json: JsonObject) {
        val list: List<Any?> = table.importFromJson(json)
        for (column in table.columns) {
            val value = list[column.indexInRow]
            if (value == null) {
                if (column.nonNull)
                    throw IllegalArgumentException("Missing value for column ${column.fieldName}")
            } else {
                column.sqlType.fromJson(value)
            }
        }
    }

    companion object : KLogging()
}
