package com.xs0.dbktx.conn

import com.xs0.dbktx.crud.*
import com.xs0.dbktx.schema.Column
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.schema.*
import com.xs0.dbktx.util.*
import io.vertx.core.json.JsonObject
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLConnection
import io.vertx.ext.sql.TransactionIsolation
import io.vertx.ext.sql.UpdateResult
import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.launch
import mu.KLogging

import java.util.*

import kotlin.collections.LinkedHashMap
import kotlin.coroutines.experimental.suspendCoroutine

internal fun <E : DbEntity<E, ID>, ID: Any>
buildSelectQuery(query: EntityQueryImpl<E, ID>): Sql {

    return Sql().apply {
        SELECT(query.table.defaultColumnNames)
        FROM(query.baseTable)
        WHERE(query.filters)

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
}

internal fun <E : DbEntity<E, ID>, ID: Any>
buildCountQuery(query: EntityQueryImpl<E, ID>): Sql {

    return Sql().apply {
        SELECT("COUNT(*)")
        FROM(query.baseTable)
        WHERE(query.filters)
    }
}

internal fun <E : DbEntity<E, ID>, ID: Any>
buildDeleteQuery(query: DeleteQueryImpl<E, ID>): Sql {

    return Sql().apply {
        raw("DELETE")
        FROM(query.baseTable)
        WHERE(query.filters ?: throw RuntimeException("Missing filters"))
    }
}


private fun <E : DbEntity<E, ID>, ID: Any>
createUpdateQuery(table: TableInQuery<E>, values: EntityValues<E>, filter: ExprBoolean?): Sql? {
    if (values.isEmpty())
        return null

    return Sql().apply {
        +"UPDATE "
        raw(table.table.quotedDbName)
        +" SET "
        tuple(values) { column ->
            raw(column.quotedFieldName)
            +"="
            emitValue(column, values, this)
        }
        WHERE(filter)
    }
}

private fun <E : DbEntity<E, ID>, ID: Any> createInsertQuery(table: DbTable<E, ID>, values: EntityValues<E>): Sql {
    return Sql().apply {
        +"INSERT INTO "
        raw(table.quotedDbName)
        paren {
            tuple(values) {
                column -> raw(column.quotedFieldName)
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


internal class DbLoaderInternal(private val publicDb: DbLoaderImpl, conn: SQLConnection, private val delayedExecScheduler: DelayedExecScheduler) {
    private val masterIndex = MasterIndex()
    private var scheduled: Boolean = false // whether the delayed loading is already scheduled
    private val queue = LinkedList<suspend (SQLConnection)->Unit>()
    private val _dbConnection: SQLConnection

    init {
        this._dbConnection = MultiQueryConn(conn)
    }

    private fun scheduleDelayedExec() {
        if (scheduled)
            return

        logger.debug("scheduling..")
        scheduled = true

        delayedExecScheduler.schedule({
            logger.debug("executing..")
            goLoadDelayed()
        })
    }

    private fun goLoadDelayed() {
        launch(Unconfined) {
            performDelayedOps()
        }
    }

    internal fun addToQueue(op: suspend (SQLConnection)->Unit) {
        scheduleDelayedExec()
        queue.add(op)
    }

    private suspend fun performDelayedOps() {
        var any = false

        try {
            startOver@
            while (true) {
                if (queue.isNotEmpty()) {
                    any = true
                    queue.removeFirst()(_dbConnection)
                    continue@startOver
                }

                for (index in masterIndex.allCachedToManyRels) {
                    if (index.loadNow(this)) { // hoop because of type system :(
                        any = true
                        continue@startOver
                    }
                }

                for (index in masterIndex.allCachedTables) {
                    if (index.loadNow(this)) {
                        any = true
                        continue@startOver
                    }
                }

                break
            }
        } finally {
            scheduled = false
            if (any) {
                scheduleDelayedExec()
            }
        }
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
            result = queryNow(table) { idField oneOf ids }
        } catch (e : Exception) {
            logger.error("Failed to query by IDs", e)
            index.reportError(ids, e)
            return true
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

        logger.debug { "Querying toMany ${rel.sourceTable.dbName} -> ${rel.targetTable.dbName} for source ids $fromIds" }

        val result = try {
            queryNow(manyTable) { rel.createCondition(fromIds, currentTable()) }
        } catch (e: Throwable) {
            index.reportError(fromIds, e)
            return true
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

    private suspend fun <E : DbEntity<E, ID>, ID: Any>
    queryNow(entities: EntityIndex<E, ID>, sb: Sql): List<E> {
        val table = entities.metainfo

        val res = ArrayList<E>()
        for (json in queryNow(sb).results) {
            val row = json.list

            val id = table.createId(row)
            val info = entities[id]

            val entity: E
            if (info.state == EntityState.LOADED) {
                val entityMaybe = info.value
                if (entityMaybe != null) {
                    entity = entityMaybe
                } else {
                    entity = table.create(publicDb, id, row)
                    info.replaceResult(entity)
                }
            } else {
                entity = table.create(publicDb, id, row)
                logger.trace { "Added entity with $id to ${table.dbName}" }
                info.handleResult(entity)
                if (entities.removeIdToLoad(id))
                    logger.trace { "Removed id to load $id" }
            }

            res.add(entity)
        }
        return res
    }

    private suspend fun <E : DbEntity<E, ID>, ID: Any>
    queryNow(table: DbTable<E, ID>, filter: FilterBuilder<E>.() -> ExprBoolean): List<E> {
        val entities = masterIndex[table]

        val query = SimpleSelectQueryImpl()
        val boundTable = BaseTableInQuery(query, table)
        val filterBuilder = TableInQueryBoundFilterBuilder(boundTable)
        val filterExpr = filterBuilder.filter()

        val sb = Sql().apply {
            SELECT(table.defaultColumnNames)
            FROM(boundTable)
            WHERE(filterExpr)
        }

        return queryNow(entities, sb)
    }

    private suspend fun queryNow(sqlBuilder: Sql): ResultSet {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        return vx { handler -> _dbConnection.queryWithParams(sql, params, handler) }
    }

    fun close() {
        _dbConnection.close()
    }

    internal suspend fun <E : DbEntity<E, ID>, ID: Any>
    enqueueInsert(table: DbTable<E, ID>, sqlBuilder: Sql, explicitId: ID?): ID {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        return suspendCoroutine { continuation ->
            addToQueue { conn ->
                val id: ID
                try {
                    val updateResult = vx<UpdateResult> { handler -> conn.updateWithParams(sql, params, handler) }

                    masterIndex.flushRelated(table)

                    val keys = updateResult.keys
                    if (keys != null && !keys.isEmpty && table.keyIsAutogenerated) {
                        @Suppress("UNCHECKED_CAST")
                        val idColumn = table.idField as Column<E, ID>

                        id = idColumn.sqlType.fromJson(keys.getValue(0))
                    } else {
                        id = explicitId ?: throw IllegalStateException("ID missing")
                    }
                    continuation.resume(id)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    internal suspend fun <E : DbEntity<E, ID>, ID: Any>
    enqueueUpdateQuery(table: DbTable<E, ID>, sqlBuilder: Sql, specificIds: Set<ID>?): Long {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        return suspendCoroutine { continuation ->
            addToQueue { conn ->
                logger.debug("Executing update:\n{}\nparams: {}", sql, params)

                try {
                    val updateResult = vx<UpdateResult> { conn.updateWithParams(sql, params, it) }

                    // TODO: use specificIdx to not destroy so much cache
                    masterIndex.flushRelated(table)

                    continuation.resume(updateResult.updated.toLong())
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    internal suspend fun <E : DbEntity<E, ID>, ID: Any>
    enqueueDeleteQuery(table: DbTable<E, ID>, sqlBuilder: Sql, specificIds: Set<ID>?): Long {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        return suspendCoroutine { continuation ->
            addToQueue { conn ->
                logger.debug("Executing delete:\n{}\nparams: {}", sql, params)

                try {
                    val updateResult = vx<UpdateResult> { conn.updateWithParams(sql, params, it) }

                    // TODO: use specificIds to not destroy so much cache
                    masterIndex.flushRelated(table)

                    continuation.resume(updateResult.updated.toLong())
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    internal suspend fun <E : DbEntity<E, ID>, ID : Any>
    find(table: DbTable<E, ID>, id: ID): E? {
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

    internal suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
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

    internal suspend fun <E : DbEntity<E, ID>, ID: Any>
    enqueueQuery(table: DbTable<E, ID>, sb: Sql): List<E> {
        val entities = masterIndex[table]

        return suspendCoroutine { continuation ->
            addToQueue { _ ->
                try {
                    continuation.resume(queryNow(entities, sb))
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    companion object : KLogging()
}

class DbLoaderImpl(conn: SQLConnection, delayedExecScheduler: DelayedExecScheduler) : DbConn, AutoCloseable {
    private val db = DbLoaderInternal(this, conn, delayedExecScheduler)

    override fun close() {
        db.close()
    }

    override suspend fun
    query(sqlBuilder: Sql): ResultSet {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        return suspendCoroutine { continuation ->
            db.addToQueue { conn ->
                try {
                    val result = vx<ResultSet> { handler -> conn.queryWithParams(sql, params, handler) }
                    continuation.resume(result)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }



    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeInsert(table: TableInQuery<E>, values: EntityValues<E>): ID {
        @Suppress("UNCHECKED_CAST")
        val dbTable = table.table as DbTable<E, ID>

        val explicitId: ID? = dbTable.idField.extract(values)
        if (explicitId == null && !dbTable.idField.isAutoGenerated) {
            throw IllegalArgumentException("Missing the ID")
        }

        val sqlBuilder = createInsertQuery(dbTable, values)

        return db.enqueueInsert(dbTable, sqlBuilder, explicitId)
    }




    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeUpdate(table: TableInQuery<E>, filters: ExprBoolean?, values: EntityValues<E>, specificIds: Set<ID>?): Long {
        val sqlBuilder = createUpdateQuery(table, values, filters) ?: return 0

        @Suppress("UNCHECKED_CAST")
        return db.enqueueUpdateQuery(table.table as DbTable<E, ID>, sqlBuilder, specificIds)
    }

    override suspend fun <E : DbEntity<E, ID>, ID : Any> executeDelete(deleteQuery: DeleteQuery<E>): Long {
        @Suppress("UNCHECKED_CAST")
        val sql = buildDeleteQuery(deleteQuery as DeleteQueryImpl<E, ID>)

        return db.enqueueDeleteQuery(deleteQuery.table, sql, null)
    }

    override suspend fun <E : DbEntity<E, ID>, ID : Any>
    findById(table: DbTable<E, ID>, id: ID?): E? {
        if (id == null)
            return null

        return db.find(table, id)
    }

    override suspend fun <E : DbEntity<E, ID>, ID : Any>
    loadById(table: DbTable<E, ID>, id: ID): E {
        return findById(table, id) ?: throw NoSuchEntity("No ${table.dbName} with id $id")
    }

    override suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    loadForAll(ref: RelToOne<FROM, TO>, sources: Collection<FROM>): Map<FROM, TO?> {
        if (sources.isEmpty())
            return emptyMap()

        @Suppress("UNCHECKED_CAST")
        val rel = ref as RelToOneImpl<FROM, FROMID, TO, TOID>

        val futures = LinkedHashMap<FROM, Deferred<TO?>>()
        for (source in sources)
            futures[source] = defer { find(source, rel) }

        val result = LinkedHashMap<FROM, TO?>()
        for ((source, future) in futures)
            result[source] = future.await()

        return result
    }


    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    count(table: DbTable<E, ID>, filter: FilterBuilder<E>.() -> ExprBoolean): Long {
        val entityQuery = EntityQueryImpl(table, this)
        entityQuery.filter(filter)

        return query(buildCountQuery(entityQuery)).results[0].getLong(0)
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeSelect(query: EntityQuery<E>): List<E> {
        @Suppress("UNCHECKED_CAST")
        query as EntityQueryImpl<E, ID>

        return db.enqueueQuery(query.table, buildSelectQuery(query))
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeCount(query: EntityQuery<E>): Long {
        @Suppress("UNCHECKED_CAST")
        query as EntityQueryImpl<E, ID>

        return query(buildCountQuery(query)).results[0].getLong(0)
    }

    override suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    find(from: FROM, relation: RelToOne<FROM, TO>): TO? {
        @Suppress("UNCHECKED_CAST")
        relation as RelToOneImpl<FROM, FROMID, TO, TOID>

        val toId: TOID? = relation.mapId(from)
        return if (toId != null) {
            loadById(relation.targetTable, toId)
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

        return loadById(relation.targetTable, toId)
    }

    override suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    load(from: FROM, relation: RelToMany<FROM, TO>): List<TO> {
        return db.load(from, relation)
    }

    override suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    load(from: FROM, relation: RelToMany<FROM, TO>, filter: FilterBuilder<TO>.()->ExprBoolean): List<TO> {
        val fromIds = setOf(from.id)

        @Suppress("UNCHECKED_CAST")
        val rel = relation as RelToManyImpl<FROM, FROMID, TO, TOID>

        val manyTable = rel.targetTable
        val query = manyTable.newQuery(this)

        query.filter { rel.createCondition(fromIds, query.baseTable) }
        query.filter(filter)

        return query.run()
    }

    override suspend fun rollback() {
        suspendCoroutine<Unit> { continuation ->
            db.addToQueue { conn ->
                try {
                    vx<Void> { conn.rollback(it) }
                    continuation.resume(Unit)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    override suspend fun commit() {
        suspendCoroutine<Unit> { continuation ->
            db.addToQueue { conn ->
                try {
                    vx<Void> { conn.commit(it) }
                    continuation.resume(Unit)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    override suspend fun setAutoCommit(autoCommit: Boolean) {
        suspendCoroutine<Unit> { continuation ->
            db.addToQueue { conn ->
                try {
                    vx<Void> { conn.setAutoCommit(autoCommit, it) }
                    continuation.resume(Unit)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    override suspend fun setTransactionIsolation(isolation: TransactionIsolation) {
        suspendCoroutine<Unit> { continuation ->
            db.addToQueue { conn ->
                try {
                    vx<Void> { conn.setTransactionIsolation(isolation, it) }
                    continuation.resume(Unit)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    findByIds(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E?> {
        @Suppress("NAME_SHADOWING")
        val ids = ids.toSet()

        if (ids.isEmpty())
            return emptyMap()

        val result = LinkedHashMap<ID, E?>(ids.size)
        val futures = ArrayList<Deferred<E?>>(ids.size)

        for (id in ids) {
            result[id] = null
            futures.add(defer { findById(table, id) })
        }

        futures.mapNotNull { it.await() }
               .forEach { result[it.id] = it }

        return result
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    loadByIds(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E> {
        @Suppress("NAME_SHADOWING")
        val ids = ids.toMutableSet()

        if (ids.isEmpty())
            return emptyMap()

        val result = LinkedHashMap<ID, E>(ids.size)
        val futures = ArrayList<Deferred<E>>(ids.size)

        for (id in ids)
            futures.add( defer { loadById(table, id) })

        futures.map { it.await() }
               .forEach { result[it.id] = it }

        return result
    }



    override suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.deleteById(id: ID): Boolean {
        val table: Z = this // we're extension
        return deleteByIds(table, setOf(id)) > 0
    }


    override suspend fun
    execute(sql: String) {
        logger.debug("Executing {}", sql)
        return suspendCoroutine { continuation ->
            db.addToQueue { conn ->
                try {
                    vx<Void> { conn.execute(sql, it) }
                    continuation.resume(Unit)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    override fun <E : DbEntity<E, ID>, ID : Any>
    importJson(table: DbTable<E, ID>, json: JsonObject) {
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
