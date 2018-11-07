package com.github.mslenc.dbktx.conn

import com.github.mslenc.asyncdb.DbConnection
import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.asyncdb.DbTxIsolation
import com.github.mslenc.asyncdb.DbTxMode
import com.github.mslenc.asyncdb.impl.values.DbValueLong
import com.github.mslenc.dbktx.aggr.AggregateQuery
import com.github.mslenc.dbktx.aggr.AggregateQueryImpl
import com.github.mslenc.dbktx.aggr.AggregateRow
import com.github.mslenc.dbktx.aggr.BoundAggregateExpr
import com.github.mslenc.dbktx.crud.*
import com.github.mslenc.dbktx.schema.Column
import com.github.mslenc.dbktx.expr.ExprBoolean
import com.github.mslenc.dbktx.expr.SqlEmitter
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.util.*
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import mu.KLogging

import java.util.*

import kotlin.collections.LinkedHashMap
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

internal fun <E : DbEntity<E, *>>
buildSelectQuery(query: EntityQueryImpl<E>): Sql {

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

internal fun <E : DbEntity<E, *>>
buildAggregateQuery(query: AggregateQueryImpl<E>): Sql {
    return Sql().apply {
        raw("SELECT ")
        for ((idx: Int, selectable: SqlEmitter) in query.selects.withIndex()) {
            if (idx > 0)
                raw(", ")
            selectable.toSql(this, true)
        }

        FROM(query.baseTable)
        WHERE(query.filters)
        GROUP_BY(query.groupBy)

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

internal fun <E : DbEntity<E, *>>
buildCountQuery(query: EntityQueryImpl<E>): Sql {

    return Sql().apply {
        SELECT("COUNT(*)")
        FROM(query.baseTable)
        WHERE(query.filters)
    }
}

internal fun <E : DbEntity<E, *>>
buildDeleteQuery(query: DeleteQueryImpl<E>): Sql {

    return Sql().apply {
        raw("DELETE FROM ")
        raw(query.baseTable.table.quotedDbName)
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


internal class DbLoaderInternal(private val publicDb: DbLoaderImpl, private val conn: DbConnection, private val delayedExecScheduler: DelayedExecScheduler) {
    internal val masterIndex = MasterIndex()
    private var scheduled: Boolean = false // whether the delayed loading is already scheduled
    private val queue = LinkedList<suspend (DbConnection)->Unit>()

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
        GlobalScope.launch(vertxDispatcher()) {
            performDelayedOps()
        }
    }

    internal fun addToQueue(op: suspend (DbConnection)->Unit) {
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
                    queue.removeFirst()(conn)
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

    internal suspend fun <E : DbEntity<E, *>>
    loadDelayedTable(mainIndex: EntityIndex<E>): Boolean {
        val indexAndKeys = mainIndex.getAndClearKeysToLoad() ?: return false
        loadDelayedTableInIndex(indexAndKeys)
        return true
    }

    private suspend fun <T: Any, E: DbEntity<E, *>>
    loadDelayedTableInIndex(indexAndKeys: IndexAndKeys<E, T>) {
        // TODO: split large lists into chunks

        val index = indexAndKeys.index
        val keys: MutableSet<T> = indexAndKeys.keys
        val table: DbTable<E, *> = index.table
        val keyDef: UniqueKeyDef<E, T> = index.keyDef

        logger.debug("Querying {} for keys {}", table.dbName, indexAndKeys.keys)

        val result: List<E>
        try {
            result = queryNow(table) { keyDef oneOf keys }
        } catch (e : Exception) {
            logger.error("Failed to query by IDs", e)
            index.reportError(keys, e)
            return
        }

        for (entity: E in result)
            keys.remove(keyDef(entity))

        if (!keys.isEmpty())
            index.reportNull(keys)
    }

    private suspend fun <E : DbEntity<E, *>>
    queryNow(entities: EntityIndex<E>, sb: Sql): List<E> {
        val res = ArrayList<E>()

        for (row in queryNow(sb)) {
            val entity: E = entities.rowLoaded(publicDb, row)
            res.add(entity)
        }

        return res
    }

    internal suspend fun <FROM : DbEntity<FROM, *>, FROM_KEY: Any, TO : DbEntity<TO, *>>
    loadDelayedToManyRel(index: ToManyIndex<FROM, FROM_KEY, TO>): Boolean {
        val fromKeys = index.getAndClearKeysToLoad() ?: return false

        // TODO: split large lists into chunks

        val rel = index.relation

        val manyTable = rel.targetTable

        logger.debug { "Querying toMany ${rel.sourceTable.dbName} -> ${rel.targetTable.dbName} for source keys $fromKeys" }

        val result = try {
            queryNow(manyTable) { rel.createCondition(fromKeys, currentTable()) }
        } catch (e: Throwable) {
            index.reportError(fromKeys, e)
            return true
        }

        val mapped = LinkedHashMap<FROM_KEY, ArrayList<TO>>()
        for (to in result) {
            val fromId = rel.reverseMap(to)!!
            mapped.computeIfAbsent(fromId) { _ -> ArrayList() }.add(to)
        }

        for ((key, value) in mapped) {
            index[key].handleResult(value)
        }

        fromKeys.removeAll(mapped.keys)
        if (fromKeys.isNotEmpty())
            index.reportNull(fromKeys)

        return true
    }

    private suspend fun <E : DbEntity<E, *>>
    queryNow(table: DbTable<E, *>, filter: FilterBuilder<E>.() -> ExprBoolean): List<E> {
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

    private suspend fun queryNow(sqlBuilder: Sql): DbResultSet {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        return conn.executeQuery(sql, params).await()
    }

    internal suspend fun <E : DbEntity<E, ID>, ID: Any>
    enqueueInsert(table: DbTable<E, ID>, sqlBuilder: Sql, explicitId: ID?): ID {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        return suspendCoroutine { continuation ->
            addToQueue { conn ->
                val id: ID
                try {
                    val updateResult = conn.executeUpdate(sql, params).await()

                    masterIndex.flushRelated(table)

                    val keys = updateResult.generatedIds
                    if (keys != null && keys.isNotEmpty() && table.keyIsAutogenerated) {
                        @Suppress("UNCHECKED_CAST")
                        val idColumn = table.primaryKey.getColumn(1) as Column<E, ID>

                        id = idColumn.sqlType.parseDbValue(DbValueLong(keys[0]))
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
                    val updateResult = conn.executeUpdate(sql, params).await()

                    // TODO: use specificIdx to not destroy so much cache
                    masterIndex.flushRelated(table)

                    continuation.resume(updateResult.rowsAffected)
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
                    val updateResult = conn.executeUpdate(sql, params).await()

                    // TODO: use specificIds to not destroy so much cache
                    masterIndex.flushRelated(table)

                    continuation.resume(updateResult.rowsAffected)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    internal suspend fun <E : DbEntity<E, ID>, ID : Any>
    find(table: DbTable<E, ID>, id: ID): E? {
        return find(table.primaryKey, id)
    }

    internal suspend fun <E : DbEntity<E, *>, KEY : Any>
    find(keyDef: UniqueKeyDef<E, KEY>, key: KEY): E? {
        val table = keyDef.table
        val entities = masterIndex[table]
        val index = entities.getSingleKeyIndex(keyDef)
        val entityInfo = index[key]

        when (entityInfo.state) {
            EntityState.LOADED -> {
                logger.trace("Key {} of table {} was already loaded", key, table.dbName)
                return entityInfo.value
            }

            EntityState.LOADING -> {
                logger.trace("Adding id {} of table {} to waiting list")
                return suspendCoroutine(entityInfo::addReceiver)
            }

            EntityState.INITIAL -> {
                logger.trace("Adding key {} of table {} to list for loading and initiating load", key, table.dbName)
                scheduleDelayedExec()
                index.addKeyToLoad(key)
                return suspendCoroutine(entityInfo::startedLoading)
            }
        }
    }

    internal suspend fun <FROM : DbEntity<FROM, *>, FROM_KEY: Any, TO : DbEntity<TO, *>>
    load(from: FROM, relation: RelToManyImpl<FROM, FROM_KEY, TO>): List<TO> {
        val relInfo = masterIndex.get<FROM, FROM_KEY, TO>(relation)
        val fromKey = relation.info.oneKey(from)
        val loadState = relInfo[fromKey]

        return when (loadState.state) {
            EntityState.LOADED -> {
                loadState.value
            }

            EntityState.LOADING -> {
                suspendCoroutine(loadState::addReceiver)
            }

            EntityState.INITIAL -> {
                scheduleDelayedExec()
                relInfo.addKeyToLoad(fromKey)
                suspendCoroutine(loadState::startedLoading)
            }
        }
    }

    internal suspend fun <E : DbEntity<E, *>>
    enqueueQuery(table: DbTable<E, *>, sb: Sql): List<E> {
        val entities = masterIndex[table]

        return suspendCoroutine { continuation ->
            addToQueue {
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

class DbLoaderImpl(conn: DbConnection, delayedExecScheduler: DelayedExecScheduler, override val requestTime: RequestTime) : DbConn {
    private val db = DbLoaderInternal(this, conn, delayedExecScheduler)

    override suspend fun
    query(sqlBuilder: Sql): DbResultSet {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        return suspendCoroutine { continuation ->
            db.addToQueue { conn ->
                try {
                    val result = conn.executeQuery(sql, params).await()
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

        val explicitId: ID? = dbTable.primaryKey.extract(values)
        if (explicitId == null && !dbTable.primaryKey.isAutoGenerated) {
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

    override suspend fun <E : DbEntity<E, *>>
    executeDelete(deleteQuery: DeleteQuery<E>): Long {
        deleteQuery as DeleteQueryImpl<E>
        val sql = buildDeleteQuery(deleteQuery)

        return deleteQuery.table.callEnqueueDeleteQuery(db, sql, null)
    }

    override suspend fun <E : DbEntity<E, ID>, ID : Any>
    findById(table: DbTable<E, ID>, id: ID?): E? {
        if (id == null)
            return null

        return db.find(table, id)
    }

    override suspend fun <E: DbEntity<E, *>, KEY: Any>
    findByKey(keyDef: UniqueKeyDef<E, KEY>, key: KEY): E? {
        return db.find(keyDef, key)
    }

    override suspend fun <E : DbEntity<E, ID>, ID : Any>
    loadById(table: DbTable<E, ID>, id: ID): E {
        return findById(table, id) ?: throw NoSuchEntity("No ${table.dbName} with id $id")
    }

    override suspend fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>
    loadForAll(ref: RelToOne<FROM, TO>, sources: Collection<FROM>): Map<FROM, TO?> {
        if (sources.isEmpty())
            return emptyMap()

        val futures = LinkedHashMap<FROM, Deferred<TO?>>()
        for (source in sources)
            futures[source] = defer { find(source, ref) }

        val result = LinkedHashMap<FROM, TO?>()
        for ((source, future) in futures)
            result[source] = future.await()

        return result
    }

    override suspend fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>
    loadForAll(ref: RelToMany<FROM, TO>, sources: Collection<FROM>): Map<FROM, List<TO>> {
        if (sources.isEmpty())
            return emptyMap()

        val futures = LinkedHashMap<FROM, Deferred<List<TO>>>()
        for (source in sources)
            futures[source] = defer { load(source, ref) }

        val result = LinkedHashMap<FROM, List<TO>>()
        for ((source, future) in futures)
            result[source] = future.await()

        return result
    }


    override suspend fun <E : DbEntity<E, *>>
    count(table: DbTable<E, *>, filter: FilterBuilder<E>.() -> ExprBoolean): Long {
        val entityQuery = EntityQueryImpl(table, this)
        entityQuery.filter(filter)

        val queryResult = query(buildCountQuery(entityQuery))
        val firstRow = queryResult[0]
        val firstColumn = firstRow[0] as Number
        return firstColumn.toLong()
    }

    override suspend fun <E : DbEntity<E,*>>
    executeSelect(query: EntityQuery<E>): List<E> {
        query as EntityQueryImpl<E>

        return db.enqueueQuery(query.table, buildSelectQuery(query))
    }

    override suspend fun <E : DbEntity<E,*>>
    executeSelect(query: AggregateQuery<E>, bindings: Map<Any, BoundAggregateExpr<*>>): List<AggregateRow> {
        query as AggregateQueryImpl<E>

        val sql = buildAggregateQuery(query)
        return query(sql).map { AggregateRow(it, bindings) }
    }

    override suspend fun <E : DbEntity<E, *>>
    executeCount(query: EntityQuery<E>): Long {
        query as EntityQueryImpl<E>

        val queryResult = query(buildCountQuery(query))
        val firstRow = queryResult[0]
        val firstColumn = firstRow[0] as Number
        return firstColumn.toLong()
    }

    override suspend fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>
    find(from: FROM, relation: RelToOne<FROM, TO>): TO? {
        relation as RelToOneImpl<FROM, TO, *>
        return relation.callFindByRelation(this, from)
    }

    override suspend fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>
    load(from: FROM, relation: RelToOne<FROM, TO>): TO {
        return find(from, relation) ?: throw NoSuchEntity("${from.metainfo.dbName} ${from.id} has no key/entity for ${relation.targetTable.dbName}")
    }

    internal suspend fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>, KEY: Any>
    findByRelation(from: FROM, relation: RelToOneImpl<FROM, TO, KEY>): TO? {
        val keyDef: UniqueKeyDef<TO, KEY> = relation.info.oneKey
        val key: KEY = relation.mapKey(from) ?: return null
        return db.find(keyDef, key)
    }

    override suspend fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>
    load(from: FROM, relation: RelToMany<FROM, TO>): List<TO> {
        relation as RelToManyImpl<FROM, *, TO>
        return relation.callLoad(db, from)
    }

    override suspend fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>
    load(from: FROM, relation: RelToMany<FROM, TO>, filter: FilterBuilder<TO>.()->ExprBoolean): List<TO> {
        relation as RelToManyImpl<FROM, *, TO>
        return relation.callLoadToManyWithFilter(this, from, filter)
    }

    internal suspend fun <FROM : DbEntity<FROM, *>, FROM_KEY: Any, TO: DbEntity<TO, *>>
    loadToManyWithFilter(from: FROM, relation: RelToManyImpl<FROM, FROM_KEY, TO>, filter: FilterBuilder<TO>.()->ExprBoolean): List<TO> {
        val fromKeys = setOf(relation.info.oneKey(from))

        val manyTable = relation.targetTable
        val query = manyTable.newQuery(this)

        query.filter { relation.createCondition(fromKeys, query.baseTable) }
        query.filter(filter)

        return query.run()
    }

    override suspend fun rollback() {
        suspendCoroutine<Unit> { continuation ->
            db.addToQueue { conn ->
                try {
                    conn.execute("ROLLBACK").await()
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
                    conn.execute("COMMIT").await()
                    continuation.resume(Unit)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    override suspend fun startTransaction() {
        suspendCoroutine<Unit> { continuation ->
            db.addToQueue { conn ->
                try {
                    conn.startTransaction().await()
                    continuation.resume(Unit)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    override suspend fun startTransaction(isolation: DbTxIsolation) {
        suspendCoroutine<Unit> { continuation ->
            db.addToQueue { conn ->
                try {
                    conn.startTransaction(isolation).await()
                    continuation.resume(Unit)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    override suspend fun startTransaction(mode: DbTxMode) {
        suspendCoroutine<Unit> { continuation ->
            db.addToQueue { conn ->
                try {
                    conn.startTransaction(mode).await()
                    continuation.resume(Unit)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    override suspend fun startTransaction(isolation: DbTxIsolation, mode: DbTxMode) {
        suspendCoroutine<Unit> { continuation ->
            db.addToQueue { conn ->
                try {
                    conn.startTransaction(isolation, mode).await()
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
                    conn.execute(sql).await()
                    continuation.resume(Unit)
                } catch (t: Throwable) {
                    continuation.resumeWithException(t)
                }
            }
        }
    }

    override fun <E : DbEntity<E, ID>, ID : Any>
    importJson(table: DbTable<E, ID>, json: JsonObject): E {
        val decoded = table.importFromJson(json)

        for (column in table.columns) {
            val value = decoded[column.indexInRow]
            if (value == null && column.nonNull)
                throw IllegalArgumentException("Missing value for column ${column.fieldName}")
        }

        return db.masterIndex[table].rowLoaded(this, decoded)
    }

    companion object : KLogging()
}
