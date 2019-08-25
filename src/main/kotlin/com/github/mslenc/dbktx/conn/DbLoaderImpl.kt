package com.github.mslenc.dbktx.conn

import com.github.mslenc.asyncdb.*
import com.github.mslenc.asyncdb.impl.values.DbValueLong
import com.github.mslenc.dbktx.crud.*
import com.github.mslenc.dbktx.schema.Column
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.filters.MatchAnything
import com.github.mslenc.dbktx.filters.MatchNothing
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.util.*
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import mu.KLogging

import java.util.*

import kotlin.collections.LinkedHashMap
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

internal fun <E : DbEntity<E, *>>
buildSelectQuery(query: EntityQueryImpl<E>, selectForUpdate: Boolean): Sql {

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

        if (selectForUpdate) {
            +" FOR UPDATE"
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
    if (query.filters == MatchAnything)
        throw RuntimeException("Missing filters")

    return Sql().apply {
        raw("DELETE FROM ")
        raw(query.baseTable.table.quotedDbName)
        WHERE(query.filters)
    }
}


private fun <E : DbEntity<E, ID>, ID: Any>
createUpdateQuery(table: TableInQuery<E>, values: EntityValues<E>, filter: FilterExpr): Sql? {
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

private fun <E : DbEntity<E, ID>, ID: Any> createInsertQuery(table: DbTable<E, ID>, values: EntityValues<E>, dbType: DbType, keyAutogenerates: Boolean): Sql {
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

        if (keyAutogenerates && dbType == DbType.POSTGRES) {
            if (table.primaryKey.isAutoGenerated) {
                +" RETURNING "
                raw(table.primaryKey.getColumn(1).quotedFieldName)
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


internal class DbLoaderInternal(private val publicDb: DbLoaderImpl, internal val conn: DbConnection) {
    internal val masterIndex = MasterIndex(publicDb.scope)
    private var scheduled: Boolean = false // whether the delayed loading is already scheduled

    private suspend fun scheduleDelayedExec() {
        if (scheduled)
            return

        scheduled = true
        publicDb.scope.launch {
            performDelayedOps()
        }
    }

    private suspend fun performDelayedOps() {
        var any = false

        try {
            startOver@
            while (true) {
                for (index in masterIndex.allCachedLoaders) {
                    if (index.loadNow(this)) {
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
        } catch (e : Throwable) {
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
    queryNow(entities: EntityIndex<E>, sb: Sql, selectForUpdate: Boolean): List<E> {
        val res = ArrayList<E>()

        for (row in queryNow(sb)) {
            val entity: E = entities.rowLoaded(publicDb, row, selectForUpdate)
            res.add(entity)
        }

        return res
    }

    internal suspend fun <KEY: Any, RESULT>
    loadCustomBatchNow(index: BatchingLoaderIndex<KEY, RESULT>): Boolean {
        val fromKeys = index.getAndClearKeysToLoad() ?: return false

        logger.debug { "Loading from custom loader ${index.loader} for source keys $fromKeys" }

        val result = try {
            index.loader.loadNow(fromKeys, publicDb)
        } catch (e: Throwable) {
            index.reportError(fromKeys, e)
            return true
        }

        for ((key, value) in result) {
            index[key].handleResult(value)
        }

        fromKeys.removeAll(result.keys)
        if (fromKeys.isNotEmpty())
            index.reportNull(fromKeys)

        return true
    }

    private suspend fun <E : DbEntity<E, *>>
    queryNow(table: DbTable<E, *>, filter: FilterBuilder<E>.() -> FilterExpr): List<E> {
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

        return queryNow(entities, sb, false)
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

        val updateResult = conn.executeUpdate(sql, params).await()

        masterIndex.flushRelated(table)

        val keys = updateResult.generatedIds
        val id: ID
        if (keys != null && keys.isNotEmpty() && table.keyIsAutogenerated) {
            @Suppress("UNCHECKED_CAST")
            val idColumn = table.primaryKey.getColumn(1) as Column<E, ID>

            id = idColumn.sqlType.parseDbValue(DbValueLong(keys[0]))
        } else {
            id = explicitId ?: throw IllegalStateException("ID missing")
        }

        return id
    }

    internal suspend fun <E : DbEntity<E, ID>, ID: Any>
    enqueueUpdateQuery(table: DbTable<E, ID>, sqlBuilder: Sql): Long {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        logger.debug("Executing update:\n{}\nparams: {}", sql, params)

        val updateResult = conn.executeUpdate(sql, params).await()

        masterIndex.flushRelated(table)

        return updateResult.rowsAffected
    }

    internal suspend fun <E : DbEntity<E, ID>, ID: Any>
    enqueueDeleteQuery(table: DbTable<E, ID>, sqlBuilder: Sql): Long {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        logger.debug("Executing delete:\n{}\nparams: {}", sql, params)

        val updateResult = conn.executeUpdate(sql, params).await()

        masterIndex.flushRelated(table)

        return updateResult.rowsAffected
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
                index.addKeyToLoad(key)
                scheduleDelayedExec()
                return suspendCoroutine(entityInfo::startedLoading)
            }
        }
    }

    internal suspend fun <KEY: Any, RESULT>
    load(loader: BatchingLoader<KEY, RESULT>, key: KEY): RESULT {
        val loaderInfo = masterIndex[loader]
        val loadState = loaderInfo[key]

        return when (loadState.state) {
            EntityState.LOADED -> {
                loadState.value
            }

            EntityState.LOADING -> {
                suspendCoroutine(loadState::addReceiver)
            }

            EntityState.INITIAL -> {
                loaderInfo.addKeyToLoad(key)
                scheduleDelayedExec()
                suspendCoroutine(loadState::startedLoading)
            }
        }
    }

    internal suspend fun <E : DbEntity<E, *>>
    enqueueQuery(table: DbTable<E, *>, sb: Sql, selectForUpdate: Boolean): List<E> {
        val entities = masterIndex[table]
        return queryNow(entities, sb, selectForUpdate)
    }

    companion object : KLogging()
}

class DbLoaderImpl(conn: DbConnection, override val scope: CoroutineScope, override val requestTime: RequestTime) : DbConn {
    private val db = DbLoaderInternal(this, conn)

    override suspend fun
    query(sqlBuilder: Sql): DbResultSet {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        return db.conn.executeQuery(sql, params).await()
    }

    override fun <E : DbEntity<E, ID>, ID : Any> flushTableCache(table: DbTable<E, ID>) {
        db.masterIndex.flushRelated(table)
    }

    override fun flushAllCaches() {
        db.masterIndex.flushAll()
    }

    override suspend fun streamQuery(sqlBuilder: Sql, receiver: (DbRow) -> Unit): Long {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params
        var numRows = 0L

        return suspendCoroutine { cont ->
            val observer = object : DbQueryResultObserver {
                override fun onNext(row: DbRow) {
                    numRows++
                    receiver(row)
                }

                override fun onError(t: Throwable) {
                    cont.resumeWithException(t)
                }

                override fun onCompleted() {
                    cont.resume(numRows)
                }
            }

            db.conn.streamQuery(sql, observer, params)
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

        val sqlBuilder = createInsertQuery(dbTable, values, db.conn.config.type(), explicitId == null)

        return db.enqueueInsert(dbTable, sqlBuilder, explicitId)
    }




    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeUpdate(table: TableInQuery<E>, filters: FilterExpr, values: EntityValues<E>): Long {
        if (filters == MatchNothing)
            return 0

        val sqlBuilder = createUpdateQuery(table, values, filters) ?: return 0

        @Suppress("UNCHECKED_CAST")
        return db.enqueueUpdateQuery(table.table as DbTable<E, ID>, sqlBuilder)
    }

    override suspend fun <E : DbEntity<E, *>>
    executeDelete(deleteQuery: DeleteQuery<E>): Long {
        deleteQuery as DeleteQueryImpl<E>

        if (deleteQuery.filters == MatchNothing)
            return 0

        val sql = buildDeleteQuery(deleteQuery)

        return deleteQuery.table.callEnqueueDeleteQuery(db, sql)
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
    loadForAll(ref: RelToOne<FROM, TO>, sources: Collection<FROM>): Map<FROM, TO?> = supervisorScope {
        if (sources.isEmpty())
            return@supervisorScope emptyMap<FROM, TO?>()

        val futures = LinkedHashMap<FROM, Deferred<TO?>>()
        for (source in sources)
            futures[source] = async { find(source, ref) }

        val result = LinkedHashMap<FROM, TO?>()
        for ((source, future) in futures)
            result[source] = future.await()

        result
    }


    override suspend fun <KEY : Any, RESULT> load(loader: BatchingLoader<KEY, RESULT>, key: KEY): RESULT {
        return db.load(loader, key)
    }

    override suspend fun <KEY : Any, RESULT> loadForAll(loader: BatchingLoader<KEY, RESULT>, keys: Collection<KEY>): Map<KEY, RESULT> = supervisorScope {
        if (keys.isEmpty())
            return@supervisorScope emptyMap<KEY, RESULT>()

        val futures = LinkedHashMap<KEY, Deferred<RESULT>>()
        for (key in keys)
            futures[key] = async { load(loader, key) }

        val result = LinkedHashMap<KEY, RESULT>()
        for ((key, future) in futures)
            result[key] = future.await()

        result
    }

    override suspend fun <E : DbEntity<E, *>>
    count(table: DbTable<E, *>, filter: FilterBuilder<E>.() -> FilterExpr): Long {
        val entityQuery = EntityQueryImpl(table, this)
        entityQuery.filter(filter)

        if (entityQuery.filteringState() == FilteringState.MATCH_NONE)
            return 0L

        val queryResult = query(buildCountQuery(entityQuery))
        val firstRow = queryResult[0]
        val firstColumn = firstRow[0] as Number
        return firstColumn.toLong()
    }

    override suspend fun <E : DbEntity<E,*>>
    executeSelect(query: EntityQuery<E>, selectForUpdate: Boolean): List<E> {
        query as EntityQueryImpl<E>

        if (query.filteringState() == FilteringState.MATCH_NONE)
            return emptyList()

        return db.enqueueQuery(query.table, buildSelectQuery(query, selectForUpdate), selectForUpdate)
    }

    override suspend fun <E : DbEntity<E, *>>
    executeCount(query: EntityQuery<E>): Long {
        query as EntityQueryImpl<E>

        if (query.filteringState() == FilteringState.MATCH_NONE)
            return 0L

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
    load(from: FROM, relation: RelToMany<FROM, TO>, filter: FilterBuilder<TO>.()->FilterExpr): List<TO> {
        relation as RelToManyImpl<FROM, *, TO>
        return relation.callLoadToManyWithFilter(this, from, filter)
    }

    internal suspend fun <FROM : DbEntity<FROM, FROM_KEY>, FROM_KEY: Any, TO: DbEntity<TO, *>>
    loadToManyWithFilter(from: FROM, relation: RelToManyImpl<FROM, FROM_KEY, TO>, filter: FilterBuilder<TO>.()->FilterExpr): List<TO> {
        val fromKeys = setOf(relation.info.oneKey(from))

        val manyTable = relation.targetTable
        val query = manyTable.newQuery(this)

        query.filter { relation.createCondition(fromKeys, query.baseTable) }
        query.filter(filter)

        if (query.filteringState() == FilteringState.MATCH_NONE)
            return emptyList()

        return query.run()
    }

    override suspend fun rollback() {
        db.conn.rollback().await()
    }

    override suspend fun commit() {
        db.conn.commit().await()
    }

    override suspend fun commitAndChain() {
        db.conn.commitAndChain().await()
    }

    override suspend fun rollbackAndChain() {
        db.conn.rollbackAndChain().await()
    }

    override suspend fun startTransaction() {
        db.conn.startTransaction().await()
    }

    override suspend fun startTransaction(isolation: DbTxIsolation) {
        db.conn.startTransaction(isolation).await()
    }

    override suspend fun startTransaction(mode: DbTxMode) {
        db.conn.startTransaction(mode).await()
    }

    override suspend fun startTransaction(isolation: DbTxIsolation, mode: DbTxMode) {
        db.conn.startTransaction(isolation, mode).await()
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    findByIds(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E?> = supervisorScope {
        @Suppress("NAME_SHADOWING")
        val ids = ids.toSet()

        if (ids.isEmpty())
            return@supervisorScope emptyMap<ID, E?>()

        val result = LinkedHashMap<ID, E?>(ids.size)
        val futures = ArrayList<Deferred<E?>>(ids.size)

        for (id in ids) {
            result[id] = null
            futures.add(async { findById(table, id) })
        }

        futures.mapNotNull { it.await() }
               .forEach { result[it.id] = it }

        result
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    loadByIds(table: DbTable<E, ID>, ids: Iterable<ID>): Map<ID, E> = supervisorScope {
        @Suppress("NAME_SHADOWING")
        val ids = ids.toMutableSet()

        if (ids.isEmpty())
            return@supervisorScope emptyMap<ID, E>()

        val result = LinkedHashMap<ID, E>(ids.size)
        val futures = ArrayList<Deferred<E>>(ids.size)

        for (id in ids)
            futures.add(async { loadById(table, id) })

        futures.map { it.await() }
               .forEach { result[it.id] = it }

        result
    }



    override suspend fun <E : DbEntity<E, ID>, ID: Any, Z: DbTable<E, ID>>
    Z.deleteById(id: ID): Boolean {
        val table: Z = this // we're extension
        return deleteByIds(table, setOf(id)) > 0
    }


    override suspend fun
    execute(sql: String) {
        logger.debug("Executing {}", sql)

        db.conn.execute(sql).await()
    }

    override fun <E : DbEntity<E, ID>, ID : Any>
    importJson(table: DbTable<E, ID>, json: Map<String, Any?>): E {
        val decoded = table.importFromJson(json)

        for (column in table.columns) {
            val value = decoded[column.indexInRow]
            if (value == null && column.nonNull)
                throw IllegalArgumentException("Missing value for column ${column.fieldName}")
        }

        return db.masterIndex[table].rowLoaded(this, decoded, false)
    }

    companion object : KLogging()
}
