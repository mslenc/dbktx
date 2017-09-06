package com.xs0.dbktx

import io.vertx.core.*
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLConnection
import io.vertx.ext.sql.TransactionIsolation
import io.vertx.ext.sql.UpdateResult
import kotlinx.coroutines.experimental.Deferred
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import si.datastat.db.api.*
import si.datastat.db.api.expr.Expr
import si.datastat.db.api.expr.ExprBoolean
import si.datastat.db.api.expr.SqlBuilder
import si.datastat.db.impl.EntityQueryImpl
import si.datastat.db.impl.OrderSpec
import si.datastat.db.impl.RelToManyImpl
import si.datastat.db.impl.RelToOneImpl

import java.util.*

import si.datastat.db.api.util.DbUtils.forwardFailure
import java.awt.FontFormatException
import kotlin.collections.LinkedHashMap
import kotlin.coroutines.experimental.suspendCoroutine

class DbLoaderImpl<CTX>(conn: SQLConnection, override var context: CTX, private val delayedExecScheduler: DelayedExecScheduler) : DbConn<CTX> {

    private val conn: SQLConnection
    private val masterIndex = MasterIndex()
    private var scheduled: Boolean = false // used when batching loads, true means that we're already scheduled (see scheduleDelayedExec())

    init {
        this.conn = MultiQueryConn(conn)
    }

    private fun scheduleDelayedExec() {
        if (scheduled)
            return

        delayedExecScheduler.schedule({
            scheduled = false
            goLoadDelayed()
        })

        scheduled = true
    }

    override suspend fun
    query(sqlBuilder: SqlBuilder): ResultSet {
        return vx { handler ->
            conn.queryWithParams(sqlBuilder.getSql(), sqlBuilder.params, handler)
        }
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeInsert(table: DbTable<E, ID>, values: EntityValues<E>): ID? {
        if (!table.idField.isAutoGenerated) {
            if (!table.idField.isSet(values)) {
                throw IllegalArgumentException("Missing the ID")
            }
        }

        val sqlBuilder = createInsertQuery(table, values)

        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        val updateResult = vx<UpdateResult> { conn.updateWithParams(sql, params, it) }

        masterIndex.flushRelated(table)

        val keys = updateResult.keys
        if (keys != null && !keys.isEmpty && table.keyIsAutogenerated) {
            @Suppress("UNCHECKED_CAST")
            val idColumn = table.idField as Column<E, ID>
            return idColumn.sqlType.fromJson(keys.getValue(0))
        } else {
            return null
        }
    }

    private fun <E : DbEntity<E, ID>, ID: Any> createInsertQuery(table: DbTable<E, ID>, values: EntityValues<E>): SqlBuilder {
        val sb = SqlBuilder()
        sb.sql("INSERT INTO ").name(table).sql("(")
        var first: Boolean

        first = true
        for ((key) in values) {
            if (first) {
                first = false
            } else {
                sb.sql(", ")
            }

            sb.name(key)
        }

        sb.sql(") VALUES (")
        first = true
        for ((_, value) in values) {
            if (first) {
                first = false
            } else {
                sb.sql(", ")
            }

            value.toSql(sb, true)
        }
        sb.sql(")")

        return sb
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    executeUpdate(table: DbTable<E, ID>, filter: Expr<in E, Boolean>, values: EntityValues<E>, specificIds: Set<ID>?): Long {
        val sqlBuilder = createUpdateQuery(table, values, filter) ?: return 0
        return executeUpdateLikeQuery(sqlBuilder, table)
    }

    private fun <E : DbEntity<E, ID>, ID: Any>
    createUpdateQuery(table: DbTable<E, ID>, values: EntityValues<E>, filter: Expr<in E, Boolean>?): SqlBuilder? {
        if (values.isEmpty())
            return null

        val sb = SqlBuilder()
        sb.sql("UPDATE ").name(table).sql(" SET ")

        var first = true
        for ((key, value) in values) {
            if (first) {
                first = false
            } else {
                sb.sql(", ")
            }

            sb.name(key)
            sb.sql("=")
            value.toSql(sb, true)
        }

        if (filter != null) {
            sb.sql(" WHERE ")
            filter.toSql(sb, true)
        }

        return sb
    }

    override suspend fun <E : DbEntity<E, ID>, ID : Any>
    find(table: DbTable<E, ID>, id: ID?): E? {
        if (id == null)
            return null

        val entities = masterIndex[table]
        val entityInfo = entities[id]

        when {
            entityInfo.loaded -> {
                log.trace("Id {} of table {} was already loaded", id, entities.metainfo.dbName)
                return entityInfo.value
            }

            entityInfo.loading -> {
                log.trace("Adding id {} of table {} to waiting list")
                return suspendCoroutine(entityInfo::addReceiver)
            }

            else -> {
                log.trace("Adding id {} of table {} to list for loading and initiating load", id, entities.metainfo.dbName)
                scheduleDelayedExec()
                entities.addIdToLoad(id)
                return suspendCoroutine(entityInfo::addReceiver)
            }
        }
    }

    override suspend fun <E : DbEntity<E, ID>, ID : Any>
    load(table: DbTable<E, ID>, id: ID): E {
        return find(table, id) ?: throw IllegalArgumentException("No ${table.dbName} with id $id")
    }

    override suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    loadForAll(ref: RelToOne<FROM, TO>, sources: Collection<FROM>?): Map<FROM, TO> {
        if (sources == null || sources.isEmpty())
            return emptyMap()

        @Suppress("UNCHECKED_CAST")
        val rel = ref as RelToOneImpl<FROM, FROMID, TO, TOID>

        val futures = LinkedHashMap<FROM, Deferred<TO>>()
        for (source in sources)
            futures.put(source, loadAsync(source, rel))

        val result = LinkedHashMap<FROM, TO>()
        for ((source, future) in futures)
            result[source] = future.await()

        return result
    }


    override suspend fun <E : DbEntity<E, ID>, ID: Any>
    count(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): Long {
        val sb = SqlBuilder()
        sb.sql("SELECT COUNT(*) FROM ").name(table)
        sb.sql(" WHERE ")
        filter.toSql(sb, true)

        return query(sb).results[0].getLong(0)
    }

    override suspend fun <E : DbEntity<E, ID>, ID: Any> query(table: DbTable<E, ID>, filter: Expr<in E, Boolean>): List<E> {
        val entities = masterIndex[table]

        val sb = SqlBuilder()
        sb.sql("SELECT ").sql(table.columnNames()).sql(" FROM ").name(table)
        if (filter != null) {
            sb.sql(" WHERE ")
            filter.toSql(sb, true)
        }

        val result = queryInternal(entities, sb)
        scheduleDelayedExec()
        return result
    }

    fun <E : DbEntity<E, ID>, ID> query(query: EntityQuery<E>, handler: Handler<AsyncResult<List<E>>>) {
        val queryImpl = query as EntityQueryImpl<E, ID>

        val table = queryImpl.getTable()
        val filter = queryImpl.getFilter()

        var maxRowCount = queryImpl.getMaxRowCount()
        var offset = queryImpl.getOffset()

        val orderBy = queryImpl.getOrderBy()


        val entities = masterIndex.get(table)

        val sb = SqlBuilder()
        sb.sql("SELECT ").sql(table.columnNames()).sql(" FROM ").sql(table.dbName)

        if (filter != null) {
            sb.sql(" WHERE ")
            filter!!.toSql(sb, true)
        }

        if (!orderBy.isEmpty()) {
            sb.sql(" ORDER BY ")
            var i = 0
            val n = orderBy.size
            while (i < n) {
                val o = orderBy.get(i)

                if (i > 0)
                    sb.sql(", ")
                o.getExpr().toSql(sb, true)
                if (!o.isAscending())
                    sb.sql(" DESC")
                i++
            }
        }

        if (offset != null || maxRowCount != null) {
            if (offset == null)
                offset = 0L
            if (maxRowCount == null)
                maxRowCount = Integer.MAX_VALUE

            sb.sql(" LIMIT ? OFFSET ?").param(maxRowCount!!).param(offset!!)
        }

        queryInternal<E, Any>(entities, sb, { result ->
            scheduleDelayedExec()
            handler.handle(result)
        })
    }

    fun <E : DbEntity<E, ID>, ID> queryCount(query: EntityQuery<E>, handler: Handler<AsyncResult<Long>>) {
        val queryImpl = query as EntityQueryImpl<E, ID>

        queryCount(queryImpl.getTable(), queryImpl.getFilter(), handler)
    }

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> load(from: FROM, relation: RelToOne<FROM, TO>, handler: Handler<AsyncResult<TO>>) {
        val rel = relation as RelToOneImpl<FROM, FROMID, TO, TOID>

        load(rel.targetTable, rel.mapId(from), handler)
    }

    fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> load(from: FROM, relation: RelToMany<FROM, TO>, handler: Handler<AsyncResult<List<TO>>>) {
        val relInfo = masterIndex.get(relation)
        val rel = relInfo.getMetainfo()
        val loadState = relInfo.getOrCreate(from.getID())

        if (loadState.addHandler(handler)) {
            log.debug("Adding {} with id {} to list for loading many {}",
                    from.metainfo.dbName, from.getID(), rel.getTargetTable().getDbName())

            relInfo.addIdToLoad(from.getID())
            scheduleDelayedExec()
        } else {
            log.debug("The {} with id {} already had many {} loaded",
                    from.metainfo.dbName, from.getID(), rel.getTargetTable().getDbName())
        }
    }

    override suspend fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    load(from: FROM, relation: RelToMany<FROM, TO>, filter: Expr<in TO, Boolean>?): List<TO> {
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
    queryInternal(entities: DelayedLoadIndex<E, ID, DbTable<E, ID>>, sb: SqlBuilder): List<E> {
        val table = entities.metainfo

        val res = ArrayList<E>()
        for (json in query(sb).results) {
            val row = json.list

            val id = table.createId(row)
            val info = entities[id]

            val entity: E
            if (info.state === EntityState.LOADED) {
                val existing = info.getExistingResult()
                if (existing.succeeded()) {
                    entity = existing.result()
                } else {
                    entity = table.create(id, row)
                    info.replaceResult(Future.succeededFuture<T>(entity))
                }
            } else {
                entity = table.create(id, row)
                info.handleResult(Future.succeededFuture<T>(entity))
            }

            res.add(entity)
        }
        return res
    }

    private fun goLoadDelayed() {
        for (index in masterIndex.allCachedTables)
            if (loadDelayedTable(index))
                return

        for (index in masterIndex.allCachedToManyRels)
            if (loadDelayedToManyRel(index))
                return
    }

    private fun <E : DbEntity<E, ID>, ID: Any>
    loadDelayedTable(index: DelayedLoadIndex<E, ID, DbTable<E, ID>>): Boolean {
        val ids: MutableSet<ID> = index.getAndClearIdsToLoad()
        if (ids.isEmpty())
            return false

        // TODO: split huge lists into chunks

        val table = index.metainfo
        val idField = table.idField

        log.debug("Querying {} for ids {}", table.dbName, ids)

        try {
            val result = query(table, idField.oneOf(ids))

            for (entity: E in result.result())
                ids.remove(entity.id)

            if (!ids.isEmpty())
                index.reportNull(ids)
        } finally {
            scheduleDelayedExec()
        }
        query(table, idField.oneOf(ids), { result ->
            try {
                if (result.failed()) {
                    log.error("Query of delayed IDs failed", result.cause())
                    val errorEvent = Future.failedFuture<E>(result.cause())
                    index.reportEvent(ids, errorEvent)
                } else {
                }
            } catch (t: Throwable) {
                fail(t)
            } finally {
                scheduleDelayedExec()
            }
        })
        return true
    }

    private fun <FROM : DbEntity<FROM, FROMID>, FROMID, TO : DbEntity<TO, TOID>, TOID> loadDelayedToManyRel(index: DelayedLoadIndex<List<TO>, FROMID, RelToManyImpl<FROM, FROMID, TO, TOID>>): Boolean {
        val fromIds = index.getAndClearIdsToLoad()
        if (fromIds.isEmpty())
            return false

        // TODO: split huge lists into chunks

        val rel = index.getMetainfo()

        val manyTable = rel.getTargetTable()
        val condition = rel.createCondition(fromIds)

        query<TO, TOID>(manyTable, condition, { result ->
            try {
                if (result.failed()) {
                    index.reportEvent(fromIds, result)
                    return@query
                }

                val mapped = LinkedHashMap<FROMID, List<TO>>()
                for (fromId in fromIds)
                    mapped.put(fromId, ArrayList())

                for (to in result.result()) {
                    val fromId = rel.reverseMap(to)
                    mapped[fromId].add(to)
                }

                for ((key, value) in mapped) {
                    index.getOrCreate(key).handleResult(Future.succeededFuture<T>(value))
                }
            } catch (t: Throwable) {
                fail(t)
            } finally {
                scheduleDelayedExec()
            }
        })

        return true
    }

    fun rollback(callback: Handler<AsyncResult<Void>>) {
        conn.rollback { result ->
            if (result.succeeded()) {
                masterIndex.flushAll()
            }
        }
    }

    fun commit(handler: Handler<AsyncResult<Void>>) {
        conn.commit(handler)
    }

    fun setAutoCommit(autoCommit: Boolean, handler: Handler<AsyncResult<Void>>) {
        conn.setAutoCommit(autoCommit, handler)
    }

    fun setTransactionIsolation(isolation: TransactionIsolation, handler: Handler<AsyncResult<Void>>) {
        conn.setTransactionIsolation(isolation, handler)
    }

    fun completer(): Handler<AsyncResult<T>> {
        return Handler { this.finish(it) }
    }

    fun <E : DbEntity<E, ID>, ID> loadMany(table: DbTable<E, ID>, ids: Iterable<ID>, handler: Handler<AsyncResult<Map<ID, E>>>) {
        val idsSet: MutableSet<ID>
        if (ids is Set<*>) {
            idsSet = ids as Set<ID>
        } else if (ids is Collection<*>) {
            idsSet = LinkedHashSet(ids as Collection<ID>)
        } else {
            idsSet = LinkedHashSet()
            for (id in ids)
                idsSet.add(id)
        }

        if (idsSet.isEmpty()) {
            handler.handle(Future.succeededFuture(emptyMap()))
            return
        }

        val result = LinkedHashMap<ID, E>(idsSet.size)
        val futures = ArrayList<Future<*>>(idsSet.size)

        for (id in idsSet) {
            result.put(id, null)
            futures.add(load(table, id))
        }

        CompositeFuture.all(futures).setHandler { combinedResult ->
            try {
                if (forwardFailure(combinedResult, handler))
                    return@CompositeFuture.all(futures).setHandler

                for (futureG in futures) {
                    val future = futureG as Future<E>

                    val entity = future.result()
                    if (entity != null) {
                        val id = entity.getID()
                        result.put(id, entity)
                    }
                }
            } catch (e: Exception) {
                handler.handle(Future.failedFuture(e))
                return@CompositeFuture.all(futures).setHandler
            }

            handler.handle(Future.succeededFuture(result))
        }
    }

    fun <E : DbEntity<E, ID>, ID> delete(entity: E, handler: Handler<AsyncResult<Long>>) {
        val table = entity.metainfo
        val filter = entity.metainfo.getIdField().equalTo(entity.getID())

        executeUpdateLikeQuery(createDeleteQuery(table, filter), table, handler)
    }

    fun <E : DbEntity<E, ID>, ID> delete(table: DbTable<E, ID>, filter: Expr<in E, Boolean>, handler: Handler<AsyncResult<Long>>) {
        executeUpdateLikeQuery(createDeleteQuery(table, filter), table, handler)
    }

    fun <E : DbEntity<E, ID>, ID> delete(table: DbTable<E, ID>, ids: Set<ID>, handler: Handler<AsyncResult<Long>>) {
        if (ids.isEmpty()) {
            handler.handle(Future.succeededFuture(0L))
            return
        }

        executeUpdateLikeQuery(createDeleteQuery(table, table.getIdField().oneOf(ids)), table, handler)
    }

    fun <E : DbEntity<E, ID>, ID> delete(table: DbTable<E, ID>, id: ID?, handler: Handler<AsyncResult<Long>>) {
        if (id == null) {
            handler.handle(Future.succeededFuture(0L))
            return
        }

        executeUpdateLikeQuery(createDeleteQuery(table, table.getIdField().equalTo(id)), table, handler)
    }

    private fun <E : DbEntity<E, ID>, ID> executeUpdateLikeQuery(sqlBuilder: SqlBuilder, table: DbTable<E, ID>, handler: Handler<AsyncResult<Long>>) {
        val sql = sqlBuilder.getSql()
        val params = sqlBuilder.params

        log.debug("Executing update:\n{}\nparams: {}", sql, params)

        val setHandler = wrapQueryHandler<UpdateResult>({ result ->
            if (result.succeeded()) {
                val updateResult = result.result()

                try {
                    // TODO: look at specific id, as to not destroy so much cache
                    masterIndex.flushRelated(table)
                } finally {
                    handler.handle(Future.succeededFuture(updateResult.getUpdated().toLong()))
                }
            } else {
                handler.handle(Future.failedFuture<Long>(result.cause()))
            }
        })

        conn.updateWithParams(sql, params, setHandler)
    }

    fun execute(sql: String, handler: Handler<AsyncResult<Void>>) {
        log.debug("Executing {}", sql)
        conn.execute(sql, handler)
    }

    private fun <E : DbEntity<E, ID>, ID> createDeleteQuery(table: DbTable<E, ID>, filter: Expr<in E, Boolean>?): SqlBuilder {
        val sb = SqlBuilder()
        sb.sql("DELETE FROM ").name(table)

        if (filter != null) {
            sb.sql(" WHERE ")
            filter.toSql(sb, true)
        }

        return sb
    }

    companion object {
        private val log = LoggerFactory.getLogger(DbLoaderImpl<*, *>::class.java)
    }
}
