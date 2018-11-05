package com.github.mslenc.dbktx.schema

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.aggr.AggregateBuilder
import com.github.mslenc.dbktx.composite.CompositeId
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.DbLoaderInternal
import com.github.mslenc.dbktx.crud.*
import com.github.mslenc.dbktx.expr.ExprBoolean
import com.github.mslenc.dbktx.util.EntityIndex
import com.github.mslenc.dbktx.util.FakeRowData
import com.github.mslenc.dbktx.util.Sql
import io.vertx.core.json.JsonObject
import kotlin.reflect.KClass

private fun createPrefixForTableName(tableName: String): String {
    return tableName.split('_')
                    .map { it.substring(0, 1) }
                    .map { it.toUpperCase() }
                    .joinToString("")
}

open class DbTable<E : DbEntity<E, ID>, ID : Any> protected constructor(
        val schema: DbSchema,
        val dbName: String,
        val entityClass: KClass<E>,
        internal val idClass: KClass<ID>) {

    val quotedDbName: String = Sql.quoteIdentifier(dbName)
    val aliasPrefix: String

    internal val columns = ArrayList<Column<E, *>>()
    internal val columnsByDbName = HashMap<String, Column<E, *>>()

    internal lateinit var factory: (DbConn, ID, DbRow) -> E
    lateinit var primaryKey: UniqueKeyDef<E, ID>
        internal set

    internal val uniqueKeys = ArrayList<UniqueKeyDef<E, *>>()

    internal lateinit var defaultColumnNames: String
    internal var keyIsAutogenerated: Boolean = false

    protected open val b: DbTableBuilder<E, ID> = DbTableBuilder(this)

    init {
        aliasPrefix = createPrefixForTableName(dbName)
        schema.register(this)
    }

    override fun hashCode(): Int {
        return dbName.hashCode()
    }

    fun validate(): DbTable<E, ID> {
        if (factory == null)
            throw IllegalStateException("Missing constructor for table " + dbName)
        if (primaryKey == null)
            throw IllegalStateException("Missing primaryKey for table " + dbName)

        // TODO
        return this
    }

    fun createId(row: DbRow): ID {
        return primaryKey(row)
    }

    fun create(db: DbConn, id: ID, row: DbRow): E {
        return factory(db, id, row)
    }

    val numColumns: Int
        get() = columns.size

    fun newQuery(dbLoader: DbConn): EntityQuery<E> {
        return EntityQueryImpl(this, dbLoader)
    }

    fun newDeleteQuery(dbLoader: DbConn): DeleteQuery<E> {
        return DeleteQueryImpl(this, dbLoader)
    }

    fun toJsonObject(entity: E): JsonObject {
        val result = JsonObject()

        for (column in columns) {
            toJson(entity, column, result)
        }

        return result
    }

    private fun <T: Any> toJson(entity: E, column: Column<E, T>, result: JsonObject) {
        val value = column(entity)

        if (value == null) {
            result.putNull(column.fieldName)
        } else {
            result.put(column.fieldName, column.sqlType.encodeForJson(value))
        }
    }

    internal fun importFromJson(jsonObject: JsonObject): DbRow {
        val result = FakeRowData()

        for (column in columns) {
            val value = jsonObject.getValue(column.fieldName)

            if (value != null) {
                result.insertJsonValue(column, value)
            }
        }

        return result
    }

    fun insertion(db: DbConn): DbInsert<E, ID> {
        val query = InsertQueryImpl()
        val boundTable = BaseTableInUpdateQuery(query, this)
        return DbInsertImpl(db, boundTable)
    }

    fun updateAll(db: DbConn): DbUpdate<E> {
        return DbUpdateImpl(db, this, null, null)
    }

    fun updateMany(db: DbConn, filter: FilterBuilder<E>.()->ExprBoolean): DbUpdate<E> {
        val update = DbUpdateImpl(db, this, null, null)
        update.filter(filter)
        return update
    }

    fun update(db: DbConn, entity: E): DbUpdate<E> {
        val update = DbUpdateImpl(db, this, setOf(entity.id), entity)
        update.filter { primaryKey eq entity.id }
        return update
    }

    fun updateById(db: DbConn, id: ID): DbUpdate<E> {
        val update = DbUpdateImpl(db, this, setOf(id), null)
        update.filter { primaryKey eq id }
        return update
    }

//    fun update(db: DbConn, vararg entities: E): DbUpdate<E> {
//        return update(db, listOf(*entities))
//    }

    fun updateByIds(db: DbConn, vararg ids: ID): DbUpdate<E> {
        val idsSet = setOf(*ids)
        val update = DbUpdateImpl(db, this, idsSet, null)
        update.filter { primaryKey oneOf idsSet }
        return update
    }

    fun update(db: DbConn, entities: Collection<E>): DbUpdate<E> {
        if (entities.size == 1)
            return update(db, entities.first())

        val idsSet = HashSet<ID>()
        for (entity in entities)
            idsSet.add(entity.id)

        val update = DbUpdateImpl(db, this, idsSet, null)
        update.filter { primaryKey oneOf idsSet }
        return update
    }

    fun updateByIds(db: DbConn, ids: Collection<ID>): DbUpdate<E> {
        val idsSet = HashSet(ids)
        val update = DbUpdateImpl(db, this, idsSet, null)
        update.filter { primaryKey oneOf idsSet }
        return update
    }

    internal fun callInsertAndResolveEntityInIndex(entityIndex: EntityIndex<E>, db: DbConn, row: DbRow): E {
        return entityIndex.insertAndResolveEntityInIndex(db, this, row)
    }

    internal suspend fun callEnqueueDeleteQuery(db: DbLoaderInternal, sql: Sql, specificIds: Set<ID>?): Long {
        return db.enqueueDeleteQuery(this, sql, specificIds)
    }

    fun aggregateQuery(db: DbConn, builder: AggregateBuilder<E>.()->Unit): AggregateQuery<E> {

    }
}

abstract class DbTableC<E : DbEntity<E, ID>, ID : CompositeId<E, ID>>(
        schema: DbSchema,
        dbName: String,
        entityClass: KClass<E>,
        idClass: KClass<ID>
)
    : DbTable<E, ID>(schema, dbName, entityClass, idClass) {

    override val b: DbTableBuilderC<E, ID> = DbTableBuilderC(this)
}