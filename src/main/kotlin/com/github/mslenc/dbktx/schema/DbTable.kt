package com.github.mslenc.dbktx.schema

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.aggr.*
import com.github.mslenc.dbktx.composite.CompositeId
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.DbLoaderInternal
import com.github.mslenc.dbktx.crud.*
import com.github.mslenc.dbktx.util.EntityIndex
import com.github.mslenc.dbktx.util.FakeRowData
import com.github.mslenc.dbktx.util.Sql
import com.github.mslenc.dbktx.util.getContextDb
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

    override fun equals(other: Any?): Boolean {
        return this === other
    }

    fun validate(): DbTable<E, ID> {
        if (!::factory.isInitialized)
            throw IllegalStateException("Missing constructor for table $dbName")
        if (!::primaryKey.isInitialized)
            throw IllegalStateException("Missing primaryKey for table $dbName")

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

    fun newEntityQuery(db: DbConn = getContextDb()): EntityQuery<E> {
        return db.newEntityQuery(this)
    }

    fun newInsertQuery(db: DbConn = getContextDb()): DbInsert<E, ID> {
        return db.newInsertQuery(this)
    }

    fun newUpdateQuery(db: DbConn = getContextDb()): DbUpdate<E> {
        return db.newUpdateQuery(this)
    }

    fun newDelete(db: DbConn = getContextDb()): DeleteQuery<E> {
        return db.newDeleteQuery(this)
    }

    fun toJsonObject(entity: E): Map<String, Any?> {
        val result = LinkedHashMap<String, Any?>()

        for (column in columns) {
            toJson(entity, column, result)
        }

        return result
    }

    private fun <T: Any> toJson(entity: E, column: Column<E, T>, result: MutableMap<String, Any?>) {
        val value = column(entity)

        if (value == null) {
            result[column.fieldName] = null
        } else {
            result[column.fieldName] = column.sqlType.encodeForJson(value)
        }
    }

    internal fun importFromJson(jsonObject: Map<String, Any?>): DbRow {
        val result = FakeRowData()

        for (column in columns) {
            val value = jsonObject[column.fieldName]

            if (value != null) {
                result.insertJsonValue(column, value)
            }
        }

        return result
    }


    internal fun callInsertAndResolveEntityInIndex(entityIndex: EntityIndex<E>, db: DbConn, row: DbRow, selectForUpdate: Boolean): E {
        return entityIndex.insertAndResolveEntityInIndex(db, this, row, selectForUpdate)
    }

    internal suspend fun callEnqueueDeleteQuery(db: DbLoaderInternal, sql: Sql): Long {
        return db.enqueueDeleteQuery(this, sql)
    }

    fun makeAggregateStreamQuery(db: DbConn, builder: AggrStreamTopLevelBuilder<E>.()->Unit): AggrStreamQuery<E> {
        val query = AggrStreamImpl(this, db)
        query.expand(builder)
        return query
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