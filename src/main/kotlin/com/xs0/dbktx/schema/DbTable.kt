package com.xs0.dbktx.schema

import com.xs0.dbktx.composite.CompositeId
import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.crud.*
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.util.Sql
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

    internal lateinit var factory: (DbConn, ID, List<Any?>) -> E
    internal lateinit var idField: NonNullRowProp<E, ID>

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
        if (idField == null)
            throw IllegalStateException("Missing idField for table " + dbName)

        // TODO
        return this
    }

    fun createId(row: List<Any?>): ID {
        return idField(row)
    }

    fun create(db: DbConn, id: ID, row: List<Any?>): E {
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
            result.put(column.fieldName, column.sqlType.toJson(value))
        }
    }

    internal fun importFromJson(jsonObject: JsonObject): List<Any?> {
        val n = columns.size
        val values = arrayOfNulls<Any>(n)
        for (i in 0 until n) {
            val column = columns[i]
            val jsonValue = jsonObject.getValue(column.fieldName)
            if (jsonValue != null) {
                values[column.indexInRow] = column.sqlType.fromJson(jsonValue)
            }
        }

        return listOf(*values)
    }

    fun insertion(db: DbConn): DbInsert<E, ID> {
        return DbInsertImpl(db, this)
    }

    fun updateAll(db: DbConn): DbUpdate<E> {
        return DbUpdateImpl(db, BaseTableInUpdateQuery(this, null, null, null)
    }

    fun update(db: DbConn, filter: ExprBoolean<E>): DbUpdate<E> {
        return DbUpdateImpl(db, this, filter, null, null)
    }

    fun update(db: DbConn, entity: E): DbUpdate<E> {
        return DbUpdateImpl(db, this, idField eq entity.id, setOf(entity.id), entity)
    }

    fun updateById(db: DbConn, id: ID): DbUpdate<E> {
        return DbUpdateImpl(db, this, idField eq id, setOf(id), null)
    }

    fun update(db: DbConn, vararg entities: E): DbUpdate<E> {
        return update(db, listOf(*entities))
    }

    fun updateByIds(db: DbConn, vararg ids: ID): DbUpdate<E> {
        val idsSet = setOf(*ids)
        val filter = idField oneOf idsSet
        return DbUpdateImpl(db, this, filter, idsSet, null)
    }

    fun update(db: DbConn, entities: Collection<E>): DbUpdate<E> {
        if (entities.size == 1)
            return update(db, entities.first())

        val idsSet = HashSet<ID>()
        for (entity in entities)
            idsSet.add(entity.id)

        val filter = idField oneOf idsSet
        return DbUpdateImpl(db, this, filter, idsSet, null)
    }

    fun updateByIds(db: DbConn, ids: Collection<ID>): DbUpdate<E> {
        val idsSet = HashSet(ids)
        val filter = idField oneOf idsSet
        return DbUpdateImpl(db, this, filter, idsSet, null)
    }
}

abstract class DbTableC<E : DbEntity<E, ID>, ID : CompositeId<E, ID>>(
        schema: DbSchema,
        dbName: String,
        entityClass: KClass<E>,
        idClass: KClass<ID>
)
    : DbTable<E, ID>(schema, dbName, entityClass, idClass) {

    override protected val b: DbTableBuilderC<E, ID> = DbTableBuilderC(this)
}