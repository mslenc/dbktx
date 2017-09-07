package com.xs0.dbktx

import io.vertx.core.json.JsonObject
import kotlin.reflect.KClass

open class DbTable<E : DbEntity<E, ID>, ID : Any> protected constructor(
        val schema: DbSchema,
        val dbName: String,
        val entityClass: KClass<E>,
        internal val idClass: KClass<ID>) {

    internal val columns = ArrayList<Column<E, *>>()
    internal val columnsByDbName = HashMap<String, Column<E, *>>()

    lateinit internal var constructor: (ID, List<Any?>) -> E
    lateinit internal var idField: NonNullRowProp<E, ID>

    lateinit internal var columnNames: String
    internal var keyIsAutogenerated: Boolean = false

    protected val b: DbTableBuilder<E, ID> = DbTableBuilder(this)

    init {
        schema.register(this)
    }

    override fun hashCode(): Int {
        return dbName.hashCode()
    }

    fun validate(): DbTable<E, ID> {
        if (constructor == null)
            throw IllegalStateException("Missing constructor for table " + dbName)
        if (idField == null)
            throw IllegalStateException("Missing idField for table " + dbName)

        // TODO
        return this
    }

    fun createId(row: List<Any?>): ID {
        return idField(row)
    }

    fun create(id: ID, row: List<Any?>): E {
        return constructor(id, row)
    }

    fun columnNames(): String {
        return columnNames
    }

    val numColumns: Int
        get() = columns.size

    fun newQuery(dbLoader: DbConn): EntityQuery<E> {
        return EntityQueryImpl(this, dbLoader)
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

    fun fromJson(jsonObject: JsonObject): E {
        val n = columns.size
        val values = arrayOfNulls<Any>(n)
        for (i in 0 until n) {
            val column = columns[i]
            val jsonValue = jsonObject.getValue(column.fieldName)
            if (jsonValue != null) {
                values[column.indexInRow] = column.sqlType.fromJson(jsonValue)
            }
        }

        val row = listOf(*values)
        return create(createId(row), row)
    }

    fun insertion(db: DbConn): DbInsert<E, ID> {
        return DbInsertImpl(db, this)
    }

    fun updateAll(db: DbConn): DbUpdate<E> {
        return DbUpdateImpl(db, this, null, null)
    }

    fun update(db: DbConn, filter: ExprBoolean<E>): DbUpdate<E> {
        return DbUpdateImpl(db, this, filter, null)
    }

    fun update(db: DbConn, entity: E): DbUpdate<E> {
        return updateById(db, entity.id)
    }

    fun updateById(db: DbConn, id: ID): DbUpdate<E> {
        return DbUpdateImpl(db, this, idField eq id, setOf(id))
    }

    fun update(db: DbConn, vararg entities: E): DbUpdate<E> {
        return update(db, listOf(*entities))
    }

    fun updateByIds(db: DbConn, vararg ids: ID): DbUpdate<E> {
        val idsSet = setOf(*ids)
        val filter = idField oneOf idsSet
        return DbUpdateImpl(db, this, filter, idsSet)
    }

    fun update(db: DbConn, entities: Collection<E>): DbUpdate<E> {
        val idsSet = HashSet<ID>()
        for (entity in entities)
            idsSet.add(entity.id)

        val filter = idField oneOf idsSet
        return DbUpdateImpl(db, this, filter, idsSet)
    }

    fun updateByIds(db: DbConn, ids: Collection<ID>): DbUpdate<E> {
        val idsSet = HashSet(ids)
        val filter = idField oneOf idsSet
        return DbUpdateImpl(db, this, filter, idsSet)
    }
}