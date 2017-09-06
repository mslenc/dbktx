package com.xs0.dbktx

import com.xs0.dbktx.sqltypes.SqlType
import com.xs0.dbktx.sqltypes.SqlTypes
import org.jetbrains.annotations.Nullable
import java.lang.reflect.ParameterizedType
import java.math.BigDecimal
import java.time.*
import java.util.*
import java.util.function.*
import kotlin.reflect.KClass
import kotlin.reflect.KClassifier
import kotlin.reflect.KType
import kotlin.reflect.full.isSubclassOf
import com.xs0.dbktx.FieldProp.Companion.VARCHAR
import com.xs0.dbktx.composite.CompositeId2
import com.xs0.dbktx.sqltypes.SqlTypeKind

class DbTableBuilder<E : DbEntity<E, ID>, ID : Any>
internal constructor(
        private val table: DbTable<E, ID>) {

    private val foreignKeys = HashMap<String, ForeignKey<*,*>>()
    private var idFieldInitialized: Boolean = false

    fun build(constructor: (ID, List<Any?>) -> E): DbTable<E, ID> {
        table.constructor = constructor

        val columnNames = StringBuilder()
        var i = 0
        val n = table.columns.size
        while (i < n) {
            if (i > 0)
                columnNames.append(", ")
            columnNames.append(table.columns[i].fieldName)
            i++
        }
        table.columnNames = columnNames.toString()

        return table.validate()
    }

    fun nonNullString(fieldName: String, type: SqlTypeDef, getter: (E) -> String,
                      primaryKey: Boolean = false,
                      references: ForeignKey<*, String>? = null): NonNullStringColumn<E> {

        val sqlType = SqlTypes.makeString(type.sqlTypeKind, type.param1, true)
        val column = NonNullStringColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey, isAutoIncrement = false)
        return column
    }

    fun nullableString(fieldName: String, type: SqlTypeDef, getter: (E) -> String?,
                       references: ForeignKey<*, String>? = null): NullableStringColumn<E> {

        val sqlType = SqlTypes.makeString(type.sqlTypeKind, type.param1, false)
        val column = NullableStringColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = false, isAutoIncrement = false)
        return column
    }



    fun nonNullInt(fieldName: String, type: SqlTypeDef, getter: (E) -> Int,
                   primaryKey: Boolean? = null,
                   autoIncrement: Boolean = false,
                   unsigned: Boolean = false,
                   references: ForeignKey<*, Int>? = null): NonNullOrderedColumn<E, Int> {

        val sqlType = SqlTypes.makeInteger(type.sqlTypeKind, isNotNull = true, isAutoGenerated = autoIncrement, isUnsigned = unsigned)
        val column = NonNullOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey ?: autoIncrement, isAutoIncrement = autoIncrement)
        return column
    }

    fun nullableInt(fieldName: String, type: SqlTypeDef, getter: (E) -> Int?,
                    unsigned: Boolean = false,
                    references: ForeignKey<*, Int>? = null): NullableOrderedColumn<E, Int> {

        val sqlType = SqlTypes.makeInteger(type.sqlTypeKind, isNotNull = false, isAutoGenerated = false, isUnsigned = unsigned)
        val column = NullableOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references)
        return column
    }



    fun nonNullLong(fieldName: String, type: SqlTypeDef, getter: (E) -> Long,
                    primaryKey: Boolean? = null,
                    autoIncrement: Boolean = false,
                    unsigned: Boolean = false,
                    references: ForeignKey<*, Long>? = null): NonNullOrderedColumn<E, Long> {

        val sqlType = SqlTypes.makeLong(type.sqlTypeKind, isNotNull = true, isAutoGenerated = autoIncrement, isUnsigned = unsigned)
        val column = NonNullOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey ?: autoIncrement, isAutoIncrement = autoIncrement)
        return column
    }

    fun nullableLong(fieldName: String, type: SqlTypeDef, getter: (E) -> Long,
                     unsigned: Boolean = false,
                     references: ForeignKey<*, Long>? = null): NullableOrderedColumn<E, Long> {

        val sqlType = SqlTypes.makeLong(type.sqlTypeKind, isNotNull = false, isAutoGenerated = false, isUnsigned = unsigned)
        val column = NullableOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = false, isAutoIncrement = false)
        return column
    }



    fun nonNullFloat(fieldName: String, type: SqlTypeDef, getter: (E) -> Float,
                     primaryKey: Boolean = false,
                     unsigned: Boolean = false,
                     references: ForeignKey<*, Float>? = null): NonNullOrderedColumn<E, Float> {

        val sqlType = SqlTypes.makeFloat(type.sqlTypeKind, isNotNull = true, isUnsigned = unsigned)
        val column = NonNullOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableFloat(fieldName: String, type: SqlTypeDef, getter: (E) -> Float?,
                      unsigned: Boolean = false,
                      references: ForeignKey<*, Float>? = null): NullableOrderedColumn<E, Float> {

        val sqlType = SqlTypes.makeFloat(type.sqlTypeKind, isNotNull = false, isUnsigned = unsigned)
        val column = NullableOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references)
        return column
    }



    fun nonNullDouble(fieldName: String, type: SqlTypeDef, getter: (E) -> Double,
                      primaryKey: Boolean = false,
                      unsigned: Boolean = false,
                      references: ForeignKey<*, Double>? = null): NonNullOrderedColumn<E, Double> {

        val sqlType = SqlTypes.makeDouble(type.sqlTypeKind, isNotNull = true, isUnsigned = unsigned)
        val column = NonNullOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableDouble(fieldName: String, type: SqlTypeDef, getter: (E) -> Double?,
                       unsigned: Boolean = false,
                       references: ForeignKey<*, Double>? = null): NullableOrderedColumn<E, Double> {

        val sqlType = SqlTypes.makeDouble(type.sqlTypeKind, isNotNull = false, isUnsigned = unsigned)
        val column = NullableOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references)
        return column
    }


    fun nonNullUUID(fieldName: String, type: SqlTypeDef, getter: (E) -> UUID,
                    primaryKey: Boolean = false,
                    references: ForeignKey<*, UUID>? = null): NonNullMultiValuedColumn<E, UUID> {

        val size = type.param1 ?: throw IllegalArgumentException("Missing size for UUID type")
        val sqlType = SqlTypes.makeUUID(type.sqlTypeKind, size = size, isNotNull = true)
        val column = NonNullMultiValuedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableUUID(fieldName: String, type: SqlTypeDef, getter: (E) -> UUID?,
                     references: ForeignKey<*, UUID>? = null): NullableMultiValuedColumn<E, UUID> {

        val size = type.param1 ?: throw IllegalArgumentException("Missing size for UUID type")
        val sqlType = SqlTypes.makeUUID(type.sqlTypeKind, size = size, isNotNull = false)
        val column = NullableMultiValuedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references)
        return column
    }



    fun nonNullYear(fieldName: String, type: SqlTypeDef, getter: (E) -> Year,
                    primaryKey: Boolean = false,
                    references: ForeignKey<*, Year>? = null): NonNullOrderedColumn<E, Year> {

        val sqlType = SqlTypes.makeYear(type.sqlTypeKind, isNotNull = true)
        val column = NonNullOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableYear(fieldName: String, type: SqlTypeDef, getter: (E) -> Year?,
                     references: ForeignKey<*, Year>? = null): NullableOrderedColumn<E, Year> {

        val sqlType = SqlTypes.makeYear(type.sqlTypeKind, isNotNull = false)
        val column = NullableOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references)
        return column
    }



    fun nonNullDateTime(fieldName: String, type: SqlTypeDef, getter: (E) -> LocalDateTime,
                        primaryKey: Boolean = false,
                        references: ForeignKey<*, LocalDateTime>? = null): NonNullOrderedColumn<E, LocalDateTime> {

        val sqlType = SqlTypes.makeLocalDateTime(type.sqlTypeKind, isNotNull = true)
        val column = NonNullOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableDateTime(fieldName: String, type: SqlTypeDef, getter: (E) -> LocalDateTime?,
                         references: ForeignKey<*, LocalDateTime>? = null): NullableOrderedColumn<E, LocalDateTime> {

        val sqlType = SqlTypes.makeLocalDateTime(type.sqlTypeKind, isNotNull = false)
        val column = NullableOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references)
        return column
    }


    fun nonNullInstant(fieldName: String, type: SqlTypeDef, getter: (E) -> Instant,
                       primaryKey: Boolean = false,
                       references: ForeignKey<*, Instant>? = null): NonNullOrderedColumn<E, Instant> {

        val sqlType = SqlTypes.makeInstant(type.sqlTypeKind, isNotNull = true)
        val column = NonNullOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableInstant(fieldName: String, type: SqlTypeDef, getter: (E) -> Instant?,
                        references: ForeignKey<*, Instant>? = null): NullableOrderedColumn<E, Instant> {

        val sqlType = SqlTypes.makeInstant(type.sqlTypeKind, isNotNull = false)
        val column = NullableOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references)
        return column
    }


    fun nonNullTime(fieldName: String, type: SqlTypeDef, getter: (E) -> LocalTime,
                    primaryKey: Boolean = false,
                    references: ForeignKey<*, LocalTime>? = null): NonNullOrderedColumn<E, LocalTime> {

        val sqlType = SqlTypes.makeLocalTime(type.sqlTypeKind, isNotNull = true)
        val column = NonNullOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableTime(fieldName: String, type: SqlTypeDef, getter: (E) -> LocalTime?,
                     references: ForeignKey<*, LocalTime>? = null): NullableOrderedColumn<E, LocalTime> {

        val sqlType = SqlTypes.makeLocalTime(type.sqlTypeKind, isNotNull = false)
        val column = NullableOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references)
        return column
    }


    fun nonNullDate(fieldName: String, type: SqlTypeDef, getter: (E) -> LocalDate,
                    primaryKey: Boolean = false,
                    references: ForeignKey<*, LocalDate>? = null): NonNullOrderedColumn<E, LocalDate> {

        val sqlType = SqlTypes.makeLocalDate(type.sqlTypeKind, isNotNull = true)
        val column = NonNullOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableDate(fieldName: String, type: SqlTypeDef, getter: (E) -> LocalDate?,
                     references: ForeignKey<*, LocalDate>? = null): NullableOrderedColumn<E, LocalDate> {

        val sqlType = SqlTypes.makeLocalDate(type.sqlTypeKind, isNotNull = false)
        val column = NullableOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references)
        return column
    }


    fun nonNullBytes(fieldName: String, type: SqlTypeDef, getter: (E) -> ByteArray)
            : NonNullSimpleColumn<E, ByteArray> {

        val sqlType = SqlTypes.makeByteArray(type.sqlTypeKind, type.param1, isNotNull = true)
        val column = NonNullSimpleColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, foreignKey = null)
        return column
    }

    fun nullableBytes(fieldName: String, type: SqlTypeDef, getter: (E) -> ByteArray?)
            : NullableSimpleColumn<E, ByteArray> {

        val sqlType = SqlTypes.makeByteArray(type.sqlTypeKind, type.param1, isNotNull = false)
        val column = NullableSimpleColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, foreignKey = null)
        return column
    }


    fun nonNullDecimal(fieldName: String, type: SqlTypeDef, getter: (E) -> BigDecimal,
                       primaryKey: Boolean = false,
                       unsigned: Boolean = false,
                       references: ForeignKey<*, BigDecimal>? = null): NonNullOrderedColumn<E, BigDecimal> {

        val precision: Int = type.param1 ?: throw IllegalArgumentException("Missing precision for DECIMAL")
        val scale: Int = type.param2 ?: throw IllegalArgumentException("Missing scale for DECIMAL")

        val sqlType = SqlTypes.makeBigDecimal(type.sqlTypeKind, precision, scale, isNotNull = true, isUnsigned = unsigned)
        val column = NonNullOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableDecimal(fieldName: String, type: SqlTypeDef, getter: (E) -> BigDecimal?,
                        primaryKey: Boolean = false,
                        unsigned: Boolean = false,
                        references: ForeignKey<*, BigDecimal>? = null): NullableOrderedColumn<E, BigDecimal> {

        val precision: Int = type.param1 ?: throw IllegalArgumentException("Missing precision for DECIMAL")
        val scale: Int = type.param2 ?: throw IllegalArgumentException("Missing scale for DECIMAL")

        val sqlType = SqlTypes.makeBigDecimal(type.sqlTypeKind, precision, scale, isNotNull = false, isUnsigned = unsigned)
        val column = NullableOrderedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey)
        return column
    }


    fun nonNullStringEnum(fieldName: String,
                          type: SqlTypeDef = SqlTypeDef(SqlTypeKind.ENUM),
                          values: Set<String>,
                          getter: (E) -> String,
                          primaryKey: Boolean = false,
                          references: ForeignKey<*, String>? = null): NonNullMultiValuedColumn<E, String> {

        val sqlType = SqlTypes.makeEnumString(type.sqlTypeKind, values, isNotNull = true)
        val column = NonNullMultiValuedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey)
        return column
    }

    fun nonNullStringEnum(fieldName: String,
                          type: SqlTypeDef = SqlTypeDef(SqlTypeKind.ENUM),
                          values: Set<String>,
                          getter: (E) -> String?,
                          references: ForeignKey<*, String>? = null): NullableMultiValuedColumn<E, String> {

        val sqlType = SqlTypes.makeEnumString(type.sqlTypeKind, values, isNotNull = false)
        val column = NullableMultiValuedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references)
        return column
    }


    inline fun <reified ENUM : Enum<ENUM>>
            nonNullIntEnum(fieldName: String,
                           noinline getter: (E) -> ENUM,
                           noinline toDbRep: (ENUM)->Int,
                           noinline fromDbRep: (Int)->ENUM,
                           typeDef: SqlTypeDef,
                           references: ForeignKey<*, ENUM>? = null,
                           primaryKey: Boolean = false
                           ): NonNullMultiValuedColumn<E, ENUM> {

        val klass = ENUM::class
        val dummyValue = enumValues<ENUM>()[0]

        return nonNullIntEnum(fieldName, getter, toDbRep, fromDbRep, typeDef, klass, dummyValue, references, primaryKey = primaryKey)
    }

    fun <ENUM : Enum<ENUM>>
            nonNullIntEnum(fieldName: String,
                           getter: (E) -> ENUM,
                           toDbRep: (ENUM)->Int,
                           fromDbRep: (Int)->ENUM,
                           typeDef: SqlTypeDef,
                           klass: KClass<ENUM>,
                           dummyValue: ENUM,
                           references: ForeignKey<*, ENUM>? = null,
                           primaryKey: Boolean = false
                           ): NonNullMultiValuedColumn<E, ENUM> {

        val sqlType = SqlTypes.makeEnumToInt(klass, dummyValue, typeDef.sqlTypeKind, toDbRep, fromDbRep, isNotNull = true)
        val column = NonNullMultiValuedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references, isPrimaryKey = primaryKey)
        return column
    }

    inline fun <reified ENUM : Enum<ENUM>>
            nullableIntEnum(fieldName: String,
                            noinline getter: (E) -> ENUM?,
                            noinline toDbRep: (ENUM)->Int,
                            noinline fromDbRep: (Int)->ENUM,
                            typeDef: SqlTypeDef,
                            references: ForeignKey<*, ENUM>? = null,
                            primaryKey: Boolean = false
                            ): NullableMultiValuedColumn<E, ENUM> {

        val klass = ENUM::class
        val dummyValue = enumValues<ENUM>()[0]

        return nullableIntEnum(fieldName, getter, toDbRep, fromDbRep, typeDef, klass, dummyValue, references)
    }

    fun <ENUM : Enum<ENUM>>
            nullableIntEnum(fieldName: String,
                            getter: (E) -> ENUM?,
                            toDbRep: (ENUM)->Int,
                            fromDbRep: (Int)->ENUM,
                            typeDef: SqlTypeDef,
                            klass: KClass<ENUM>,
                            dummyValue: ENUM,
                            references: ForeignKey<*, ENUM>? = null
                            ): NullableMultiValuedColumn<E, ENUM> {

        val sqlType = SqlTypes.makeEnumToInt(klass, dummyValue, typeDef.sqlTypeKind, toDbRep, fromDbRep, isNotNull = true)
        val column = NullableMultiValuedColumn(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, references)
        return column
    }



    private fun <T : Any> finishAddColumn(column: Column<E, T>, foreignKey: ForeignKey<*,T>?, isPrimaryKey: Boolean = false, isAutoIncrement: Boolean = false) {
        if (isAutoIncrement) {
            if (!isPrimaryKey)
                throw IllegalArgumentException("To be auto_increment, the column must be a primary key")
            table.keyIsAutogenerated = true
        }

        if (isPrimaryKey) {
            if (column is NonNullRowProp<*,*>) {
                @Suppress("UNCHECKED_CAST")
                setIdField(column as NonNullRowProp<E, T>, column.sqlType.kotlinType)
            } else {
                throw IllegalArgumentException("Primary key must be non null")
            }
        }

        if (foreignKey != null)
            foreignKeys.put(column.fieldName, foreignKey)

        if (table.columnsByDbName.put(column.fieldName, column) != null)
            throw IllegalArgumentException("A column named " + column.fieldName + " already exists")

        table.columns.add(column)
    }

    private fun <T : Any>
    setIdField(idField: NonNullRowProp<E, T>, actualClass: KClass<T>) {
        if (!actualClass.isSubclassOf(table.idClass))
            throw IllegalStateException("ID type mismatch in table " + table.dbName + " -- expected " + table.idClass + ", but actual is " + actualClass)

        @Suppress("UNCHECKED_CAST")
        val casted = idField as NonNullRowProp<E, ID>

        if (idFieldInitialized)
            throw IllegalStateException("ID column is already set for table " + table.dbName)

        table.idField = casted
        idFieldInitialized = true
    }


    // TODO: boolean


    internal fun dummyRow(): List<Any> {
        return dummyRow(table.columns)
    }

    fun <COL : Column<E, TID>, TARGET : DbEntity<TARGET, TID>, TID: Any>
    relToOne(sourceColumn: COL): RelToOne<E, TARGET> {
        val foreignKey = foreignKeys[sourceColumn.fieldName] ?: throw IllegalArgumentException("Missing foreign key info for column " + sourceColumn.dbName)

        @Suppress("UNCHECKED_CAST")
        return this.relToOne(sourceColumn, foreignKey.foreignClass as KClass<TARGET>)
    }

    fun <TARGET : DbEntity<TARGET, TID>, TID : CompositeId2<TARGET, A, B, TID>, A : Any, B : Any> relToOne(columnA: Column<E, A>, columnB: Column<E, B>, targetClass: Class<TARGET>): RelToOne<E, TARGET> {
        return relToOne(columnA, columnB, targetClass, null)
    }

    fun <TARGET : DbEntity<TARGET, TID>, TID : CompositeId2<TARGET, A, B, TID>, A : Any, B : Any> relToOne(columnA: Column<E, A>, columnB: Column<E, B>, targetClass: Class<TARGET>, idConstructor: BiFunction<A, B, TID>?): RelToOne<E, TARGET> {
        val result = RelToOneImpl<E, ID, TARGET, TID>()
        table.schema.addLazyInit(PRIORITY_REL_TO_ONE) {
            val targetTable = table.schema.getTableFor(targetClass)
            val targetIdColumns = targetTable.idField as MultiColumn<TARGET, TID>
            val targetId = targetIdColumns.from(dummyRow(targetTable.columns))

            val fields = arrayOfNulls<ColumnMapping<*, *, *>>(2)

            fields[0] = ColumnMapping(columnA, targetId.columnA)
            fields[1] = ColumnMapping(columnB, targetId.columnB)

            val info = ManyToOneInfo<E, ID, TARGET, TID>(table, targetTable, fields)

            val idCons: Function<E, TID>
            if (idConstructor != null) {
                idCons = { source -> idConstructor.apply(columnA.from(source), columnB.from(source)) }
            } else {
                idCons = info.makeForwardMapper()
            }

            result.init(info, idCons)
        }
        return result
    }

    fun <TARGET : DbEntity<TARGET, TID>, TID: Any>
    relToOne(sourceField: Column<E, TID>, targetClass: KClass<TARGET>): RelToOne<E, TARGET> {
        val result = RelToOneImpl<E, ID, TARGET, TID>()
        table.schema.addLazyInit(PRIORITY_REL_TO_ONE) {
            val targetTable = table.schema.getTableFor(targetClass)

            if (!targetTable.idClass.isAssignableFrom(sourceField.sqlType.javaType))
                throw IllegalStateException("Type mismatch on relToOne mapping for table " + table.dbName)

            val fields = arrayOfNulls<ColumnMapping<*, *, *>>(1)

            val field0 = ColumnMapping(sourceField, targetTable.idField as Column<*, *>)

            fields[0] = field0

            val info = ManyToOneInfo<E, ID, TARGET, TID>(table, targetTable, fields)

            result.init(info) { sourceField.from(it) }
        }
        return result
    }

    fun <TARGET : DbEntity<TARGET, TID>, TID> relToMany(oppositeRel: Supplier<RelToOne<TARGET, E>>): RelToMany<E, TARGET> {
        val rel = RelToManyImpl<E, ID, TARGET, TID>()
        table.schema.addLazyInit(PRIORITY_REL_TO_MANY) {
            val relToOne = oppositeRel.get() as RelToOneImpl<TARGET, TID, E, ID>
            val info = relToOne.info
            rel.init(info, relToOne.idMapper, info.makeReverseQueryBuilder())
        }
        return rel
    }

    fun <CTX> withContext(klass: Class<CTX>): WithContext<E, ID, CTX> {
        return table.getOrCreateContext(klass)
    }

    fun onInsert(insertModifier: Consumer<DbInsert<E, ID>>) {
        Objects.requireNonNull(insertModifier)
        table.getOrCreateContext<Any>(null).onInsert { insert, noCtx -> insertModifier.accept(insert) }
    }

    fun onUpdate(updateModifier: Consumer<DbUpdate<E>>) {
        Objects.requireNonNull(updateModifier)
        table.getOrCreateContext<Any>(null).onUpdate { update, ctx -> updateModifier.accept(update) }
    }

    companion object {

        protected val PRIORITY_COLUMNS = 0
        protected val PRIORITY_REL_TO_ONE = 1
        protected val PRIORITY_REL_TO_MANY = 2

        internal fun <E : DbEntity<E, *>> dummyRow(columns: ArrayList<Column<E, *>>): List<Any> {
            val res = ArrayList<Any>()

            for (column in columns)
                addDummy<*>(column.sqlType, res)

            return res
        }

        internal fun <T> addDummy(sqlType: SqlType<T>, out: MutableList<Any>) {
            out.add(sqlType.toJson(sqlType.dummyValue()))
        }

        fun <E : DbEntity<E, ID>, ID : Any>
        determineIdClass(entityClass: KClass<E>): KClass<ID> {
            for (type in entityClass.supertypes) {
                if (type.classifier == DbTable::class) {
                    val actual = type.arguments[1].type
                    if (actual is KClass<*>) {
                        @Suppress("UNCHECKED_CAST")
                        return actual as KClass<ID>
                    }
                }
            }

            throw IllegalStateException("Can't determine ID type of " + entityClass)
        }
    }
}
