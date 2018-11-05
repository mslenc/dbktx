package com.github.mslenc.dbktx.schema

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.asyncdb.util.ULong
import com.github.mslenc.dbktx.composite.CompositeId
import com.github.mslenc.dbktx.sqltypes.SqlTypes
import java.math.BigDecimal
import java.time.*
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf
import com.github.mslenc.dbktx.composite.CompositeId2
import com.github.mslenc.dbktx.composite.CompositeId3
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.SqlTypeDef
import com.github.mslenc.dbktx.util.FakeRowData
import com.github.mslenc.dbktx.util.StringSet
import java.util.*

open class DbTableBuilder<E : DbEntity<E, ID>, ID : Any>
internal constructor(
        protected val table: DbTable<E, ID>) {

    private var primaryKeyInitialized: Boolean = false

    fun build(factory: (DbConn, ID, DbRow) -> E): DbTable<E, ID> {
        table.factory = factory

        val columnNames = StringBuilder()
        var i = 0
        val n = table.columns.size
        while (i < n) {
            if (i > 0)
                columnNames.append(", ")
            columnNames.append(table.aliasPrefix)
            columnNames.append(".")
            columnNames.append(table.columns[i].fieldName)
            i++
        }
        table.defaultColumnNames = columnNames.toString()

        return table.validate()
    }

    fun nonNullString(fieldName: String, type: SqlTypeDef, getter: (E) -> String,
                      primaryKey: Boolean = false
                      ): NonNullStringColumn<E> {

        val sqlType = SqlTypes.makeString(type.sqlTypeKind, type.param1, true)
        val column = NonNullStringColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableString(fieldName: String, type: SqlTypeDef, getter: (E) -> String?
                       ): NullableStringColumn<E> {

        val sqlType = SqlTypes.makeString(type.sqlTypeKind, type.param1, false)
        val column = NullableStringColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }

    fun nonNullStringSet(fieldName: String, type: SqlTypeDef, getter: (E) -> StringSet,
                         surroundedWithCommas: Boolean = false): NonNullStringSetColumn<E> {

        val sqlType = SqlTypes.makeStringSet(type.sqlTypeKind, size = type.param1, isNotNull = true, surroundedWithCommas = surroundedWithCommas)
        val column = NonNullStringSetColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }

    fun nullableStringSet(fieldName: String, type: SqlTypeDef, getter: (E) -> StringSet?,
                          surroundedWithCommas: Boolean = false): NullableStringSetColumn<E> {

        val sqlType = SqlTypes.makeStringSet(type.sqlTypeKind, size = type.param1, isNotNull = false, surroundedWithCommas = surroundedWithCommas)
        val column = NullableStringSetColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }


    fun <T: Any> nonNullStringJson(fieldName: String, type: SqlTypeDef, getter: (E) -> T, parsedClass: KClass<T>, dummyValue: T): NonNullColumn<E, T> {

        val sqlType = SqlTypes.makeStringJson(type.sqlTypeKind, size = type.param1, isNotNull = true, parsedClass = parsedClass, dummyValue = dummyValue)
        val column = NonNullColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }

    fun <T: Any> nullableStringJson(fieldName: String, type: SqlTypeDef, getter: (E) -> T?, parsedClass: KClass<T>, dummyValue: T): NullableColumn<E, T> {

        val sqlType = SqlTypes.makeStringJson(type.sqlTypeKind, size = type.param1, isNotNull = false, parsedClass = parsedClass, dummyValue = dummyValue)
        val column = NullableColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }


    fun nonNullInt(fieldName: String, type: SqlTypeDef, getter: (E) -> Int,
                   primaryKey: Boolean? = null,
                   autoIncrement: Boolean = false,
                   unsigned: Boolean = false
                   ): NonNullOrderedColumn<E, Int> {

        val sqlType = SqlTypes.makeInteger(type.sqlTypeKind, isNotNull = true, isAutoGenerated = autoIncrement, isUnsigned = unsigned)
        val column = NonNullOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey ?: autoIncrement, isAutoIncrement = autoIncrement)
        return column
    }

    fun nullableInt(fieldName: String, type: SqlTypeDef, getter: (E) -> Int?,
                    unsigned: Boolean = false
                    ): NullableOrderedColumn<E, Int> {

        val sqlType = SqlTypes.makeInteger(type.sqlTypeKind, isNotNull = false, isAutoGenerated = false, isUnsigned = unsigned)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }



    fun nonNullLong(fieldName: String, type: SqlTypeDef, getter: (E) -> Long,
                    primaryKey: Boolean? = null,
                    autoIncrement: Boolean = false,
                    unsigned: Boolean = false
                    ): NonNullOrderedColumn<E, Long> {

        val sqlType = SqlTypes.makeLong(type.sqlTypeKind, isNotNull = true, isAutoGenerated = autoIncrement, isUnsigned = unsigned)
        val column = NonNullOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey ?: autoIncrement, isAutoIncrement = autoIncrement)
        return column
    }

    fun nullableLong(fieldName: String, type: SqlTypeDef, getter: (E) -> Long?,
                     unsigned: Boolean = false
                     ): NullableOrderedColumn<E, Long> {

        val sqlType = SqlTypes.makeLong(type.sqlTypeKind, isNotNull = false, isAutoGenerated = false, isUnsigned = unsigned)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }


    fun nonNullULong(fieldName: String, type: SqlTypeDef, getter: (E) -> ULong,
                    primaryKey: Boolean? = null,
                    autoIncrement: Boolean = false,
                    unsigned: Boolean = false
    ): NonNullOrderedColumn<E, ULong> {

        val sqlType = SqlTypes.makeULong(type.sqlTypeKind, isNotNull = true, isAutoGenerated = autoIncrement, isUnsigned = unsigned)
        val column = NonNullOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey ?: autoIncrement, isAutoIncrement = autoIncrement)
        return column
    }

    fun nullableULong(fieldName: String, type: SqlTypeDef, getter: (E) -> ULong?,
                     unsigned: Boolean = false
    ): NullableOrderedColumn<E, ULong> {

        val sqlType = SqlTypes.makeULong(type.sqlTypeKind, isNotNull = false, isAutoGenerated = false, isUnsigned = unsigned)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }



    fun nonNullFloat(fieldName: String, type: SqlTypeDef, getter: (E) -> Float,
                     primaryKey: Boolean = false,
                     unsigned: Boolean = false
                     ): NonNullOrderedColumn<E, Float> {

        val sqlType = SqlTypes.makeFloat(type.sqlTypeKind, isNotNull = true, isUnsigned = unsigned)
        val column = NonNullOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableFloat(fieldName: String, type: SqlTypeDef, getter: (E) -> Float?,
                      unsigned: Boolean = false
                      ): NullableOrderedColumn<E, Float> {

        val sqlType = SqlTypes.makeFloat(type.sqlTypeKind, isNotNull = false, isUnsigned = unsigned)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }



    fun nonNullDouble(fieldName: String, type: SqlTypeDef, getter: (E) -> Double,
                      primaryKey: Boolean = false,
                      unsigned: Boolean = false
                      ): NonNullOrderedColumn<E, Double> {

        val sqlType = SqlTypes.makeDouble(type.sqlTypeKind, isNotNull = true, isUnsigned = unsigned)
        val column = NonNullOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableDouble(fieldName: String, type: SqlTypeDef, getter: (E) -> Double?,
                       unsigned: Boolean = false
                       ): NullableOrderedColumn<E, Double> {

        val sqlType = SqlTypes.makeDouble(type.sqlTypeKind, isNotNull = false, isUnsigned = unsigned)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }


    fun nonNullUUID(fieldName: String, type: SqlTypeDef, getter: (E) -> UUID,
                    primaryKey: Boolean = false
                    ): NonNullColumn<E, UUID> {

        val size = type.param1 ?: throw IllegalArgumentException("Missing size for UUID type")
        val sqlType = SqlTypes.makeUUID(type.sqlTypeKind, size = size, isNotNull = true)
        val column = NonNullColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableUUID(fieldName: String, type: SqlTypeDef, getter: (E) -> UUID?
                     ): NullableColumn<E, UUID> {

        val size = type.param1 ?: throw IllegalArgumentException("Missing size for UUID type")
        val sqlType = SqlTypes.makeUUID(type.sqlTypeKind, size = size, isNotNull = false)
        val column = NullableColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }



    fun nonNullYear(fieldName: String, type: SqlTypeDef, getter: (E) -> Year,
                    primaryKey: Boolean = false
                    ): NonNullOrderedColumn<E, Year> {

        val sqlType = SqlTypes.makeYear(type.sqlTypeKind, isNotNull = true)
        val column = NonNullOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableYear(fieldName: String, type: SqlTypeDef, getter: (E) -> Year?
                     ): NullableOrderedColumn<E, Year> {

        val sqlType = SqlTypes.makeYear(type.sqlTypeKind, isNotNull = false)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }



    fun nonNullDateTime(fieldName: String, type: SqlTypeDef, getter: (E) -> LocalDateTime,
                        primaryKey: Boolean = false
                        ): NonNullOrderedColumn<E, LocalDateTime> {

        val sqlType = SqlTypes.makeLocalDateTime(type.sqlTypeKind, isNotNull = true)
        val column = NonNullOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableDateTime(fieldName: String, type: SqlTypeDef, getter: (E) -> LocalDateTime?
                         ): NullableOrderedColumn<E, LocalDateTime> {

        val sqlType = SqlTypes.makeLocalDateTime(type.sqlTypeKind, isNotNull = false)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }


    fun nonNullInstant(fieldName: String, type: SqlTypeDef, getter: (E) -> Instant,
                       primaryKey: Boolean = false
                       ): NonNullOrderedColumn<E, Instant> {

        val sqlType = SqlTypes.makeInstant(type.sqlTypeKind, isNotNull = true)
        val column = NonNullOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableInstant(fieldName: String, type: SqlTypeDef, getter: (E) -> Instant?
                        ): NullableOrderedColumn<E, Instant> {

        val sqlType = SqlTypes.makeInstant(type.sqlTypeKind, isNotNull = false)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }

    fun nullableInstantFromMillis(fieldName: String, type: SqlTypeDef, getter: (E) -> Instant?
                                  ): NullableOrderedColumn<E, Instant> {

        val sqlType = SqlTypes.makeInstantFromMillis(type.sqlTypeKind, isNotNull = false)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }


    fun nonNullTime(fieldName: String, type: SqlTypeDef, getter: (E) -> LocalTime,
                    primaryKey: Boolean = false
                    ): NonNullOrderedColumn<E, LocalTime> {

        val sqlType = SqlTypes.makeLocalTime(type.sqlTypeKind, isNotNull = true)
        val column = NonNullOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableTime(fieldName: String, type: SqlTypeDef, getter: (E) -> LocalTime?
                     ): NullableOrderedColumn<E, LocalTime> {

        val sqlType = SqlTypes.makeLocalTime(type.sqlTypeKind, isNotNull = false)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }


    fun nonNullDuration(fieldName: String, type: SqlTypeDef, getter: (E) -> Duration,
                    primaryKey: Boolean = false
    ): NonNullOrderedColumn<E, Duration> {

        val sqlType = SqlTypes.makeDuration(type.sqlTypeKind, isNotNull = true)
        val column = NonNullOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableDuration(fieldName: String, type: SqlTypeDef, getter: (E) -> Duration?
    ): NullableOrderedColumn<E, Duration> {

        val sqlType = SqlTypes.makeDuration(type.sqlTypeKind, isNotNull = false)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }


    fun nonNullDate(fieldName: String, type: SqlTypeDef, getter: (E) -> LocalDate,
                    primaryKey: Boolean = false
                    ): NonNullOrderedColumn<E, LocalDate> {

        val sqlType = SqlTypes.makeLocalDate(type.sqlTypeKind, isNotNull = true)
        val column = NonNullOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableDate(fieldName: String, type: SqlTypeDef, getter: (E) -> LocalDate?
                     ): NullableOrderedColumn<E, LocalDate> {

        val sqlType = SqlTypes.makeLocalDate(type.sqlTypeKind, isNotNull = false)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }


    fun nonNullBytes(fieldName: String, type: SqlTypeDef, getter: (E) -> ByteArray)
            : NonNullColumn<E, ByteArray> {

        val sqlType = SqlTypes.makeByteArray(type.sqlTypeKind, type.param1, isNotNull = true)
        val column = NonNullColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }

    fun nullableBytes(fieldName: String, type: SqlTypeDef, getter: (E) -> ByteArray?)
            : NullableColumnImpl<E, ByteArray> {

        val sqlType = SqlTypes.makeByteArray(type.sqlTypeKind, type.param1, isNotNull = false)
        val column = NullableColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }


    fun nonNullDecimal(fieldName: String, type: SqlTypeDef, getter: (E) -> BigDecimal,
                       primaryKey: Boolean = false,
                       unsigned: Boolean = false
                       ): NonNullOrderedColumn<E, BigDecimal> {

        val precision: Int = type.param1 ?: throw IllegalArgumentException("Missing precision for DECIMAL")
        val scale: Int = type.param2 ?: throw IllegalArgumentException("Missing scale for DECIMAL")

        val sqlType = SqlTypes.makeBigDecimal(type.sqlTypeKind, precision, scale, isNotNull = true, isUnsigned = unsigned)
        val column = NonNullOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    fun nullableDecimal(fieldName: String, type: SqlTypeDef, getter: (E) -> BigDecimal?,
                        primaryKey: Boolean = false,
                        unsigned: Boolean = false
                        ): NullableOrderedColumn<E, BigDecimal> {

        val precision: Int = type.param1 ?: throw IllegalArgumentException("Missing precision for DECIMAL")
        val scale: Int = type.param2 ?: throw IllegalArgumentException("Missing scale for DECIMAL")

        val sqlType = SqlTypes.makeBigDecimal(type.sqlTypeKind, precision, scale, isNotNull = false, isUnsigned = unsigned)
        val column = NullableOrderedColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    inline fun <reified ENUM : Enum<ENUM>>
            nonNullStringEnum(fieldName: String,
                              typeDef: SqlTypeDef,
                              noinline getter: (E) -> ENUM,
                              noinline toDbRep: (ENUM)->String,
                              noinline fromDbRep: (String)->ENUM,
                              primaryKey: Boolean = false
                              ): NonNullColumn<E, ENUM> {

        val klass = ENUM::class
        val dummyValue = enumValues<ENUM>()[0]

        return nonNullStringEnum(fieldName, typeDef, getter, toDbRep, fromDbRep, klass, dummyValue, primaryKey = primaryKey)
    }

    fun <ENUM : Enum<ENUM>>
            nonNullStringEnum(fieldName: String,
                              typeDef: SqlTypeDef,
                              getter: (E) -> ENUM,
                              toDbRep: (ENUM)->String,
                              fromDbRep: (String)->ENUM,
                              klass: KClass<ENUM>,
                              dummyValue: ENUM,
                              primaryKey: Boolean = false
    ): NonNullColumn<E, ENUM> {

        val sqlType = SqlTypes.makeEnumToString(klass, dummyValue, typeDef.sqlTypeKind, toDbRep, fromDbRep, isNotNull = true)
        val column = NonNullColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    inline fun <reified ENUM : Enum<ENUM>>
            nullableStringEnum(fieldName: String,
                               typeDef: SqlTypeDef,
                               noinline getter: (E) -> ENUM?,
                               noinline toDbRep: (ENUM)->String,
                               noinline fromDbRep: (String)->ENUM
                               ): NullableColumn<E, ENUM> {

        val klass = ENUM::class
        val dummyValue = enumValues<ENUM>()[0]

        return nullableStringEnum(fieldName, typeDef, getter, toDbRep, fromDbRep, klass, dummyValue)
    }

    fun <ENUM : Enum<ENUM>>
            nullableStringEnum(fieldName: String,
                               typeDef: SqlTypeDef,
                               getter: (E) -> ENUM?,
                               toDbRep: (ENUM)->String,
                               fromDbRep: (String)->ENUM,
                               klass: KClass<ENUM>,
                               dummyValue: ENUM
                               ): NullableColumn<E, ENUM> {

        val sqlType = SqlTypes.makeEnumToString(klass, dummyValue, typeDef.sqlTypeKind, toDbRep, fromDbRep, isNotNull = true)
        val column = NullableColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }



    inline fun <reified ENUM : Enum<ENUM>>
            nonNullIntEnum(fieldName: String,
                           typeDef: SqlTypeDef,
                           noinline getter: (E) -> ENUM,
                           noinline toDbRep: (ENUM)->Int,
                           noinline fromDbRep: (Int)->ENUM,
                           primaryKey: Boolean = false
                           ): NonNullColumn<E, ENUM> {

        val klass = ENUM::class
        val dummyValue = enumValues<ENUM>()[0]

        return nonNullIntEnum(fieldName, typeDef, getter, toDbRep, fromDbRep, klass, dummyValue, primaryKey = primaryKey)
    }

    fun <ENUM : Enum<ENUM>>
            nonNullIntEnum(fieldName: String,
                           typeDef: SqlTypeDef,
                           getter: (E) -> ENUM,
                           toDbRep: (ENUM)->Int,
                           fromDbRep: (Int)->ENUM,
                           klass: KClass<ENUM>,
                           dummyValue: ENUM,
                           primaryKey: Boolean = false
                           ): NonNullColumn<E, ENUM> {

        val sqlType = SqlTypes.makeEnumToInt(klass, dummyValue, typeDef.sqlTypeKind, toDbRep, fromDbRep, isNotNull = true)
        val column = NonNullColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column, isPrimaryKey = primaryKey)
        return column
    }

    inline fun <reified ENUM : Enum<ENUM>>
            nullableIntEnum(fieldName: String,
                            noinline getter: (E) -> ENUM?,
                            noinline toDbRep: (ENUM)->Int,
                            noinline fromDbRep: (Int)->ENUM,
                            typeDef: SqlTypeDef
                            ): NullableColumn<E, ENUM> {

        val klass = ENUM::class
        val dummyValue = enumValues<ENUM>()[0]

        return nullableIntEnum(fieldName, getter, toDbRep, fromDbRep, typeDef, klass, dummyValue)
    }

    fun <ENUM : Enum<ENUM>>
            nullableIntEnum(fieldName: String,
                            getter: (E) -> ENUM?,
                            toDbRep: (ENUM)->Int,
                            fromDbRep: (Int)->ENUM,
                            typeDef: SqlTypeDef,
                            klass: KClass<ENUM>,
                            dummyValue: ENUM
                            ): NullableColumn<E, ENUM> {

        val sqlType = SqlTypes.makeEnumToInt(klass, dummyValue, typeDef.sqlTypeKind, toDbRep, fromDbRep, isNotNull = true)
        val column = NullableColumnImpl(table, getter, fieldName, sqlType, table.columns.size)
        finishAddColumn(column)
        return column
    }



    private fun <T : Any> finishAddColumn(column: Column<E, T>, isPrimaryKey: Boolean = false, isAutoIncrement: Boolean = false) {
        if (isAutoIncrement) {
            if (!isPrimaryKey)
                throw IllegalArgumentException("To be auto_increment, the column must be a primary key")
            table.keyIsAutogenerated = true
        }

        if (isPrimaryKey) {
            if (column is NonNullColumn<E, T>) {
                @Suppress("UNCHECKED_CAST")
                setPrimaryKey(SingleColumnKeyDefImpl(table, table.uniqueKeys.size, column as NonNullColumn<E, ID>, isPrimaryKey = true), column.sqlType.kotlinType)
            } else {
                throw IllegalArgumentException("Primary key must be non null")
            }
        }

        if (table.columnsByDbName.put(column.fieldName, column) != null)
            throw IllegalArgumentException("A column named " + column.fieldName + " already exists")

        table.columns.add(column)
    }

    protected fun <T : Any>
    setPrimaryKey(idField: UniqueKeyDef<E, T>, actualClass: KClass<out T>) {
        if (!actualClass.isSubclassOf(table.idClass))
            throw IllegalStateException("ID type mismatch in table " + table.dbName + " -- expected " + table.idClass + ", but actual is " + actualClass)

        @Suppress("UNCHECKED_CAST")
        val casted = idField as UniqueKeyDef<E, ID>

        if (primaryKeyInitialized)
            throw IllegalStateException("ID column is already set for table " + table.dbName)

        if (!casted.isPrimaryKey)
            throw IllegalStateException("ID column not marked as primary key for table " + table.dbName)

        table.primaryKey = casted
        primaryKeyInitialized = true

        table.uniqueKeys.add(casted)
    }


    // TODO: boolean?


    internal fun dummyRow(): DbRow {
        return dummyRow(table.columns)
    }

    fun <TARGET : DbEntity<TARGET, TID>, TID: Any>
    relToOne(sourceColumn: Column<E, TID>, targetClass: KClass<TARGET>): RelToOne<E, TARGET> {
        val result = RelToOneImpl<E, TARGET, TID>()
        table.schema.addLazyInit(PRIORITY_REL_TO_ONE) {
            val targetTable = table.schema.getTableFor(targetClass)

            if (!sourceColumn.sqlType.kotlinType.isSubclassOf(targetTable.idClass))
                throw IllegalStateException("Type mismatch on relToOne mapping for table " + table.dbName)

            val targetPrimaryKey = targetTable.primaryKey
            if (targetPrimaryKey.isComposite)
                throw IllegalStateException("Target has a composite primary key when defining relation ${sourceColumn.fieldName} for table ${table.dbName}")

            @Suppress("UNCHECKED_CAST")
            val targetColumn = targetPrimaryKey.getColumn(1) as NonNullColumn<TARGET, TID>

            val fields: Array<ColumnMapping<E, TARGET, *>> = arrayOf(
                ColumnMappingActualColumn(sourceColumn, targetColumn)
            )

            val info = ManyToOneInfo(table, targetTable, targetPrimaryKey, fields)

            result.init(info, sourceColumn::invoke)
        }
        return result
    }

    fun <TARGET : DbEntity<TARGET, TID>, TID: Any>
    relToMany(oppositeRel: ()-> RelToOne<TARGET, E>): RelToMany<E, TARGET> {
        val rel = RelToManyImpl<E, ID, TARGET>()
        table.schema.addLazyInit(PRIORITY_REL_TO_MANY) {
            @Suppress("UNCHECKED_CAST")
            val relToOne = oppositeRel() as RelToOneImpl<TARGET, E, ID>
            val info = relToOne.info
            rel.init(relToOne, info, relToOne.keyMapper, info.makeReverseQueryBuilder())
        }
        return rel
    }

    fun <TARGET : DbEntity<TARGET, TID>, TID : CompositeId2<TARGET, A, B, TID>, A : Any, B : Any>
    relToOne(columnA: Column<E, A>, columnB: Column<E, B>, targetClass: KClass<TARGET>, idConstructor: ((A, B)->TID)? = null): RelToOne<E, TARGET> {
        val result = RelToOneImpl<E, TARGET, TID>()
        table.schema.addLazyInit(PRIORITY_REL_TO_ONE) {
            val targetTable = table.schema.getTableFor(targetClass)
            @Suppress("UNCHECKED_CAST")
            val targetIdColumns = targetTable.primaryKey as MultiColumnKeyDef<TARGET, TID>
            val targetId = targetIdColumns(dummyRow(targetTable.columns))

            val fields: Array<ColumnMapping<E, TARGET, *>> = arrayOf(
                    ColumnMappingActualColumn(columnA, targetId.column1),
                    ColumnMappingActualColumn(columnB, targetId.column2)
            )

            val info = ManyToOneInfo(table, targetTable, targetIdColumns, fields)

            val idCons: (E)->TID?
            if (idConstructor != null) {
                idCons = { source ->
                    val valA = columnA(source)
                    val valB = columnB(source)
                    if (valA != null && valB != null) idConstructor(valA, valB) else null
                }
            } else {
                idCons = info.makeForwardMapper()
            }

            result.init(info, idCons)
        }
        return result
    }

    fun <TARGET : DbEntity<TARGET, TID>, TID : CompositeId3<TARGET, T1, T2, T3, TID>, T1: Any, T2: Any, T3: Any>
    relToOne(column1: Column<E, T1>, column2: Column<E, T2>, column3: Column<E, T3>, targetClass: KClass<TARGET>, idConstructor: ((T1, T2, T3)->TID)? = null): RelToOne<E, TARGET> {
        val result = RelToOneImpl<E, TARGET, TID>()
        table.schema.addLazyInit(PRIORITY_REL_TO_ONE) {
            val targetTable = table.schema.getTableFor(targetClass)

            @Suppress("UNCHECKED_CAST")
            val targetIdColumns = targetTable.primaryKey as MultiColumnKeyDef<TARGET, TID>
            val targetId = targetIdColumns(dummyRow(targetTable.columns))

            val fields: Array<ColumnMapping<E, TARGET, *>> = arrayOf(
                    ColumnMappingActualColumn(column1, targetId.column1),
                    ColumnMappingActualColumn(column2, targetId.column2),
                    ColumnMappingActualColumn(column3, targetId.column3)
            )

            val info = ManyToOneInfo(table, targetTable, targetIdColumns, fields)

            val idCons: (E)->TID?
            if (idConstructor != null) {
                idCons = { source ->
                    val val1 = column1(source)
                    val val2 = column2(source)
                    val val3 = column3(source)
                    if (val1 != null && val2 != null && val3 != null) idConstructor(val1, val2, val3) else null
                }
            } else {
                idCons = info.makeForwardMapper()
            }

            result.init(info, idCons)
        }
        return result
    }

    fun <TARGET : DbEntity<TARGET, *>, KEY : CompositeId3<TARGET, T1, T2, T3, KEY>, T1: Any, T2: Any, T3: Any>
    relToOne(column1: Column<E, T1>, column2: Column<E, T2>, column3: Column<E, T3>, targetKeyGetter: ()->MultiColumnKeyDef<TARGET, KEY>): RelToOne<E, TARGET> {
        val result = RelToOneImpl<E, TARGET, KEY>()
        table.schema.addLazyInit(PRIORITY_REL_TO_ONE) {
            val targetKey = targetKeyGetter()
            val targetTable = targetKey.table

            val targetIdColumns = targetKey
            val targetId = targetIdColumns(dummyRow(targetTable.columns))

            val fields: Array<ColumnMapping<E, TARGET, *>> = arrayOf(
                    ColumnMappingActualColumn(column1, targetId.column1),
                    ColumnMappingActualColumn(column2, targetId.column2),
                    ColumnMappingActualColumn(column3, targetId.column3)
            )

            val info = ManyToOneInfo(table, targetTable, targetKey, fields)

            val idCons: (E)->KEY? = info.makeForwardMapper()

            result.init(info, idCons)
        }
        return result
    }

    fun <TARGET : DbEntity<TARGET, *>, KEY : Any>
    relToOne(column: Column<E, KEY>, targetKeyGetter: ()->SingleColumnKeyDef<TARGET, KEY>): RelToOne<E, TARGET> {
        val result = RelToOneImpl<E, TARGET, KEY>()
        table.schema.addLazyInit(PRIORITY_REL_TO_ONE) {
            val targetKey = targetKeyGetter()
            val targetTable = targetKey.table

            val fields: Array<ColumnMapping<E, TARGET, *>> = arrayOf(
                ColumnMappingActualColumn(column, targetKey.column)
            )

            val info = ManyToOneInfo(table, targetTable, targetKey, fields)

            val idCons: (E)->KEY? = info.makeForwardMapper()

            result.init(info, idCons)
        }
        return result
    }

    fun <T: CompositeId<E, T>>
    uniqueKey(keyBuilder: (DbRow)->T, keyExtractor: (E)->T): MultiColumnKeyDef<E, T> {
        if (!primaryKeyInitialized)
            throw IllegalStateException("Must first set primary key, before other keys in table " + table.dbName)

        val keyDef = MultiColumnKeyDef(table, table.uniqueKeys.size, keyBuilder, keyExtractor, keyBuilder(dummyRow()), isPrimaryKey = false)
        table.uniqueKeys.add(keyDef)
        return keyDef
    }

    fun <T: Any>
    uniqueKey(column: NonNullColumn<E, T>): SingleColumnKeyDef<E, T> {
        val keyDef = SingleColumnKeyDefImpl(table, table.uniqueKeys.size, column, isPrimaryKey = false)
        table.uniqueKeys.add(keyDef)
        return keyDef
    }

    companion object {
        const val PRIORITY_REL_TO_ONE = 1
        const val PRIORITY_REL_TO_MANY = 2

        internal fun <E : DbEntity<E, *>> dummyRow(columns: ArrayList<Column<E, *>>): DbRow {
            val res = FakeRowData()

            for (column in columns)
                res.insertDummyValue(column)

            return res
        }
    }
}