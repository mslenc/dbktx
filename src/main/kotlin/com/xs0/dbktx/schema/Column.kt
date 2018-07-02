package com.xs0.dbktx.schema

import com.xs0.dbktx.crud.BoundColumnForSelect
import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.expr.Literal
import com.xs0.dbktx.sqltypes.SqlType
import com.xs0.dbktx.crud.EntityValues
import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.expr.ExprIsNull
import com.xs0.dbktx.util.Sql
import com.xs0.dbktx.util.StringSet

interface Column<E: DbEntity<E, *>, T : Any> : RowProp<E, T> {
    val table: DbTable<E, *>
    val fieldName: String
    val quotedFieldName: String
    val sqlType: SqlType<T>
    val indexInRow: Int
    val nonNull: Boolean
    val nullable: Boolean get() = !nonNull

    override operator fun invoke(row: List<Any?>): T? {
        val value = row[indexInRow] ?: return null

        return sqlType.fromJson(value)
    }

    override val isAutoGenerated: Boolean
        get() = sqlType.isAutoGenerated

    override fun extract(values: EntityValues<E>): T? {
        return values.getValue(this)
    }

    override fun makeLiteral(value: T): Expr<E, T> {
        return Literal(value, sqlType)
    }

    operator fun invoke(entity: E): T?
}

interface NonNullColumn<E: DbEntity<E, *>, T: Any>: Column<E, T>, NonNullRowProp<E, T> {
    override fun invoke(row: List<Any?>): T {
        val value = row[indexInRow] ?:
            throw IllegalStateException("Null value for NOT NULL column $fieldName")

        return sqlType.fromJson(value)
    }

    override operator fun invoke(entity: E): T
}

interface NullableColumn<E: DbEntity<E, *>, T: Any>: Column<E, T>, NullableRowProp<E, T>


interface OrderedColumn<E: DbEntity<E, *>, T: Comparable<T>>: Column<E, T>, OrderedProp<E, T>
interface NonNullOrderedColumn<E: DbEntity<E, *>, T: Comparable<T>>: OrderedColumn<E, T>, NonNullColumn<E, T>, NonNullOrderedProp<E, T>
interface NullableOrderedColumn<E: DbEntity<E, *>, T: Comparable<T>>: OrderedColumn<E, T>, NullableColumn<E, T>, NullableOrderedProp<E, T>


interface StringColumn<E: DbEntity<E, *>> : OrderedColumn<E, String>
interface NonNullStringColumn<E: DbEntity<E, *>> : NonNullOrderedColumn<E, String>, StringColumn<E>
interface NullableStringColumn<E: DbEntity<E, *>> : NullableOrderedColumn<E, String>, StringColumn<E>


interface StringSetColumn<E: DbEntity<E, *>> : Column<E, StringSet>
interface NonNullStringSetColumn<E: DbEntity<E, *>> : StringSetColumn<E>, NonNullColumn<E, StringSet>
interface NullableStringSetColumn<E: DbEntity<E, *>> : StringSetColumn<E>, NullableColumn<E, StringSet>


abstract class ColumnImpl<E : DbEntity<E, *>, T: Any>(
        final override val table: DbTable<E, *>,
        private val getter: (E) -> T?,
        final override val fieldName: String,
        final override val sqlType: SqlType<T>,
        final override val indexInRow: Int)

    : Column<E, T> {

    final override val quotedFieldName = Sql.quoteIdentifier(fieldName)

    override fun invoke(entity: E): T? {
        return getter(entity)
    }

    override fun bindForSelect(tableInQuery: TableInQuery<E>): Expr<E, T> {
        return BoundColumnForSelect(this, tableInQuery)
    }
}

class NonNullColumnImpl<E : DbEntity<E, *>, T: Any>(
        table: DbTable<E, *>,
        private val getter: (E) -> T,
        fieldName: String,
        sqlType: SqlType<T>,
        indexInRow: Int)

    : ColumnImpl<E, T>(table, getter, fieldName, sqlType, indexInRow),
        NonNullColumn<E, T> {

    override val nonNull: Boolean = true

    override fun invoke(entity: E): T {
        return getter(entity)
    }
}

open class NullableColumnImpl<E : DbEntity<E, *>, T: Any>(
        table: DbTable<E, *>,
        getter: (E) -> T?,
        fieldName: String,
        sqlType: SqlType<T>,
        indexInRow: Int)

    : ColumnImpl<E, T>(table, getter, fieldName, sqlType, indexInRow),
        NullableColumn<E, T> {

    override val nonNull: Boolean = false

    override fun makeIsNullExpr(currentTable: TableInQuery<E>, isNull: Boolean): ExprBoolean {
        return ExprIsNull(bindForSelect(currentTable), isNull)
    }
}


abstract class OrderedColumnImpl<E : DbEntity<E, *>, T: Comparable<T>>(
        final override val table: DbTable<E, *>,
        private val getter: (E) -> T?,
        final override val fieldName: String,
        final override val sqlType: SqlType<T>,
        final override val indexInRow: Int)

    : OrderedColumn<E, T> {

    final override val quotedFieldName: String = Sql.quoteIdentifier(fieldName)

    override fun invoke(entity: E): T? {
        return getter(entity)
    }

    override fun bindForSelect(tableInQuery: TableInQuery<E>): Expr<E, T> {
        return BoundColumnForSelect(this, tableInQuery)
    }
}

class NonNullOrderedColumnImpl<E : DbEntity<E, *>, T: Comparable<T>>(
        table: DbTable<E, *>,
        private val getter: (E) -> T,
        fieldName: String,
        sqlType: SqlType<T>,
        indexInRow: Int)

    : OrderedColumnImpl<E, T>(table, getter, fieldName, sqlType, indexInRow),
        NonNullOrderedColumn<E, T> {

    override val nonNull = true

    override fun invoke(entity: E): T {
        return getter(entity)
    }
}

class NullableOrderedColumnImpl<E : DbEntity<E, *>, T: Comparable<T>>(
        table: DbTable<E, *>,
        getter: (E) -> T?,
        fieldName: String,
        sqlType: SqlType<T>,
        indexInRow: Int)

    : OrderedColumnImpl<E, T>(table, getter, fieldName, sqlType, indexInRow),
        NullableOrderedColumn<E, T> {

    override val nonNull = false

    override fun makeIsNullExpr(currentTable: TableInQuery<E>, isNull: Boolean): ExprBoolean {
        return ExprIsNull(bindForSelect(currentTable), isNull)
    }
}




sealed class StringColumnImpl<E : DbEntity<E, *>>(
        override val table: DbTable<E, *>,
        private val getter: (E) -> String?,
        final override val fieldName: String,
        override val sqlType: SqlType<String>,
        override val indexInRow: Int)

    : StringColumn<E> {

    override val quotedFieldName: String = Sql.quoteIdentifier(fieldName)

    override fun invoke(entity: E): String? {
        return getter(entity)
    }

    override fun bindForSelect(tableInQuery: TableInQuery<E>): Expr<E, String> {
        return BoundColumnForSelect(this, tableInQuery)
    }
}

class NonNullStringColumnImpl<E : DbEntity<E, *>>(
        table: DbTable<E, *>,
        private val getter: (E) -> String,
        fieldName: String,
        sqlType: SqlType<String>,
        indexInRow: Int)

    : StringColumnImpl<E>(table, getter, fieldName, sqlType, indexInRow),
        NonNullStringColumn<E>
{
    override val nonNull = true

    override fun invoke(entity: E): String {
        return getter(entity)
    }
}

class NullableStringColumnImpl<E : DbEntity<E, *>>(
        table: DbTable<E, *>,
        getter: (E) -> String?,
        fieldName: String,
        sqlType: SqlType<String>,
        indexInRow: Int)
    : StringColumnImpl<E>(table, getter, fieldName, sqlType, indexInRow),
        NullableStringColumn<E> {

    override val nonNull = false

    override fun makeIsNullExpr(currentTable: TableInQuery<E>, isNull: Boolean): ExprBoolean {
        return ExprIsNull(bindForSelect(currentTable), isNull)
    }
}



sealed class StringSetColumnImpl<E : DbEntity<E, *>>(
        override val table: DbTable<E, *>,
        private val getter: (E) -> StringSet?,
        final override val fieldName: String,
        override val sqlType: SqlType<StringSet>,
        override val indexInRow: Int)

    : StringSetColumn<E> {

    override val quotedFieldName: String = Sql.quoteIdentifier(fieldName)

    override fun invoke(entity: E): StringSet? {
        return getter(entity)
    }

    override fun bindForSelect(tableInQuery: TableInQuery<E>): Expr<E, StringSet> {
        return BoundColumnForSelect(this, tableInQuery)
    }
}

class NonNullStringSetColumnImpl<E : DbEntity<E, *>>(
        table: DbTable<E, *>,
        private val getter: (E) -> StringSet,
        fieldName: String,
        sqlType: SqlType<StringSet>,
        indexInRow: Int)

    : StringSetColumnImpl<E>(table, getter, fieldName, sqlType, indexInRow),
        NonNullStringSetColumn<E>
{
    override val nonNull = true

    override fun invoke(entity: E): StringSet {
        return getter(entity)
    }
}

class NullableStringSetColumnImpl<E : DbEntity<E, *>>(
        table: DbTable<E, *>,
        getter: (E) -> StringSet?,
        fieldName: String,
        sqlType: SqlType<StringSet>,
        indexInRow: Int)
    : StringSetColumnImpl<E>(table, getter, fieldName, sqlType, indexInRow),
        NullableStringSetColumn<E> {

    override val nonNull = false

    override fun makeIsNullExpr(currentTable: TableInQuery<E>, isNull: Boolean): ExprBoolean {
        return ExprIsNull(bindForSelect(currentTable), isNull)
    }
}