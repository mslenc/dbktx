package com.github.mslenc.dbktx.schema

import com.github.mslenc.dbktx.crud.BoundColumnForSelect
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.filters.FilterCompare
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.filters.MatchAnything
import com.github.mslenc.dbktx.filters.MatchNothing

enum class ColumnInMappingKind {
    COLUMN, // an actual column
    CONSTANT, // a fixed value
    PARAMETER // a parameterized value
}

interface ColumnMapping<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>, TYPE: Any> {
    val columnFromKind: ColumnInMappingKind

    fun bindColumnFrom(tableInQuery: TableInQuery<FROM>): BoundColumnForSelect<FROM, TYPE>
    fun bindColumnTo(tableInQuery: TableInQuery<TO>): BoundColumnForSelect<TO, TYPE>

    fun bindFrom(tableInQuery: TableInQuery<FROM>): Expr<FROM, TYPE>
    fun makeEqRef(ref: TO, tableInQuery: TableInQuery<FROM>): FilterExpr

    val columnFromAsNullable: NullableColumn<FROM, TYPE>?
    val columnFromAsNonNull: NonNullColumn<FROM, TYPE>?

    val columnFromLiteral: Expr<FROM, TYPE>

    val rawColumnTo: NonNullColumn<TO, TYPE>
    val rawColumnFrom: Column<FROM, TYPE>
    val rawLiteralFromValue: TYPE
}

class ColumnMappingActualColumn<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>, TYPE: Any>(
        override val rawColumnFrom: Column<FROM, TYPE>,
        override val rawColumnTo: NonNullColumn<TO, TYPE>): ColumnMapping<FROM, TO, TYPE> {

    init {
        if (rawColumnFrom.sqlType.kotlinType !== rawColumnTo.sqlType.kotlinType)
            throw IllegalArgumentException("Mismatch between column types")
    }

    override val columnFromKind: ColumnInMappingKind
        get() = ColumnInMappingKind.COLUMN

    override fun bindColumnFrom(tableInQuery: TableInQuery<FROM>): BoundColumnForSelect<FROM, TYPE> {
        return BoundColumnForSelect(rawColumnFrom, tableInQuery)
    }

    override fun bindColumnTo(tableInQuery: TableInQuery<TO>): BoundColumnForSelect<TO, TYPE> {
        return BoundColumnForSelect(rawColumnTo, tableInQuery)
    }

    override fun bindFrom(tableInQuery: TableInQuery<FROM>): Expr<FROM, TYPE> {
        return bindColumnFrom(tableInQuery)
    }

    override val columnFromAsNullable: NullableColumn<FROM, TYPE>?
        get() = rawColumnFrom as? NullableColumn<FROM, TYPE>

    override val columnFromAsNonNull: NonNullColumn<FROM, TYPE>?
        get() = rawColumnFrom as? NonNullColumn<FROM, TYPE>

    override val columnFromLiteral: Expr<FROM, TYPE>
        get() = throw IllegalStateException("Not a literal")

    override val rawLiteralFromValue: TYPE
        get() = throw IllegalStateException("Not a literal")

    override fun makeEqRef(ref: TO, tableInQuery: TableInQuery<FROM>): FilterExpr {
        return FilterCompare(rawColumnFrom.bindForSelect(tableInQuery), FilterCompare.Op.EQ, rawColumnFrom.makeLiteral(rawColumnTo(ref)))
    }
}

class ColumnMappingLiteral<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>, TYPE: Any>(
        override val rawLiteralFromValue: TYPE,
        override val rawColumnTo: NonNullColumn<TO, TYPE>,
        val isParameter: Boolean): ColumnMapping<FROM, TO, TYPE> {

    override val columnFromKind: ColumnInMappingKind
        get() = if (isParameter) ColumnInMappingKind.PARAMETER else ColumnInMappingKind.CONSTANT

    override fun bindColumnFrom(tableInQuery: TableInQuery<FROM>): BoundColumnForSelect<FROM, TYPE> {
        throw IllegalStateException("Not a column")
    }

    override val rawColumnFrom: Column<FROM, TYPE>
        get() = throw IllegalStateException("Not a column")

    override fun bindColumnTo(tableInQuery: TableInQuery<TO>): BoundColumnForSelect<TO, TYPE> {
        return BoundColumnForSelect(rawColumnTo, tableInQuery)
    }

    override fun bindFrom(tableInQuery: TableInQuery<FROM>): Expr<FROM, TYPE> {
        return columnFromLiteral // (literals aren't bound to tables anyway)
    }

    override val columnFromAsNullable: NullableColumn<FROM, TYPE>?
        get() = null

    override val columnFromAsNonNull: NonNullColumn<FROM, TYPE>?
        get() = null

    override val columnFromLiteral: Expr<FROM, TYPE>
        get() {
            // it's ok to use the target column to make literal, as that's the column the value will actually be compared with..
            @Suppress("UNCHECKED_CAST")
            return rawColumnTo.makeLiteral(rawLiteralFromValue) as Expr<FROM, TYPE>
        }

    override fun makeEqRef(ref: TO, tableInQuery: TableInQuery<FROM>): FilterExpr {
        return if (rawLiteralFromValue == rawColumnTo(ref)) {
            MatchAnything
        } else {
            MatchNothing
        }
    }
}