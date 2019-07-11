package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.expr.*
import com.github.mslenc.dbktx.filters.*
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.sqltypes.SqlTypeVarchar
import com.github.mslenc.dbktx.util.escapeSqlLikePattern

@DslMarker
annotation class SqlExprBuilder

@SqlExprBuilder
interface FilterBuilder<E: DbEntity<E, *>> {
    fun currentTable(): TableInQuery<E>
    fun <T: Any> bind(prop: RowProp<E, T>): Expr<E, T>

    infix fun <T: Any> Expr<E, T>.eq(other: Expr<E, T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.EQ, other)
    }

    infix fun <T: Any> RowProp<E, T>.eq(other: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.EQ, this.makeLiteral(other))
    }

    infix fun <T: Any> Expr<E, T>.neq(other: Expr<E, T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.NEQ, other)
    }

    infix fun <T: Any> RowProp<E, T>.neq(other: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.NEQ, this.makeLiteral(other))
    }

    infix fun <T: Any> RowProp<E, T>.oneOf(values: Set<T>): FilterExpr {
        return when {
            values.isEmpty() ->
                throw IllegalArgumentException("Can't have empty set with oneOf")
            values.size == 1 ->
                this.eq(values.first())
            else ->
                this.bindForSelect(currentTable()).oneOf(values.map { makeLiteral(it) })
        }
    }

    infix fun <T: Any> RowProp<E, T>.oneOf(values: Iterable<T>): FilterExpr {
        return if (values is Set) {
            oneOf(values)
        } else {
            oneOf(values.toSet())
        }
    }

    infix fun <T : Any> Expr<E, T>.lt(value: Expr<E, T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.LT, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lt(value: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.LT, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lt(value: Expr<E, T>): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.LT, value)
    }

    infix fun <T : Any> Expr<E, T>.lte(value: Expr<E, T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.LTE, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lte(value: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.LTE, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lte(value: Expr<E, T>): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.LTE, value)
    }

    infix fun <T : Any> Expr<E, T>.gt(value: Expr<E, T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.GT, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gt(value: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.GT, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gt(value: Expr<E, T>): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.GT, value)
    }

    infix fun <T : Any> Expr<E, T>.gte(value: Expr<E, T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.GTE, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gte(value: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.GTE, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gte(value: Expr<E, T>): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.GTE, value)
    }

    fun <T : Comparable<T>> Expr<E, T>.between(minimum: Expr<E, T>, maximum: Expr<E, T>): FilterExpr {
        return FilterBetween(this, minimum, maximum, between = true)
    }

    fun <T : Comparable<T>> Expr<E, T>.notBetween(minimum: Expr<E, T>, maximum: Expr<E, T>): FilterExpr {
        return FilterBetween(this, minimum, maximum, between = false)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.between(range: ClosedRange<T>): FilterExpr {
        if (range.isEmpty())
            throw IllegalArgumentException("Can't have an empty range")

        return bind(this).between(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun <T : Comparable<T>> OrderedProp<E, T>.between(minimum: T, maximum: T): FilterExpr {
        return bind(this).between(makeLiteral(minimum), makeLiteral(maximum))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.notBetween(range: ClosedRange<T>): FilterExpr {
        if (range.isEmpty())
            throw IllegalArgumentException("Can't have an empty range")

        return bind(this).notBetween(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun <T : Comparable<T>> OrderedProp<E, T>.notBetween(minimum: T, maximum: T): FilterExpr {
        return bind(this).notBetween(makeLiteral(minimum), makeLiteral(maximum))
    }


    infix fun ExprString<E>.contains(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun ExprString<E>.startsWith(value: String): FilterExpr {
        return like(escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun ExprString<E>.endsWith(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|'), '|')
    }

    infix fun ExprString<E>.like(pattern: String): FilterExpr {
        return like(pattern, '|')
    }

    infix fun ExprString<E>.like(pattern: Expr<in E, String>): FilterExpr {
        return like(pattern, '|')
    }

    fun ExprString<E>.like(pattern: String, escapeChar: Char): FilterExpr {
        return like(SqlTypeVarchar.makeLiteral(pattern), escapeChar)
    }

    fun ExprString<E>.like(pattern: Expr<in E, String>, escapeChar: Char): FilterExpr {
        return FilterLike(this, pattern, escapeChar)
    }


    infix fun StringColumn<E>.contains(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun StringColumn<E>.startsWith(value: String): FilterExpr {
        return like(escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun StringColumn<E>.endsWith(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|'), '|')
    }

    infix fun StringColumn<E>.like(pattern: String): FilterExpr {
        return like(pattern, '|')
    }

    infix fun StringColumn<E>.like(pattern: Expr<in E, String>): FilterExpr {
        return like(pattern, '|')
    }

    fun StringColumn<E>.like(pattern: String, escapeChar: Char): FilterExpr {
        return like(SqlTypeVarchar.makeLiteral(pattern), escapeChar)
    }

    fun StringColumn<E>.like(pattern: Expr<in E, String>, escapeChar: Char): FilterExpr {
        return FilterLike(bind(this), pattern, escapeChar)
    }

    infix fun StringSetColumn<E>.contains(value: String): FilterExpr {
        return ExprFindInSet(SqlTypeVarchar.makeLiteral(value), bindForSelect(currentTable()))
    }

    infix fun <T : Any> Expr<E, T>.oneOf(values: List<Expr<E, T>>): FilterExpr {
        if (values.isEmpty())
            throw IllegalArgumentException("No possibilities specified")

        return FilterOneOf.oneOf(this, values)
    }

    infix fun FilterExpr.and(other: FilterExpr): FilterExpr {
        return FilterBoolean.create(this, FilterBoolean.Op.AND, other)
    }

    infix fun FilterExpr.or(other: FilterExpr): FilterExpr {
        return FilterBoolean.create(this, FilterBoolean.Op.OR, other)
    }

    fun <T : Any> NOW(): ExprNow<E, T> {
        return ExprNow()
    }

    fun <TO: DbEntity<TO, *>> RelToZeroOrOne<E, TO>.has(block: FilterBuilder<TO>.() -> FilterExpr): FilterExpr {
        val dstTable = currentTable().subQueryOrJoin(this)
        val dstFilter = TableInQueryBoundFilterBuilder(dstTable)
        val setFilter = dstFilter.block()
        val relImpl = this as RelToZeroOrOneImpl<E, *, TO>

        return FilterHasAssociated(currentTable(), relImpl.info, setFilter, dstTable)
    }

    fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.has(block: FilterBuilder<TO>.() -> FilterExpr): FilterExpr {
        val dstTable = currentTable().subQueryOrJoin(this)
        val dstFilter = TableInQueryBoundFilterBuilder(dstTable)
        val parentFilter = dstFilter.block()

        return FilterHasParent((this as RelToOneImpl<E, TO, *>).info, parentFilter, currentTable(), dstTable)
    }

    infix fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.oneOf(parentFilter: EntityQuery<TO>): FilterExpr {
        parentFilter as EntityQueryImpl<TO>

        if (parentFilter.filters == null)
            return this.isNotNull

        val dstTable = currentTable().subQueryOrJoin(this)
        val remappedFilter = parentFilter.copyAndRemapFilters(dstTable)

        return FilterHasParent((this as RelToOneImpl<E, TO, *>).info, remappedFilter!!, currentTable(), dstTable)
    }

    val NullableRowProp<E, *>.isNull: FilterExpr
        get() {
            return this.makeIsNullExpr(currentTable(), isNull = true)
        }

    val NullableRowProp<E, *>.isNotNull: FilterExpr
        get() {
            return this.makeIsNullExpr(currentTable(), isNull = false)
        }

    val <TO : DbEntity<TO, *>> RelToOne<E, TO>.isNull: FilterExpr
        get() {
            // a multi-column reference is null if any of its parts are null, because we only allow references to non-null columns..

            val rel = this as RelToOneImpl<E, TO, *>
            val parts = ArrayList<FilterExpr>()

            rel.info.columnMappings.forEach { colMap ->
                colMap.columnFromAsNullable?.let { column ->
                    parts.add(column.makeIsNullExpr(currentTable(), isNull = true))
                }
            }

            if (parts.isEmpty())
                throw IllegalStateException("isNull used on a reference where nothing can be null")

            return FilterExpr.createOR(parts)
        }

    val <TO : DbEntity<TO, *>> RelToOne<E, TO>.isNotNull: FilterExpr
        get() {
            return !isNull
        }

    infix fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.eq(ref: TO): FilterExpr {
        this as RelToOneImpl<E, TO, *>

        val colMappings = this.info.columnMappings

        val parts = ArrayList<FilterExpr>()

        colMappings.forEach { colMap ->
            when (colMap.columnFromKind) {
                ColumnInMappingKind.COLUMN -> {
                    // this is basically equivalent to col_from = value_of_col_to(ref)
                    parts.add(colMap.makeEqRef(ref, currentTable()))
                }

                ColumnInMappingKind.CONSTANT,
                ColumnInMappingKind.PARAMETER -> {
                    // here, we have a constant on the from side, and also a constant on the to side..
                    val fakeEqRef = colMap.makeEqRef(ref, currentTable())
                    if (fakeEqRef is FilterDummy) {
                        if (fakeEqRef.matchAll) {
                            // well then, we can obviously ignore it
                        } else {
                            // well then, we will fail everything anyway
                            return fakeEqRef // (it's already what we'd return, so no point in making a new one..)
                        }
                    } else {
                        throw IllegalStateException("Expected an ExprDummy")
                    }
                }
            }
        }

        return FilterBoolean(parts, FilterBoolean.Op.AND)
    }

    infix fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.oneOf(refs: Iterable<TO>): FilterExpr {
        this as RelToOneImpl<E, TO, *>

        val refList = refs as? List ?: refs.toList()

        return when {
            refList.isEmpty() ->
                throw IllegalArgumentException("No choices provided to oneOf")

            refList.size == 1 ->
                eq(refList.first())

            else ->
                FilterOneOfRelToOne(currentTable(), info, refList)
        }
    }

    fun <TO: DbEntity<TO, *>> RelToMany<E, TO>.isNotEmpty(): FilterExpr {
        val dstTable = currentTable().forcedSubQuery(this)
        val relImpl = this as RelToManyImpl<E, *, TO>

        return FilterContainsChild(currentTable(), relImpl.info, null, dstTable)
    }

    fun <TO: DbEntity<TO, *>> RelToMany<E, TO>.isEmpty(): FilterExpr {
        val dstTable = currentTable().forcedSubQuery(this)
        val relImpl = this as RelToManyImpl<E, *, TO>

        return FilterContainsChild(currentTable(), relImpl.info, null, dstTable, negated = true)
    }

    fun <TO: DbEntity<TO, *>> RelToMany<E, TO>.contains(block: FilterBuilder<TO>.() -> FilterExpr): FilterExpr {
        val dstTable = currentTable().forcedSubQuery(this)
        val dstFilter = TableInQueryBoundFilterBuilder(dstTable)
        val setFilter = dstFilter.block()
        val relImpl = this as RelToManyImpl<E, *, TO>

        return FilterContainsChild(currentTable(), relImpl.info, setFilter, dstTable)
    }

    infix fun <TO: DbEntity<TO, *>> RelToMany<E, TO>.contains(childQuery: EntityQuery<TO>): FilterExpr {
        val relImpl = this as RelToManyImpl<E, *, TO>
        val dstTable = currentTable().forcedSubQuery(this)
        val dstFilter = childQuery.copyAndRemapFilters(dstTable)

        return FilterContainsChild(currentTable(), relImpl.info, dstFilter, dstTable)
    }

    fun <T> combineWithOR(values: Iterable<T>, map: FilterBuilder<E>.(T)->FilterExpr): FilterExpr {
        var parts = values.map { map(it) }
        return FilterBoolean.create(FilterBoolean.Op.OR, parts)
    }

    infix fun Column<E, Int>.hasAnyOfBits(bits: Int): FilterExpr {
        return FilterBitwise(this.bindForSelect(currentTable()), FilterBitwise.Op.HAS_ANY_BITS, makeLiteral(bits))
    }

    infix fun Column<E, Long>.hasAnyOfBits(bits: Long): FilterExpr {
        return FilterBitwise(this.bindForSelect(currentTable()), FilterBitwise.Op.HAS_ANY_BITS, makeLiteral(bits))
    }
}