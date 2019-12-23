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
    fun <T: Any> bind(prop: RowProp<E, T>): Expr<T>

    infix fun <T: Any> Expr<T>.eq(other: Expr<T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.EQ, other)
    }

    infix fun <T: Any> NonNullRowProp<E, T>.eq(other: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.EQ, this.makeLiteral(other))
    }

    infix fun <T: Any> NullableRowProp<E, T>.eq(other: T?): FilterExpr {
        return when (other) {
            null -> isNull()
            else -> FilterCompare(bind(this), FilterCompare.Op.EQ, this.makeLiteral(other))
        }
    }

    infix fun <T: Any> Expr<T>.neq(other: Expr<T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.NEQ, other)
    }

    infix fun <T: Any> NonNullRowProp<E, T>.neq(other: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.NEQ, this.makeLiteral(other))
    }

    infix fun <T: Any> NullableRowProp<E, T>.neq(other: T?): FilterExpr {
        return when (other) {
            null -> isNotNull()
            else -> FilterCompare(bind(this), FilterCompare.Op.NEQ, this.makeLiteral(other))
        }
    }

    infix fun <T: Any> NonNullRowProp<E, T>.oneOf(values: Set<T>): FilterExpr {
        return when {
            values.isEmpty() ->
                MatchNothing
            values.size == 1 ->
                this.eq(values.first())
            else ->
                this.bindForSelect(currentTable()).oneOf(values.map { makeLiteral(it) })
        }
    }

    infix fun <T: Any> NullableRowProp<E, T>.oneOf(values: Set<T?>): FilterExpr {
        return when {
            values.isEmpty() ->
                MatchNothing
            values.size == 1 ->
                this.eq(values.first())
            else -> {
                if (values.contains(null)) {
                    return oneOf(values.filterNotNullTo(HashSet())) or isNull()
                } else {
                    values as Set<T>
                    this.bindForSelect(currentTable()).oneOf(values.map { makeLiteral(it) })
                }
            }
        }
    }


    infix fun <T: Any> NonNullRowProp<E, T>.oneOf(values: Iterable<T>): FilterExpr {
        return if (values is Set) {
            oneOf(values)
        } else {
            oneOf(values.toSet())
        }
    }

    infix fun <T: Any> NullableRowProp<E, T>.oneOf(values: Iterable<T?>): FilterExpr {
        return if (values is Set) {
            oneOf(values)
        } else {
            oneOf(values.toSet())
        }
    }


    infix fun <T : Any> Expr<T>.lt(value: Expr<T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.LT, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lt(value: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.LT, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lt(value: Expr<T>): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.LT, value)
    }

    infix fun <T : Any> Expr<T>.lte(value: Expr<T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.LTE, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lte(value: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.LTE, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lte(value: Expr<T>): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.LTE, value)
    }

    infix fun <T : Any> Expr<T>.gt(value: Expr<T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.GT, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gt(value: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.GT, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gt(value: Expr<T>): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.GT, value)
    }

    infix fun <T : Any> Expr<T>.gte(value: Expr<T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.GTE, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gte(value: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.GTE, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gte(value: Expr<T>): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.GTE, value)
    }

    fun <T : Comparable<T>> Expr<T>.between(minimum: Expr<T>, maximum: Expr<T>): FilterExpr {
        return FilterBetween(this, minimum, maximum, between = true)
    }

    fun <T : Comparable<T>> Expr<T>.notBetween(minimum: Expr<T>, maximum: Expr<T>): FilterExpr {
        return FilterBetween(this, minimum, maximum, between = false)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.within(range: ClosedRange<T>): FilterExpr {
        if (range.isEmpty())
            return MatchNothing

        return bind(this).between(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun <T : Comparable<T>> OrderedProp<E, T>.between(minimum: T, maximum: T): FilterExpr {
        return bind(this).between(makeLiteral(minimum), makeLiteral(maximum))
    }

    fun <T : Comparable<T>> OrderedProp<E, T>.betweenOpt(minimum: T?, maximum: T?): FilterExpr {
        return when {
            minimum != null && maximum != null -> between(minimum, maximum)
            minimum != null -> this gte minimum
            maximum != null -> this lte maximum
            else -> MatchAnything
        }
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.notWithin(range: ClosedRange<T>): FilterExpr {
        if (range.isEmpty())
            return MatchAnything

        return bind(this).notBetween(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun <T : Comparable<T>> OrderedProp<E, T>.notBetween(minimum: T, maximum: T): FilterExpr {
        return bind(this).notBetween(makeLiteral(minimum), makeLiteral(maximum))
    }

    fun <T : Comparable<T>> OrderedProp<E, T>.notBetweenOpt(minimum: T?, maximum: T?): FilterExpr {
        return !betweenOpt(minimum, maximum)
    }

    infix fun ExprString.contains(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun ExprString.icontains(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|', caseInsensitive = true)
    }

    infix fun ExprString.startsWith(value: String): FilterExpr {
        return like(escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun ExprString.istartsWith(value: String): FilterExpr {
        return like(escapeSqlLikePattern(value, '|') + "%", '|', caseInsensitive = true)
    }

    infix fun ExprString.endsWith(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|'), '|')
    }

    infix fun ExprString.iendsWith(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|'), '|', caseInsensitive = true)
    }

    infix fun ExprString.like(pattern: String): FilterExpr {
        return like(pattern, '|')
    }

    infix fun ExprString.ilike(pattern: String): FilterExpr {
        return like(pattern, '|', caseInsensitive = true)
    }

    infix fun ExprString.like(pattern: Expr<String>): FilterExpr {
        return like(pattern, '|')
    }

    infix fun ExprString.ilike(pattern: Expr<String>): FilterExpr {
        return like(pattern, '|', caseInsensitive = true)
    }

    fun ExprString.like(pattern: String, escapeChar: Char, caseInsensitive: Boolean = false): FilterExpr {
        return like(SqlTypeVarchar.makeLiteral(pattern), escapeChar, caseInsensitive = caseInsensitive)
    }

    fun ExprString.like(pattern: Expr<String>, escapeChar: Char, caseInsensitive: Boolean = false): FilterExpr {
        return FilterLike(this, pattern, escapeChar)
    }


    infix fun StringColumn<E>.contains(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun StringColumn<E>.icontains(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|', caseInsensitive = true)
    }

    infix fun StringColumn<E>.startsWith(value: String): FilterExpr {
        return like(escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun StringColumn<E>.istartsWith(value: String): FilterExpr {
        return like(escapeSqlLikePattern(value, '|') + "%", '|', caseInsensitive = true)
    }

    infix fun StringColumn<E>.endsWith(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|'), '|')
    }

    infix fun StringColumn<E>.iendsWith(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|'), '|', caseInsensitive = true)
    }

    infix fun StringColumn<E>.like(pattern: String): FilterExpr {
        return like(pattern, '|')
    }

    infix fun StringColumn<E>.ilike(pattern: String): FilterExpr {
        return like(pattern, '|', caseInsensitive = true)
    }

    infix fun StringColumn<E>.like(pattern: Expr<String>): FilterExpr {
        return like(pattern, '|')
    }

    infix fun StringColumn<E>.ilike(pattern: Expr<String>): FilterExpr {
        return like(pattern, '|', caseInsensitive = true)
    }

    fun StringColumn<E>.like(pattern: String, escapeChar: Char, caseInsensitive: Boolean = false): FilterExpr {
        return like(SqlTypeVarchar.makeLiteral(pattern), escapeChar, caseInsensitive = caseInsensitive)
    }

    fun StringColumn<E>.like(pattern: Expr<String>, escapeChar: Char, caseInsensitive: Boolean = false): FilterExpr {
        return FilterLike(bind(this), pattern, escapeChar, caseInsensitive = caseInsensitive)
    }

    infix fun StringSetColumn<E>.contains(value: String): FilterExpr {
        return ExprFindInSet(SqlTypeVarchar.makeLiteral(value), bindForSelect(currentTable()))
    }

    infix fun <T : Any> Expr<T>.oneOf(values: List<Expr<T>>): FilterExpr {
        return FilterOneOf.oneOf(this, values)
    }

    fun <T : Any> NOW(): ExprNow<T> {
        return ExprNow()
    }

    fun <TO: DbEntity<TO, *>> RelToZeroOrOne<E, TO>.has(block: FilterBuilder<TO>.() -> FilterExpr): FilterExpr {
        val dstTable = currentTable().subQueryOrJoin(this)
        val dstFilter = TableInQueryBoundFilterBuilder(dstTable)

        return when (val setFilter = dstFilter.block()) {
            MatchNothing -> MatchNothing
            else -> FilterHasAssociated(currentTable(), (this as RelToZeroOrOneImpl<E, *, TO>).info, setFilter, dstTable)
        }
    }

    fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.has(block: FilterBuilder<TO>.() -> FilterExpr): FilterExpr {
        val dstTable = currentTable().subQueryOrJoin(this)
        val dstFilter = TableInQueryBoundFilterBuilder(dstTable)

        return when (val parentFilter = dstFilter.block()) {
            MatchAnything -> this.isNotNull()
            MatchNothing -> MatchNothing
            else -> FilterHasParent((this as RelToOneImpl<E, TO, *>).info, parentFilter, currentTable(), dstTable)
        }
    }

    fun <TO : DbEntity<TO, *>> RelToSingle<E, TO>.has(block: FilterBuilder<TO>.() -> FilterExpr): FilterExpr {
        return when (this) {
            is RelToOne -> this.has(block)
            is RelToZeroOrOne -> this.has(block)
            else -> throw IllegalStateException()
        }
    }

    fun <TO : DbEntity<TO, *>> Rel<E, TO>.matches(block: FilterBuilder<TO>.() -> FilterExpr): FilterExpr {
        return when (this) {
            is RelToOne -> this.has(block)
            is RelToMany -> this.contains(block)
            is RelToZeroOrOne -> this.has(block)
            else -> throw IllegalStateException()
        }
    }

    infix fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.oneOf(parentFilter: EntityQuery<TO>): FilterExpr {
        return when (parentFilter.filteringState()) {
            FilteringState.MATCH_ALL -> this.isNotNull()
            FilteringState.MATCH_NONE -> MatchNothing
            FilteringState.MATCH_SOME -> {
                val dstTable = currentTable().subQueryOrJoin(this)
                val remappedFilter = parentFilter.copyAndRemapFilters(dstTable)

                FilterHasParent((this as RelToOneImpl<E, TO, *>).info, remappedFilter, currentTable(), dstTable)
            }
        }
    }

    infix fun <TO : DbEntity<TO, *>> RelToZeroOrOne<E, TO>.oneOf(parentFilter: EntityQuery<TO>): FilterExpr {
        return when (parentFilter.filteringState()) {
            FilteringState.MATCH_NONE -> MatchNothing
            else -> {
                val dstTable = currentTable().subQueryOrJoin(this)
                val remappedFilter = parentFilter.copyAndRemapFilters(dstTable)

                FilterHasAssociated(currentTable(), (this as RelToZeroOrOneImpl<E, *, TO>).info, remappedFilter, dstTable)
            }
        }
    }

    infix fun <TO : DbEntity<TO, *>> RelToSingle<E, TO>.oneOf(parentFilter: EntityQuery<TO>): FilterExpr {
        return when (this) {
            is RelToOne -> oneOf(parentFilter)
            is RelToZeroOrOne -> oneOf(parentFilter)
            else -> throw IllegalStateException()
        }
    }

    fun NullableRowProp<E, *>.isNull(): FilterExpr = this.makeIsNullExpr(currentTable(), isNull = true)

    fun NullableRowProp<E, *>.isNotNull(): FilterExpr = this.makeIsNullExpr(currentTable(), isNull = false)

    fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.isNull(): FilterExpr {
        // a multi-column reference is null if any of its parts are null, because we only allow references to non-null columns..

        val rel = this as RelToOneImpl<E, TO, *>
        val parts = ArrayList<FilterExpr>()

        rel.info.columnMappings.forEach { colMap ->
            colMap.columnFromAsNullable?.let { column ->
                parts.add(column.makeIsNullExpr(currentTable(), isNull = true))
            }
        }

        return FilterOr.create(parts)
    }

    fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.isNotNull(): FilterExpr = !isNull()

    fun <TO : DbEntity<TO, *>> RelToZeroOrOne<E, TO>.isNotNull(): FilterExpr {
        return has { MatchAnything }
    }

    fun <TO : DbEntity<TO, *>> RelToZeroOrOne<E, TO>.isNull(): FilterExpr {
        return !isNotNull()
    }

    fun <TO : DbEntity<TO, *>> RelToSingle<E, TO>.isNotNull(): FilterExpr {
        return when (this) {
            is RelToOne -> isNotNull()
            is RelToZeroOrOne -> isNotNull()
            else -> throw IllegalStateException()
        }
    }

    fun <TO : DbEntity<TO, *>> RelToSingle<E, TO>.isNull(): FilterExpr {
        return when (this) {
            is RelToOne -> isNull()
            is RelToZeroOrOne -> isNull()
            else -> throw IllegalStateException()
        }
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

                    when (colMap.makeEqRef(ref, currentTable())) {
                        MatchAnything -> { /* ignore, it does nothing */ }
                        MatchNothing -> return MatchNothing // we'll match nothing overall, so..
                        else -> throw IllegalStateException("Expected MatchAnything or MatchNothing")
                    }
                }
            }
        }

        return FilterAnd(parts)
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

        return FilterContainsChild(currentTable(), relImpl.info, MatchAnything, dstTable)
    }

    fun <TO: DbEntity<TO, *>> RelToMany<E, TO>.isEmpty(): FilterExpr {
        val dstTable = currentTable().forcedSubQuery(this)
        val relImpl = this as RelToManyImpl<E, *, TO>

        return FilterContainsChild(currentTable(), relImpl.info, MatchAnything, dstTable, negated = true)
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

    infix fun Column<E, Int>.hasAnyOfBits(bits: Int): FilterExpr {
        return FilterBitwise(this.bindForSelect(currentTable()), FilterBitwise.Op.HAS_ANY_BITS, makeLiteral(bits))
    }

    infix fun Column<E, Long>.hasAnyOfBits(bits: Long): FilterExpr {
        return FilterBitwise(this.bindForSelect(currentTable()), FilterBitwise.Op.HAS_ANY_BITS, makeLiteral(bits))
    }
}