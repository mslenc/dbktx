package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.*
import com.github.mslenc.dbktx.filters.*
import com.github.mslenc.dbktx.filters.FilterAnd
import com.github.mslenc.dbktx.filters.FilterBitwise
import com.github.mslenc.dbktx.filters.FilterCompare
import com.github.mslenc.dbktx.filters.FilterOr
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.sqltypes.SqlTypeDouble
import com.github.mslenc.dbktx.sqltypes.SqlTypeLong
import com.github.mslenc.dbktx.sqltypes.SqlTypeVarchar
import com.github.mslenc.dbktx.util.escapeSqlLikePattern

class RelPath<BASE: DbEntity<BASE, *>, LAST: DbEntity<LAST, *>>(
    internal val table: TableInQuery<LAST>
)

@DslMarker
annotation class SqlExprBuilder

@SqlExprBuilder
interface ExprBuilderBase<E: DbEntity<E, *>> {
    val table: TableInQuery<E>
    fun <T: Any> bind(prop: RowProp<E, T>): Expr<T> = prop.bindForSelect(table)
    operator fun <T: Any> Column<E, T>.unaryPlus() = bind(this)
}

@SqlExprBuilder
interface ExprBuilder<E: DbEntity<E, *>> : ExprBuilderBase<E> {
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
                bind(this).oneOf(values.map { makeLiteral(it) })
        }
    }

    infix fun <T: Any> NullableRowProp<E, T>.oneOf(values: Set<T?>): Expr<Boolean> {
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
                    bind(this).oneOf(values.map { makeLiteral(it) })
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

    infix fun <T: Any> NullableRowProp<E, T>.oneOf(values: Iterable<T?>): Expr<Boolean> {
        return if (values is Set) {
            oneOf(values)
        } else {
            oneOf(values.toSet())
        }
    }


    infix fun <T : Any> Expr<T>.lt(value: Expr<T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.LT, value)
    }

    infix fun <T : Comparable<T>> RowProp<E, T>.lt(value: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.LT, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> RowProp<E, T>.lt(value: Expr<T>): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.LT, value)
    }

    infix fun <T : Any> Expr<T>.lte(value: Expr<T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.LTE, value)
    }

    infix fun <T : Comparable<T>> RowProp<E, T>.lte(value: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.LTE, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> RowProp<E, T>.lte(value: Expr<T>): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.LTE, value)
    }

    infix fun <T : Any> Expr<T>.gt(value: Expr<T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.GT, value)
    }

    infix fun <T : Comparable<T>> RowProp<E, T>.gt(value: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.GT, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> RowProp<E, T>.gt(value: Expr<T>): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.GT, value)
    }

    infix fun <T : Any> Expr<T>.gte(value: Expr<T>): FilterExpr {
        return FilterCompare(this, FilterCompare.Op.GTE, value)
    }

    infix fun <T : Comparable<T>> RowProp<E, T>.gte(value: T): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.GTE, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> RowProp<E, T>.gte(value: Expr<T>): FilterExpr {
        return FilterCompare(bind(this), FilterCompare.Op.GTE, value)
    }

    fun <T : Comparable<T>> Expr<T>.between(minimum: Expr<T>, maximum: Expr<T>): FilterExpr {
        return FilterBetween(this, minimum, maximum, between = true)
    }

    fun <T : Comparable<T>> Expr<T>.notBetween(minimum: Expr<T>, maximum: Expr<T>): FilterExpr {
        return FilterBetween(this, minimum, maximum, between = false)
    }

    infix fun <T : Comparable<T>> Expr<T>.within(range: SqlRange<T>): FilterExpr {
        return FilterBetween(this, range.minumum, range.maximum, between = true)
    }

    infix fun <T : Comparable<T>> Expr<T>.notWithin(range: SqlRange<T>): FilterExpr {
        return FilterBetween(this, range.minumum, range.maximum, between = false)
    }

    infix fun <T : Comparable<T>> RowProp<E, T>.within(range: ClosedRange<T>): FilterExpr {
        if (range.isEmpty())
            return MatchNothing

        return bind(this).between(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun <T : Comparable<T>> RowProp<E, T>.between(minimum: T, maximum: T): FilterExpr {
        return bind(this).between(makeLiteral(minimum), makeLiteral(maximum))
    }

    fun <T : Comparable<T>> RowProp<E, T>.betweenOpt(minimum: T?, maximum: T?): FilterExpr {
        return when {
            minimum != null && maximum != null -> between(minimum, maximum)
            minimum != null -> this gte minimum
            maximum != null -> this lte maximum
            else -> MatchAnything
        }
    }

    infix fun <T : Comparable<T>> RowProp<E, T>.notWithin(range: ClosedRange<T>): FilterExpr {
        if (range.isEmpty())
            return MatchAnything

        return bind(this).notBetween(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun <T : Comparable<T>> RowProp<E, T>.notBetween(minimum: T, maximum: T): FilterExpr {
        return bind(this).notBetween(makeLiteral(minimum), makeLiteral(maximum))
    }

    fun <T : Comparable<T>> RowProp<E, T>.notBetweenOpt(minimum: T?, maximum: T?): Expr<Boolean> {
        return !betweenOpt(minimum, maximum)
    }

    infix fun Expr<String>.contains(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun Expr<String>.icontains(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|', caseInsensitive = true)
    }

    infix fun Expr<String>.startsWith(value: String): FilterExpr {
        return like(escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun Expr<String>.istartsWith(value: String): FilterExpr {
        return like(escapeSqlLikePattern(value, '|') + "%", '|', caseInsensitive = true)
    }

    infix fun Expr<String>.endsWith(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|'), '|')
    }

    infix fun Expr<String>.iendsWith(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|'), '|', caseInsensitive = true)
    }

    infix fun Expr<String>.like(pattern: String): FilterExpr {
        return like(pattern, '|')
    }

    infix fun Expr<String>.ilike(pattern: String): FilterExpr {
        return like(pattern, '|', caseInsensitive = true)
    }

    infix fun Expr<String>.like(pattern: Expr<String>): FilterExpr {
        return like(pattern, '|')
    }

    infix fun Expr<String>.ilike(pattern: Expr<String>): FilterExpr {
        return like(pattern, '|', caseInsensitive = true)
    }

    fun Expr<String>.like(pattern: String, escapeChar: Char, caseInsensitive: Boolean = false): FilterExpr {
        return like(SqlTypeVarchar.makeLiteral(pattern), escapeChar, caseInsensitive = caseInsensitive)
    }

    fun Expr<String>.like(pattern: Expr<String>, escapeChar: Char, caseInsensitive: Boolean = false): FilterExpr {
        return FilterLike(this, pattern, escapeChar)
    }


    infix fun Column<E, String>.contains(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun Column<E, String>.icontains(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|', caseInsensitive = true)
    }

    infix fun Column<E, String>.startsWith(value: String): FilterExpr {
        return like(escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun Column<E, String>.istartsWith(value: String): FilterExpr {
        return like(escapeSqlLikePattern(value, '|') + "%", '|', caseInsensitive = true)
    }

    infix fun Column<E, String>.endsWith(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|'), '|')
    }

    infix fun Column<E, String>.iendsWith(value: String): FilterExpr {
        return like("%" + escapeSqlLikePattern(value, '|'), '|', caseInsensitive = true)
    }

    infix fun Column<E, String>.like(pattern: String): FilterExpr {
        return like(pattern, '|')
    }

    infix fun Column<E, String>.ilike(pattern: String): FilterExpr {
        return like(pattern, '|', caseInsensitive = true)
    }

    infix fun Column<E, String>.like(pattern: Expr<String>): FilterExpr {
        return like(pattern, '|')
    }

    infix fun Column<E, String>.ilike(pattern: Expr<String>): FilterExpr {
        return like(pattern, '|', caseInsensitive = true)
    }

    fun Column<E, String>.like(pattern: String, escapeChar: Char, caseInsensitive: Boolean = false): FilterExpr {
        return like(SqlTypeVarchar.makeLiteral(pattern), escapeChar, caseInsensitive = caseInsensitive)
    }

    fun Column<E, String>.like(pattern: Expr<String>, escapeChar: Char, caseInsensitive: Boolean = false): FilterExpr {
        return FilterLike(bind(this), pattern, escapeChar, caseInsensitive = caseInsensitive)
    }

    infix fun <T : Any> Expr<T>.oneOf(values: List<Expr<T>>): FilterExpr {
        return FilterOneOf.oneOf(this, values)
    }

    fun <T : Any> NOW(): ExprNow<T> {
        return ExprNow()
    }

    fun <TO: DbEntity<TO, *>> RelToZeroOrOne<E, TO>.has(block: ExprBuilder<TO>.() -> Expr<Boolean>): FilterExpr {
        val dstTable = table.subQueryOrJoin(this)
        val dstFilter = dstTable.newExprBuilder()

        return when (val setFilter = dstFilter.block()) {
            MatchNothing -> MatchNothing
            else -> FilterHasAssociated(table, (this as RelToZeroOrOneImpl<E, *, TO>).info, setFilter, dstTable)
        }
    }

    fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.has(block: ExprBuilder<TO>.() -> Expr<Boolean>): Expr<Boolean> {
        val dstTable = table.subQueryOrJoin(this)
        val dstFilter = dstTable.newExprBuilder()

        return when (val parentFilter = dstFilter.block()) {
            MatchAnything -> this.isNotNull()
            MatchNothing -> MatchNothing
            else -> FilterHasParent((this as RelToOneImpl<E, TO, *>).info, parentFilter, table, dstTable)
        }
    }

    fun <TO : DbEntity<TO, *>> RelToSingle<E, TO>.has(block: ExprBuilder<TO>.() -> Expr<Boolean>): Expr<Boolean> {
        return when (this) {
            is RelToOne -> this.has(block)
            is RelToZeroOrOne -> this.has(block)
            else -> throw IllegalStateException()
        }
    }

    fun <TO : DbEntity<TO, *>> Rel<E, TO>.matches(block: ExprBuilder<TO>.() -> Expr<Boolean>): Expr<Boolean> {
        return when (this) {
            is RelToOne -> this.has(block)
            is RelToMany -> this.contains(block)
            is RelToZeroOrOne -> this.has(block)
            else -> throw IllegalStateException()
        }
    }

    infix fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.oneOf(parentFilter: EntityQuery<TO>): Expr<Boolean> {
        return when (parentFilter.filteringState()) {
            FilteringState.MATCH_ALL -> this.isNotNull()
            FilteringState.MATCH_NONE -> MatchNothing
            FilteringState.MATCH_SOME -> {
                val dstTable = table.subQueryOrJoin(this)
                val remappedFilter = parentFilter.copyAndRemapFilters(dstTable)

                FilterHasParent((this as RelToOneImpl<E, TO, *>).info, remappedFilter, table, dstTable)
            }
        }
    }

    infix fun <TO : DbEntity<TO, *>> RelToZeroOrOne<E, TO>.oneOf(parentFilter: EntityQuery<TO>): FilterExpr {
        return when (parentFilter.filteringState()) {
            FilteringState.MATCH_NONE -> MatchNothing
            else -> {
                val dstTable = table.subQueryOrJoin(this)
                val remappedFilter = parentFilter.copyAndRemapFilters(dstTable)

                FilterHasAssociated(table, (this as RelToZeroOrOneImpl<E, *, TO>).info, remappedFilter, dstTable)
            }
        }
    }

    infix fun <TO : DbEntity<TO, *>> RelToSingle<E, TO>.oneOf(parentFilter: EntityQuery<TO>): Expr<Boolean> {
        return when (this) {
            is RelToOne -> oneOf(parentFilter)
            is RelToZeroOrOne -> oneOf(parentFilter)
            else -> throw IllegalStateException()
        }
    }

    fun NullableRowProp<E, *>.isNull(): FilterExpr = this.makeIsNullExpr(table, isNull = true)

    fun NullableRowProp<E, *>.isNotNull(): FilterExpr = this.makeIsNullExpr(table, isNull = false)

    fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.isNull(): Expr<Boolean> {
        // a multi-column reference is null if any of its parts are null, because we only allow references to non-null columns..

        val rel = this as RelToOneImpl<E, TO, *>
        val parts = ArrayList<FilterExpr>()

        rel.info.columnMappings.forEach { colMap ->
            colMap.columnFromAsNullable?.let { column ->
                parts.add(column.makeIsNullExpr(table, isNull = true))
            }
        }

        return FilterOr.create(parts)
    }

    fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.isNotNull(): Expr<Boolean> = !isNull()

    fun <TO : DbEntity<TO, *>> RelToZeroOrOne<E, TO>.isNotNull(): FilterExpr {
        return has { MatchAnything }
    }

    fun <TO : DbEntity<TO, *>> RelToZeroOrOne<E, TO>.isNull(): Expr<Boolean> {
        return !isNotNull()
    }

    fun <TO : DbEntity<TO, *>> RelToSingle<E, TO>.isNotNull(): Expr<Boolean> {
        return when (this) {
            is RelToOne -> isNotNull()
            is RelToZeroOrOne -> isNotNull()
            else -> throw IllegalStateException()
        }
    }

    fun <TO : DbEntity<TO, *>> RelToSingle<E, TO>.isNull(): Expr<Boolean> {
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
                    parts.add(colMap.makeEqRef(ref, table))
                }

                ColumnInMappingKind.CONSTANT,
                ColumnInMappingKind.PARAMETER -> {
                    // here, we have a constant on the from side, and also a constant on the to side..

                    when (colMap.makeEqRef(ref, table)) {
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
                MatchNothing

            refList.size == 1 ->
                eq(refList.first())

            else ->
                FilterOneOfRelToOne(table, info, refList)
        }
    }

    fun <TO: DbEntity<TO, *>> RelToMany<E, TO>.isNotEmpty(): FilterExpr {
        val dstTable = table.forcedSubQuery(this)
        val relImpl = this as RelToManyImpl<E, *, TO>

        return FilterContainsChild(table, relImpl.info, MatchAnything, dstTable)
    }

    fun <TO: DbEntity<TO, *>> RelToMany<E, TO>.isEmpty(): FilterExpr {
        val dstTable = table.forcedSubQuery(this)
        val relImpl = this as RelToManyImpl<E, *, TO>

        return FilterContainsChild(table, relImpl.info, MatchAnything, dstTable, negated = true)
    }

    fun <TO: DbEntity<TO, *>> RelToMany<E, TO>.contains(block: ExprBuilder<TO>.() -> Expr<Boolean>): Expr<Boolean> {
        val dstTable = table.forcedSubQuery(this)
        val dstFilter = dstTable.newExprBuilder()
        val setFilter = dstFilter.block()
        val relImpl = this as RelToManyImpl<E, *, TO>

        return when (setFilter) {
            is MatchNothing -> MatchNothing
            else -> FilterContainsChild(table, relImpl.info, setFilter, dstTable)
        }
    }

    infix fun <TO: DbEntity<TO, *>> RelToMany<E, TO>.contains(childQuery: EntityQuery<TO>): FilterExpr {
        val relImpl = this as RelToManyImpl<E, *, TO>
        val dstTable = table.forcedSubQuery(this)
        val dstFilter = childQuery.copyAndRemapFilters(dstTable)

        return when (dstFilter) {
            is MatchNothing -> MatchNothing
            else -> FilterContainsChild(table, relImpl.info, dstFilter, dstTable)
        }
    }

    infix fun Column<E, Int>.hasAnyOfBits(bits: Int): FilterExpr {
        return FilterBitwise(bind(this), FilterBitwise.Op.HAS_ANY_BITS, makeLiteral(bits))
    }

    infix fun Column<E, Long>.hasAnyOfBits(bits: Long): FilterExpr {
        return FilterBitwise(bind(this), FilterBitwise.Op.HAS_ANY_BITS, makeLiteral(bits))
    }

    operator fun <MID: DbEntity<MID, *>, T: Any>
    RelToSingle<E, MID>.rangeTo(column: Column<MID, T>): Expr<T> {
        return column.bindForSelect(table.innerJoin(this))
    }

    operator fun <MID: DbEntity<MID, *>, T: Any>
    RelToMany<E, MID>.rangeTo(column: Column<MID, T>): Expr<T> {
        return column.bindForSelect(table.innerJoin(this))
    }

    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>>
    RelToSingle<E, MID>.rangeTo(rel: RelToSingle<MID, NEXT>): RelPath<E, NEXT> {
        return RelPath(table.innerJoin(this).innerJoin(rel))
    }

    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>>
    RelToSingle<E, MID>.rangeTo(rel: RelToMany<MID, NEXT>): RelPath<E, NEXT> {
        return RelPath(table.innerJoin(this).innerJoin(rel))
    }

    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>>
    RelToMany<E, MID>.rangeTo(rel: RelToSingle<MID, NEXT>): RelPath<E, NEXT> {
        return RelPath(table.innerJoin(this).innerJoin(rel))
    }

    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>>
    RelToMany<E, MID>.rangeTo(rel: RelToMany<MID, NEXT>): RelPath<E, NEXT> {
        return RelPath(table.innerJoin(this).innerJoin(rel))
    }

    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>>
    RelPath<E, MID>.rangeTo(rel: RelToOne<MID, NEXT>): RelPath<E, NEXT> {
        return RelPath(table.innerJoin(rel))
    }

    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>>
    RelPath<E, MID>.rangeTo(rel: RelToMany<MID, NEXT>): RelPath<E, NEXT> {
        return RelPath(table.innerJoin(rel))
    }

    operator fun <MID: DbEntity<MID, *>, T: Any>
    RelPath<E, MID>.rangeTo(column: Column<MID, T>): Expr<T> {
        return column.bindForSelect(table)
    }



    fun <T: Any> binary(left: Expr<T>, op: BinaryOp, right: Expr<T>): Expr<T> {
        return ExprBinary(left, op, right)
    }

    operator fun <T: Any> Expr<T>.  plus(other: Expr<T>  ) = binary(this, BinaryOp.PLUS, other)
    operator fun <T: Any> Expr<T>.  plus(other: Column<E, T>) = this + +other
    operator fun <T: Any> Expr<T>.  plus(other: T           ) = this + makeLiteral(other)
    operator fun <T: Any> Column<E, T>.plus(other: Expr<T>  ) = +this + other
    operator fun <T: Any> Column<E, T>.plus(other: Column<E, T>) = +this + +other
    operator fun <T: Any> Column<E, T>.plus(other: T           ) = +this + makeLiteral(other)
    operator fun <T: Any> T.           plus(other: Expr<T>  ) = other.makeLiteral(this) + other
    operator fun <T: Any> T.           plus(other: Column<E, T>) = other.makeLiteral(this) + other

    operator fun <T: Any> Expr<T>.  minus(other: Expr<T>  ) = binary(this, BinaryOp.MINUS, other)
    operator fun <T: Any> Expr<T>.  minus(other: Column<E, T>) = this - +other
    operator fun <T: Any> Expr<T>.  minus(other: T           ) = this - makeLiteral(other)
    operator fun <T: Any> Column<E, T>.minus(other: Expr<T>  ) = +this - other
    operator fun <T: Any> Column<E, T>.minus(other: Column<E, T>) = +this - +other
    operator fun <T: Any> Column<E, T>.minus(other: T           ) = +this - makeLiteral(other)
    operator fun <T: Any> T.           minus(other: Expr<T>  ) = other.makeLiteral(this) - other
    operator fun <T: Any> T.           minus(other: Column<E, T>) = other.makeLiteral(this) - other

    operator fun <T: Any> Expr<T>.  times(other: Expr<T>  ) = binary(this, BinaryOp.TIMES, other)
    operator fun <T: Any> Expr<T>.  times(other: Column<E, T>) = this * +other
    operator fun <T: Any> Expr<T>.  times(other: T           ) = this * makeLiteral(other)
    operator fun <T: Any> Column<E, T>.times(other: Expr<T>  ) = +this * other
    operator fun <T: Any> Column<E, T>.times(other: Column<E, T>) = +this * +other
    operator fun <T: Any> Column<E, T>.times(other: T           ) = +this * makeLiteral(other)
    operator fun <T: Any> T.           times(other: Expr<T>  ) = other.makeLiteral(this) * other
    operator fun <T: Any> T.           times(other: Column<E, T>) = other.makeLiteral(this) * other

    operator fun <T: Any> Expr<T>.  div(other: Expr<T>  ) = binary(this, BinaryOp.DIV, other)
    operator fun <T: Any> Expr<T>.  div(other: Column<E, T>) = this / +other
    operator fun <T: Any> Expr<T>.  div(other: T           ) = this / makeLiteral(other)
    operator fun <T: Any> Column<E, T>.div(other: Expr<T>  ) = +this / other
    operator fun <T: Any> Column<E, T>.div(other: Column<E, T>) = +this / +other
    operator fun <T: Any> Column<E, T>.div(other: T           ) = +this / makeLiteral(other)
    operator fun <T: Any> T.           div(other: Expr<T>  ) = other.makeLiteral(this) / other
    operator fun <T: Any> T.           div(other: Column<E, T>) = other.makeLiteral(this) / other

    operator fun <T: Any> Expr<T>.  rem(other: Expr<T>  ) = binary(this, BinaryOp.REM, other)
    operator fun <T: Any> Expr<T>.  rem(other: Column<E, T>) = this % +other
    operator fun <T: Any> Expr<T>.  rem(other: T           ) = this % makeLiteral(other)
    operator fun <T: Any> Column<E, T>.rem(other: Expr<T>  ) = +this % other
    operator fun <T: Any> Column<E, T>.rem(other: Column<E, T>) = +this % +other
    operator fun <T: Any> Column<E, T>.rem(other: T           ) = +this % makeLiteral(other)
    operator fun <T: Any> T.           rem(other: Expr<T>  ) = other.makeLiteral(this) % other
    operator fun <T: Any> T.           rem(other: Column<E, T>) = other.makeLiteral(this) % other

    fun <T: Any> coalesce(vararg options: Expr<T>, ifAllNull: T? = null): Expr<T> {
        return ExprCoalesce.create(options.toList(), ifAllNull)
    }

    fun <T: Number> Expr<T>.orZero(): Expr<T> = coalesce(this, Literal(this.sqlType.zeroValue, this.sqlType))
    fun <T: Number> NullableColumn<E, T>.orZero(): Expr<T> = coalesce(bind(this), this.makeLiteral(this.sqlType.zeroValue))
}

// in some places we allow any type of expression; in some places we only allow scalar expressions (e.g. within aggregate
// functions, in WHERE clauses or when querying entities)


@SqlExprBuilder
interface AggrExprBuilder<E: DbEntity<E, *>> : ExprBuilder<E> {

    fun <T: Any> sum(block: ExprBuilder<E>.() -> Expr<T>): NullableAggrExpr<T> {
        val inner = table.newExprBuilder().block()
        return AggrExprImpl(AggrExprOp.SUM, inner, inner.sqlType)
    }

    fun <T: Comparable<T>> min(block: ExprBuilder<E>.() -> Expr<T>): NullableAggrExpr<T> {
        val inner = table.newExprBuilder().block()
        return AggrExprImpl(AggrExprOp.MIN, inner, inner.sqlType)
    }

    fun <T: Comparable<T>> max(block: ExprBuilder<E>.() -> Expr<T>): NullableAggrExpr<T> {
        val inner = table.newExprBuilder().block()
        return AggrExprImpl(AggrExprOp.MAX, inner, inner.sqlType)
    }

    fun <T: Number> average(block: ExprBuilder<E>.() -> Expr<T>): NullableAggrExpr<Double> {
        val inner = table.newExprBuilder().block()
        return AggrExprImpl(AggrExprOp.AVG, inner, SqlTypeDouble.INSTANCE_FOR_AVG)
    }

    fun <T: Any> count(block: ExprBuilder<E>.() -> Expr<T>): NonNullAggrExpr<Long> {
        val inner = table.newExprBuilder().block()
        return CountExpr(CountExprOp.COUNT, inner, SqlTypeLong.INSTANCE_FOR_COUNT)
    }

    fun <T: Any> countDistinct(block: ExprBuilder<E>.() -> Expr<T>): NonNullAggrExpr<Long> {
        val inner = table.newExprBuilder().block()
        return CountExpr(CountExprOp.COUNT_DISTINCT, inner, SqlTypeLong.INSTANCE_FOR_COUNT)
    }
}

fun <E: DbEntity<E, *>> TableInQuery<E>.newExprBuilder(): AggrExprBuilder<E> {
    return ExprBuilderImpl(this)
}

private class ExprBuilderImpl<E: DbEntity<E, *>>(override val table: TableInQuery<E>) : AggrExprBuilder<E>