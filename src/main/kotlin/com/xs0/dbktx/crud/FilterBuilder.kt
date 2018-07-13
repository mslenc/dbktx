package com.xs0.dbktx.crud

import com.xs0.dbktx.expr.*
import com.xs0.dbktx.schema.*
import com.xs0.dbktx.sqltypes.SqlTypeVarchar
import com.xs0.dbktx.util.escapeSqlLikePattern
import sun.reflect.generics.scope.DummyScope

@DslMarker
annotation class SqlExprBuilder

@SqlExprBuilder
interface FilterBuilder<E: DbEntity<E, *>> {
    fun currentTable(): TableInQuery<E>
    fun <T: Any> bind(prop: RowProp<E, T>): Expr<E, T>

    infix fun <T: Any> Expr<E, T>.eq(other: Expr<E, T>): ExprBoolean {
        return ExprBinary(this, ExprBinary.Op.EQ, other)
    }

    infix fun <T: Any> RowProp<E, T>.eq(other: T): ExprBoolean {
        return ExprBinary(bind(this), ExprBinary.Op.EQ, this.makeLiteral(other))
    }

    infix fun <T: Any> Expr<E, T>.neq(other: Expr<E, T>): ExprBoolean {
        return ExprBinary(this, ExprBinary.Op.NEQ, other)
    }

    infix fun <T: Any> RowProp<E, T>.neq(other: T): ExprBoolean {
        return ExprBinary(bind(this), ExprBinary.Op.NEQ, this.makeLiteral(other))
    }

    infix fun <T: Any> RowProp<E, T>.oneOf(values: Set<T>): ExprBoolean {
        return when {
            values.isEmpty() ->
                throw IllegalArgumentException("Can't have empty set with oneOf")
            values.size == 1 ->
                this.eq(values.first())
            else ->
                this.bindForSelect(currentTable()).oneOf(values.map { makeLiteral(it) })
        }
    }

    infix fun <T: Any> RowProp<E, T>.oneOf(values: Iterable<T>): ExprBoolean {
        return if (values is Set) {
            oneOf(values)
        } else {
            oneOf(values.toSet())
        }
    }

    infix fun <T> Expr<E, T>.lt(value: Expr<E, T>): ExprBoolean {
        return ExprBinary(this, ExprBinary.Op.LT, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lt(value: T): ExprBoolean {
        return ExprBinary(bind(this), ExprBinary.Op.LT, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lt(value: Expr<E, T>): ExprBoolean {
        return ExprBinary(bind(this), ExprBinary.Op.LT, value)
    }

    infix fun <T> Expr<E, T>.lte(value: Expr<E, T>): ExprBoolean {
        return ExprBinary(this, ExprBinary.Op.LTE, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lte(value: T): ExprBoolean {
        return ExprBinary(bind(this), ExprBinary.Op.LTE, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lte(value: Expr<E, T>): ExprBoolean {
        return ExprBinary(bind(this), ExprBinary.Op.LTE, value)
    }

    infix fun <T> Expr<E, T>.gt(value: Expr<E, T>): ExprBoolean {
        return ExprBinary(this, ExprBinary.Op.GT, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gt(value: T): ExprBoolean {
        return ExprBinary(bind(this), ExprBinary.Op.GT, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gt(value: Expr<E, T>): ExprBoolean {
        return ExprBinary(bind(this), ExprBinary.Op.GT, value)
    }

    infix fun <T> Expr<E, T>.gte(value: Expr<E, T>): ExprBoolean {
        return ExprBinary(this, ExprBinary.Op.GTE, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gte(value: T): ExprBoolean {
        return ExprBinary(bind(this), ExprBinary.Op.GTE, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gte(value: Expr<E, T>): ExprBoolean {
        return ExprBinary(bind(this), ExprBinary.Op.GTE, value)
    }

    fun <T : Comparable<T>> Expr<E, T>.between(minimum: Expr<E, T>, maximum: Expr<E, T>): ExprBoolean {
        return ExprBetween(this, minimum, maximum, between = true)
    }

    fun <T : Comparable<T>> Expr<E, T>.notBetween(minimum: Expr<E, T>, maximum: Expr<E, T>): ExprBoolean {
        return ExprBetween(this, minimum, maximum, between = false)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.between(range: ClosedRange<T>): ExprBoolean {
        if (range.isEmpty())
            throw IllegalArgumentException("Can't have an empty range")

        return bind(this).between(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun <T : Comparable<T>> OrderedProp<E, T>.between(minimum: T, maximum: T): ExprBoolean {
        return bind(this).between(makeLiteral(minimum), makeLiteral(maximum))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.notBetween(range: ClosedRange<T>): ExprBoolean {
        if (range.isEmpty())
            throw IllegalArgumentException("Can't have an empty range")

        return bind(this).notBetween(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun <T : Comparable<T>> OrderedProp<E, T>.notBetween(minimum: T, maximum: T): ExprBoolean {
        return bind(this).notBetween(makeLiteral(minimum), makeLiteral(maximum))
    }


    infix fun ExprString<E>.contains(value: String): ExprBoolean {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun ExprString<E>.startsWith(value: String): ExprBoolean {
        return like(escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun ExprString<E>.endsWith(value: String): ExprBoolean {
        return like("%" + escapeSqlLikePattern(value, '|'), '|')
    }

    infix fun ExprString<E>.like(pattern: String): ExprBoolean {
        return like(pattern, '|')
    }

    infix fun ExprString<E>.like(pattern: Expr<in E, String>): ExprBoolean {
        return like(pattern, '|')
    }

    fun ExprString<E>.like(pattern: String, escapeChar: Char): ExprBoolean {
        return like(SqlTypeVarchar.makeLiteral(pattern), escapeChar)
    }

    fun ExprString<E>.like(pattern: Expr<in E, String>, escapeChar: Char): ExprBoolean {
        return ExprLike(this, pattern, escapeChar)
    }


    infix fun StringColumn<E>.contains(value: String): ExprBoolean {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun StringColumn<E>.startsWith(value: String): ExprBoolean {
        return like(escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun StringColumn<E>.endsWith(value: String): ExprBoolean {
        return like("%" + escapeSqlLikePattern(value, '|'), '|')
    }

    infix fun StringColumn<E>.like(pattern: String): ExprBoolean {
        return like(pattern, '|')
    }

    infix fun StringColumn<E>.like(pattern: Expr<in E, String>): ExprBoolean {
        return like(pattern, '|')
    }

    fun StringColumn<E>.like(pattern: String, escapeChar: Char): ExprBoolean {
        return like(SqlTypeVarchar.makeLiteral(pattern), escapeChar)
    }

    fun StringColumn<E>.like(pattern: Expr<in E, String>, escapeChar: Char): ExprBoolean {
        return ExprLike(bind(this), pattern, escapeChar)
    }

    infix fun StringSetColumn<E>.contains(value: String): ExprBoolean {
        return ExprFindInSet(SqlTypeVarchar.makeLiteral(value), bindForSelect(currentTable()))
    }

    infix fun <T> Expr<E, T>.oneOf(values: List<Expr<E, T>>): ExprBoolean {
        if (values.isEmpty())
            throw IllegalArgumentException("No possibilities specified")

        return ExprOneOf.oneOf(this, values)
    }

    infix fun ExprBoolean.and(other: ExprBoolean): ExprBoolean {
        return ExprBools.create(this, ExprBools.Op.AND, other)
    }

    infix fun ExprBoolean.or(other: ExprBoolean): ExprBoolean {
        return ExprBools.create(this, ExprBools.Op.OR, other)
    }

    fun <T> NOW(): ExprNow<E, T> {
        return ExprNow()
    }

    fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.has(block: FilterBuilder<TO>.() -> ExprBoolean): ExprBoolean {
        val dstTable = currentTable().subQueryOrJoin(this)
        val dstFilter = TableInQueryBoundFilterBuilder(dstTable)
        val parentFilter = dstFilter.block()

        return ExprFilterHasParent((this as RelToOneImpl<E, TO, *>).info, parentFilter, currentTable(), dstTable)
    }

    infix fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.oneOf(parentFilter: EntityQuery<TO>): ExprBoolean {
        parentFilter as EntityQueryImpl<TO>

        if (parentFilter.filters == null)
            return this.isNotNull

        val dstTable = currentTable().subQueryOrJoin(this)
        val remappedFilter = parentFilter.copyAndRemapFilters(dstTable)

        return ExprFilterHasParent((this as RelToOneImpl<E, TO, *>).info, remappedFilter!!, currentTable(), dstTable)
    }

    val NullableRowProp<E, *>.isNull: ExprBoolean
        get() {
            return this.makeIsNullExpr(currentTable(), isNull = true)
        }

    val NullableRowProp<E, *>.isNotNull: ExprBoolean
        get() {
            return this.makeIsNullExpr(currentTable(), isNull = false)
        }

    val <TO : DbEntity<TO, *>> RelToOne<E, TO>.isNull: ExprBoolean
        get() {
            // a multi-column reference is null if any of its parts are null, because we only allow references to non-null columns..

            val rel = this as RelToOneImpl<E, TO, *>
            val parts = ArrayList<ExprBoolean>()

            rel.info.columnMappings.forEach { colMap ->
                colMap.columnFromAsNullable?.let { column ->
                    parts.add(column.makeIsNullExpr(currentTable(), isNull = true))
                }
            }

            if (parts.isEmpty())
                throw IllegalStateException("isNull used on a reference where nothing can be null")

            return ExprBoolean.createOR(parts)
        }

    val <TO : DbEntity<TO, *>> RelToOne<E, TO>.isNotNull: ExprBoolean
        get() {
            return !isNull
        }

    infix fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.eq(ref: TO): ExprBoolean {
        this as RelToOneImpl<E, TO, *>

        val colMappings = this.info.columnMappings

        val parts = ArrayList<ExprBoolean>()

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
                    if (fakeEqRef is ExprDummy) {
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

        return ExprBools(parts, ExprBools.Op.AND)
    }

    infix fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.oneOf(refs: Iterable<TO>): ExprBoolean {
        this as RelToOneImpl<E, TO, *>

        val refList = refs as? List ?: refs.toList()

        return when {
            refList.isEmpty() ->
                throw IllegalArgumentException("No choices provided to oneOf")

            refList.size == 1 ->
                eq(refList.first())

            else ->
                RelToOneOneOf(currentTable(), info, refList)
        }
    }


    fun <TO: DbEntity<TO, *>> RelToMany<E, TO>.contains(block: FilterBuilder<TO>.() -> ExprBoolean): ExprBoolean {
        val dstTable = currentTable().forcedSubQuery(this)
        val dstFilter = TableInQueryBoundFilterBuilder(dstTable)
        val setFilter = dstFilter.block()
        val relImpl = this as RelToManyImpl<E, *, TO>

        return ExprFilterContainsChild(currentTable(), relImpl.info, setFilter, dstTable)
    }

    infix fun <TO: DbEntity<TO, *>> RelToMany<E, TO>.contains(childQuery: EntityQuery<TO>): ExprBoolean {
        val relImpl = this as RelToManyImpl<E, *, TO>
        val dstTable = currentTable().forcedSubQuery(this)
        val dstFilter = childQuery.copyAndRemapFilters(dstTable) ?: ExprDummy(true)

        return ExprFilterContainsChild(currentTable(), relImpl.info, dstFilter, dstTable)
    }

    fun <T> combineWithOR(values: Iterable<T>, map: FilterBuilder<E>.(T)->ExprBoolean): ExprBoolean {
        var parts = values.map { map(it) }
        return ExprBools.create(ExprBools.Op.OR, parts)
    }

    infix fun Column<E, Int>.hasAnyOfBits(bits: Int): ExprBoolean {
        return ExprBitwise(this.bindForSelect(currentTable()), ExprBitwise.Op.HAS_ANY_BITS, makeLiteral(bits))
    }

    infix fun Column<E, Long>.hasAnyOfBits(bits: Long): ExprBoolean {
        return ExprBitwise(this.bindForSelect(currentTable()), ExprBitwise.Op.HAS_ANY_BITS, makeLiteral(bits))
    }
}