package com.xs0.dbktx.crud

import com.xs0.dbktx.expr.*
import com.xs0.dbktx.schema.*
import com.xs0.dbktx.sqltypes.SqlTypeVarchar
import com.xs0.dbktx.util.escapeSqlLikePattern

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
                oneOf(values.map { makeLiteral(it) })
        }
    }

    infix fun <T> oneOf(values: Iterable<T>): ExprBoolean {
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

    infix fun <T> Expr<E, T>.oneOf(values: List<Expr<E, T>>): ExprBoolean {
        if (values.isEmpty())
            throw IllegalArgumentException("No possibilities specified")

        return ExprOneOf.oneOf(this, values)
    }

    operator fun ExprBoolean.not(): ExprBoolean {
        return ExprNegate(this)
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

        return ExprFilterHasParent((this as RelToOneImpl<E, *, TO, *>).info, parentFilter, currentTable(), dstTable)
    }

    infix fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.eq(ref: TO): ExprBoolean {
        this as RelToOneImpl<E, *, TO, *>

        val colMappings = this.info.columnMappings

        return if (colMappings.size == 1) {
            RelToOneImpl.makeEq(colMappings[0], ref, currentTable())
        } else {
            ExprBools(colMappings.map { RelToOneImpl.makeEq(it, ref, currentTable()) }.toList(), ExprBools.Op.AND)
        }
    }

    infix fun <TO : DbEntity<TO, *>> RelToOne<E, TO>.oneOf(refs: Iterable<TO>): ExprBoolean {
        this as RelToOneImpl<E, *, TO, *>

        val colMappings = this.info.columnMappings
        val refList = refs as? List ?: refs.toList()

        return when {
            refList.isEmpty() ->
                throw IllegalArgumentException("No choices provided to oneOf")

            refList.size == 1 ->
                eq(refList.first())

            colMappings.size == 1 ->
                RelToOneImpl.makeOneOf(colMappings[0], refList, currentTable())

            else ->
                MultiColOneOf(currentTable(), info, refList)
        }
    }


    fun <TO: DbEntity<TO, *>> RelToMany<E, TO>.contains(block: FilterBuilder<TO>.() -> ExprBoolean): ExprBoolean {
        val dstTable = currentTable().forcedSubQuery(this)
        val dstFilter = TableInQueryBoundFilterBuilder(dstTable)
        val setFilter = dstFilter.block()
        val relImpl = this as RelToManyImpl<E, *, TO, *>

        return ExprFilterContainsChild(currentTable(), relImpl.info, setFilter, dstTable)
    }
}