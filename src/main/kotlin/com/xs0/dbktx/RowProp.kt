package com.xs0.dbktx

interface RowProp<E : DbEntity<E, *>, T> : Expr<E, T> {
    fun from(row: List<Any?>): T?

    val isAutoGenerated: Boolean
    fun extract(values: EntityValues<E>): T?
    fun makeLiteral(value: T): Expr<E, T>

    infix fun eq(value: T): ExprBoolean<E> = eq(makeLiteral(value))
    infix fun neq(value: T): ExprBoolean<E> = neq(makeLiteral(value))

    infix fun oneOf(values: Set<T>): ExprBoolean<E> = oneOf(values.map { makeLiteral(it) })

    infix fun oneOf(values: Iterable<T>): ExprBoolean<E> {
        return if (values is Set) {
            oneOf(values)
        } else {
            oneOf(values.toSet())
        }
    }
}

interface NullableRowProp<E: DbEntity<E, *>, T> : RowProp<E, T>, NullableExpr<E, T>

interface NonNullRowProp<E: DbEntity<E, *>, T> : RowProp<E, T> {
    override fun from(row: List<Any?>): T
}

interface OrderedProp<E : DbEntity<E, *>, T : Comparable<T>> : RowProp<E, T>, OrderedExpr<E, T> {
    infix fun lt(value: T): ExprBoolean<E> = lt(makeLiteral(value))
    infix fun lte(value: T): ExprBoolean<E> = lte(makeLiteral(value))
    infix fun gt(value: T): ExprBoolean<E> = gt(makeLiteral(value))
    infix fun gte(value: T): ExprBoolean<E> = gte(makeLiteral(value))

    infix fun between(range: ClosedRange<T>): ExprBoolean<E> {
        if (range.isEmpty())
            throw IllegalArgumentException("Can't have an empty range")

        return between(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun between(minimum: T, maximum: T): ExprBoolean<E> {
        return between(makeLiteral(minimum), makeLiteral(maximum))
    }

    infix fun notBetween(range: ClosedRange<T>): ExprBoolean<E> {
        if (range.isEmpty())
            throw IllegalArgumentException("Can't have an empty range")

        return notBetween(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun notBetween(minimum: T, maximum: T): ExprBoolean<E> {
        return notBetween(makeLiteral(minimum), makeLiteral(maximum))
    }
}

interface NullableOrderedProp<E: DbEntity<E, *>, T : Comparable<T>> : OrderedProp<E, T>, NullableOrderedExpr<E, T>
interface NonNullOrderedProp<E: DbEntity<E, *>, T : Comparable<T>> : OrderedProp<E, T>, NonNullOrderedExpr<E, T>