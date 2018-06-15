package com.xs0.dbktx.schema

import com.xs0.dbktx.expr.ExprBoolean

interface RelToOne<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>> {
    fun has(relatedProperty: ExprBoolean<TO>): ExprBoolean<FROM>

    fun <Z : DbTable<TO, *>>
    has(table: Z, filter: Z.()->ExprBoolean<TO>): ExprBoolean<FROM> {
        return has(table.filter())
    }

    infix fun eq(ref: TO): ExprBoolean<FROM>
    infix fun `==`(ref: TO): ExprBoolean<FROM> = eq(ref)

    infix fun oneOf(refs: Iterable<TO>): ExprBoolean<FROM>

    suspend operator fun invoke(from: FROM): TO?

    val targetTable: DbTable<TO, *>
}