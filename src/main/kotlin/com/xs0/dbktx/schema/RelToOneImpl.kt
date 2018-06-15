package com.xs0.dbktx.schema

import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.expr.ExprBools
import com.xs0.dbktx.expr.ExprFilterHasParent
import com.xs0.dbktx.expr.MultiColOneOf

class RelToOneImpl<FROM : DbEntity<FROM, FROMID>, FROMID : Any, TO : DbEntity<TO, TOID>, TOID : Any> : RelToOne<FROM, TO> {
    internal lateinit var info: ManyToOneInfo<FROM, FROMID, TO, TOID>
    internal lateinit var idMapper: (FROM)->TOID?

    fun init(info: ManyToOneInfo<FROM, FROMID, TO, TOID>, idMapper: (FROM)->TOID?) {
        this.info = info
        this.idMapper = idMapper
    }

    fun mapId(from: FROM): TOID? {
        return idMapper(from)
    }

    override val targetTable: DbTable<TO, TOID>
        get() = info.oneTable

    override fun has(relatedProperty: ExprBoolean<TO>): ExprBoolean<FROM> {
        return ExprFilterHasParent(info, relatedProperty)
    }

    override fun eq(ref: TO): ExprBoolean<FROM> {
        return if (info.columnMappings.size == 1) {
            makeEq(info.columnMappings[0], ref)
        } else {
            ExprBools(info.columnMappings.map { makeEq(it, ref) }.toList(), ExprBools.Op.AND)
        }
    }

    private inline fun <T: Any> makeEq(map: ColumnMapping<FROM, TO, T>, ref: TO): ExprBoolean<FROM> {
        return map.columnFrom eq map.columnTo(ref)
    }

    override fun oneOf(refs: Iterable<TO>): ExprBoolean<FROM> {
        val refList = refs as? List ?: refs.toList()

        return when {
            refList.isEmpty() ->
                throw IllegalArgumentException("No choices provided to oneOf")

            refList.size == 1 ->
                eq(refList.first())

            info.columnMappings.size == 1 ->
                makeOneOf(info.columnMappings[0], refList)

            else ->
                MultiColOneOf(info, refList)
        }
    }

    private fun <T: Any> makeOneOf(map: ColumnMapping<FROM, TO, T>, refs: List<TO>): ExprBoolean<FROM> {
        val set = LinkedHashSet<T>(refs.size)
        refs.mapTo(set) { map.columnTo(it) }

        return map.columnFrom oneOf set
    }

    override suspend fun invoke(from: FROM): TO? {
        return from.db.find(from, this)
    }
}
