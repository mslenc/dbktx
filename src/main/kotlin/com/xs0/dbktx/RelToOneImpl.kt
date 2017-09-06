package com.xs0.dbktx

import java.util.function.Function

class RelToOneImpl<FROM : DbEntity<FROM, FROMID>, FROMID : Any, TO : DbEntity<TO, TOID>, TOID : Any> : RelToOne<FROM, TO> {
    var info: ManyToOneInfo<FROM, FROMID, TO, TOID>? = null
        private set
    var idMapper: Function<FROM, TOID>? = null
        private set

    fun init(info: ManyToOneInfo<FROM, FROMID, TO, TOID>, idMapper: Function<FROM, TOID>) {
        this.info = info
        this.idMapper = idMapper
    }

    fun mapId(from: FROM): TOID {
        return idMapper!!.apply(from)
    }

    val targetTable: DbTable<TO, TOID>
        get() = info!!.oneTable

    override fun has(relatedProperty: Expr<TO, Boolean>): ExprBoolean<FROM> {
        return ExprFilterHasParent(info, relatedProperty)
    }

//    override // "same erasure, neither overrides the other..."
//    fun has(relatedProperty: Expr<*, *>): ExprBoolean<FROM> {

//    }
}
