package com.xs0.dbktx

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

    val targetTable: DbTable<TO, TOID>
        get() = info.oneTable

    override fun has(relatedProperty: Expr<TO, Boolean>): ExprBoolean<FROM> {
        return ExprFilterHasParent(info, relatedProperty)
    }

//    override // "same erasure, neither overrides the other..."
//    fun has(relatedProperty: Expr<*, *>): ExprBoolean<FROM> {

//    }
}
