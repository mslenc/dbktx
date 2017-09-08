package com.xs0.dbktx.schema

import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.expr.ExprFilterContainsChild

class RelToManyImpl<FROM : DbEntity<FROM, FID>, FID: Any, TO : DbEntity<TO, TID>, TID: Any> : RelToMany<FROM, TO> {

    private lateinit var info: ManyToOneInfo<TO, TID, FROM, FID>
    private lateinit var reverseIdMapper: (TO)->FID?
    private lateinit var queryExprBuilder: (Set<FID>)-> ExprBoolean<TO>

    fun init(info: ManyToOneInfo<TO, TID, FROM, FID>, reverseIdMapper: (TO)->FID?, queryExprBuilder: (Set<FID>)-> ExprBoolean<TO>) {
        this.info = info
        this.reverseIdMapper = reverseIdMapper
        this.queryExprBuilder = queryExprBuilder
    }

    fun reverseMap(to: TO): FID? {
        return reverseIdMapper(to)
    }

    val sourceTable: DbTable<FROM, FID>
        get() = info.oneTable

    val targetTable: DbTable<TO, TID>
        get() = info.manyTable

    fun createCondition(fromIds: Set<FID>): ExprBoolean<TO> {
        return queryExprBuilder(fromIds)
    }

    override fun contains(setFilter: ExprBoolean<TO>): ExprBoolean<FROM> {
        return ExprFilterContainsChild(info, setFilter)
    }

    override suspend fun invoke(from: FROM): List<TO> {
        return from.db.load(from, this)
    }
}
