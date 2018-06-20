package com.xs0.dbktx.schema

import com.xs0.dbktx.crud.FilterBuilder
import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.expr.ExprBoolean

class RelToManyImpl<FROM : DbEntity<FROM, FID>, FID: Any, TO : DbEntity<TO, TID>, TID: Any> : RelToMany<FROM, TO> {

    internal lateinit var info: ManyToOneInfo<TO, TID, FROM, FID>
    private lateinit var reverseIdMapper: (TO)->FID?
    private lateinit var queryExprBuilder: (Set<FID>, TableInQuery<TO>)-> ExprBoolean
    private lateinit var oppositeRel: RelToOneImpl<TO, TID, FROM, FID>

    internal fun init(oppositeRel: RelToOneImpl<TO, TID, FROM, FID>, info: ManyToOneInfo<TO, TID, FROM, FID>, reverseIdMapper: (TO)->FID?, queryExprBuilder: (Set<FID>, TableInQuery<TO>)-> ExprBoolean) {
        this.oppositeRel = oppositeRel
        this.info = info
        this.reverseIdMapper = reverseIdMapper
        this.queryExprBuilder = queryExprBuilder
    }

    fun reverseMap(to: TO): FID? {
        return reverseIdMapper(to)
    }

    val sourceTable: DbTable<FROM, FID>
        get() = info.oneTable

    override val targetTable: DbTable<TO, TID>
        get() = info.manyTable

    fun createCondition(fromIds: Set<FID>, tableInQuery: TableInQuery<TO>): ExprBoolean {
        return queryExprBuilder(fromIds, tableInQuery)
    }

    override suspend fun invoke(from: FROM): List<TO> {
        return from.db.load(from, this)
    }

    override suspend fun invoke(from: FROM, block: FilterBuilder<TO>.() -> ExprBoolean): List<TO> {
        val query = info.manyTable.newQuery(from.db)

        query.filter { oppositeRel.has { block() } }
        query.filter { createCondition(setOf(from.id), query.baseTable) }

        return query.run()
    }
}
