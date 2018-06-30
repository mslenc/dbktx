package com.xs0.dbktx.schema

import com.xs0.dbktx.crud.EntityQuery
import com.xs0.dbktx.crud.FilterBuilder
import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.expr.ExprBoolean

class RelToManyImpl<FROM : DbEntity<FROM, *>, FROM_KEY: Any, TO : DbEntity<TO, *>> : RelToMany<FROM, TO> {

    internal lateinit var info: ManyToOneInfo<TO, FROM, FROM_KEY>
    private lateinit var reverseKeyMapper: (TO)->FROM_KEY?
    private lateinit var queryExprBuilder: (Set<FROM_KEY>, TableInQuery<TO>)-> ExprBoolean
    private lateinit var oppositeRel: RelToOneImpl<TO, FROM, FROM_KEY>

    internal fun init(oppositeRel: RelToOneImpl<TO, FROM, FROM_KEY>, info: ManyToOneInfo<TO, FROM, FROM_KEY>, reverseKeyMapper: (TO)->FROM_KEY?, queryExprBuilder: (Set<FROM_KEY>, TableInQuery<TO>)-> ExprBoolean) {
        this.oppositeRel = oppositeRel
        this.info = info
        this.reverseKeyMapper = reverseKeyMapper
        this.queryExprBuilder = queryExprBuilder
    }

    fun reverseMap(to: TO): FROM_KEY? {
        return reverseKeyMapper(to)
    }

    val sourceTable: DbTable<FROM, *>
        get() = info.oneTable

    override val targetTable: DbTable<TO, *>
        get() = info.manyTable

    fun createCondition(fromIds: Set<FROM_KEY>, tableInQuery: TableInQuery<TO>): ExprBoolean {
        return queryExprBuilder(fromIds, tableInQuery)
    }

    override suspend fun invoke(from: FROM): List<TO> {
        return from.db.load(from, this)
    }

    override suspend fun invoke(from: FROM, block: FilterBuilder<TO>.() -> ExprBoolean): List<TO> {
        val query: EntityQuery<TO> = info.manyTable.newQuery(from.db)

        query.filter { createCondition(setOf(info.oneKey(from)), query.baseTable) }
        query.filter(block)

        return query.run()
    }
}
