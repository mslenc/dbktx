package com.xs0.dbktx.schema

import com.xs0.dbktx.conn.DbLoaderImpl
import com.xs0.dbktx.conn.DbLoaderInternal
import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.expr.*

class RelToOneImpl<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>, KEY : Any> : RelToOne<FROM, TO> {
    internal lateinit var info: ManyToOneInfo<FROM, TO, KEY>
    internal lateinit var keyMapper: (FROM)->KEY?

    fun init(info: ManyToOneInfo<FROM, TO, KEY>, keyMapper: (FROM)->KEY?) {
        this.info = info
        this.keyMapper = keyMapper
    }

    fun mapKey(from: FROM): KEY? {
        return keyMapper(from)
    }

    override val targetTable: DbTable<TO, *>
        get() = info.oneTable

    override suspend fun invoke(from: FROM): TO? {
        return from.db.find(from, this)
    }

    internal suspend fun callFindByRelation(db: DbLoaderImpl, from: FROM): TO? {
        return db.findByRelation(from, this)
    }

    companion object {
        internal fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>, T: Any>
            makeEq(map: ColumnMapping<FROM, TO, T>, ref: TO, tableInQuery: TableInQuery<FROM>): ExprBoolean {

            return ExprBinary(
                map.columnFrom.bindForSelect(tableInQuery),
                ExprBinary.Op.EQ,
                map.columnFrom.makeLiteral(map.columnTo(ref))
            )
        }

        internal fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>, T: Any>
            makeOneOf(map: ColumnMapping<FROM, TO, T>, refs: List<TO>, tableInQuery: TableInQuery<FROM>): ExprBoolean {

            val set = LinkedHashSet<T>(refs.size)
            refs.mapTo(set) { map.columnTo(it) }

            return ExprOneOf(map.columnFrom.bindForSelect(tableInQuery), set.map { map.columnFrom.makeLiteral(it) })
        }
    }
}
