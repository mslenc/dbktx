package com.xs0.dbktx.schema

import com.xs0.dbktx.conn.DbLoaderImpl

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
}
