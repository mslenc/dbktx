package com.github.mslenc.dbktx.schemas.test2

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.composite.CompositeId3
import com.github.mslenc.dbktx.fieldprops.INT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTableC


class LocalName(id: LocalName.Id, val row: DbRow)
    : DbEntity<LocalName, LocalName.Id>(id) {

    override val metainfo get() = LocalName

    val propName: String get() = PROP_NAME(row)
    val entityId: Int get() = ENTITY_ID(row)
    val langCode: String get() = LANG_CODE(row)
    val name: String get() = NAME(row)

    class Id : CompositeId3<LocalName, String, Int, String, Id> {
        override val column1 get() = PROP_NAME
        override val column2 get() = ENTITY_ID
        override val column3 get() = LANG_CODE

        val propName: String get() = component1
        val entityId: Int get() = component2
        val langCode: String get() = component3

        constructor(propName: String, entityId: Int, langCode: String) : super(propName, entityId, langCode)
        constructor(row: DbRow) : super(row)

        override val tableMetainfo get() = LocalName
    }

    companion object : DbTableC<LocalName, LocalName.Id>(TestSchema2, "local_names", LocalName::class, LocalName.Id::class) {
        val PROP_NAME = b.nonNullString("prop_name", VARCHAR(255), LocalName::propName)
        val ENTITY_ID = b.nonNullInt("entity_id", INT(), LocalName::entityId)
        val LANG_CODE = b.nonNullString("lang_code", VARCHAR(255), LocalName::langCode)

        val NAME = b.nonNullString("name", VARCHAR(255), LocalName::name)

        val ID = b.primaryKey(::Id)

        init {
            b.build(::LocalName)
        }
    }
}
