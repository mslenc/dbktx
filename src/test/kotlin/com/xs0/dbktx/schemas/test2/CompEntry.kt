package com.xs0.dbktx.schemas.test2

import com.xs0.asyncdb.common.RowData
import com.xs0.dbktx.composite.CompositeId3
import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.fieldprops.INT
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable


class CompEntry(db: DbConn, id: Int, private val row: RowData)
    : DbEntity<CompEntry, Int>(db, id) {

    override val metainfo get() = CompEntry

    val idPerson: Int get() = ID_PERSON(row)
    val idCountry: Int get() = ID_COUNTRY(row)
    val idWeight: Int get() = ID_WEIGHT(row)

    val dataId: DataId get() = DataId(row)

    class DataId : CompositeId3<CompEntry, Int, Int, Int, DataId> {
        override val column1 get() = ID_PERSON
        override val column2 get() = ID_COUNTRY
        override val column3 get() = ID_WEIGHT

        val idPerson: Int get() = component1
        val idCountry: Int get() = component2
        val idWeight: Int get() = component3

        constructor(idPerson: Int, idCountry: Int, idWeight: Int) : super(idPerson, idCountry, idWeight)
        constructor(row: RowData) : super(row)

        override val tableMetainfo get() = CompEntry
    }

    companion object : DbTable<CompEntry, Int>(TestSchema2, "comp_entries", CompEntry::class, Int::class) {
        val ID_ENTRY = b.nonNullInt("id_entry", INT(), CompEntry::id, primaryKey = true, autoIncrement = true)

        val ID_PERSON = b.nonNullInt("id_person", INT(), CompEntry::idPerson)
        val ID_WEIGHT = b.nonNullInt("id_weight", INT(), CompEntry::idWeight)
        val ID_COUNTRY = b.nonNullInt("id_country", INT(), CompEntry::idCountry)

        val KEY_DATA = b.uniqueKey(::DataId, CompEntry::dataId)

        val REF_PERSON = b.relToOne(ID_PERSON, Person::class)
        val REF_WEIGHT = b.relToOne(ID_WEIGHT, Weight::class)
        val REF_COUNTRY = b.relToOne(ID_COUNTRY, Country::class)

        val REF_RESULT = b.relToOne(ID_PERSON, ID_COUNTRY, ID_WEIGHT) { CompResult.KEY_DATA }

        init {
            b.build(::CompEntry)
        }
    }
}
