package com.github.mslenc.dbktx.schemas.test2

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.composite.CompositeId3
import com.github.mslenc.dbktx.fieldprops.INT
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable


class CompResult(id: Int, val row: DbRow)
    : DbEntity<CompResult, Int>(id) {

    override val metainfo get() = CompResult

    val idPerson: Int get() = ID_PERSON(row)
    val idCountry: Int get() = ID_COUNTRY(row)
    val idWeight: Int get() = ID_WEIGHT(row)

    val place: Int? get() = PLACE(row)

    val dataId: DataId get() = DataId(row)

    class DataId : CompositeId3<CompResult, Int, Int, Int, DataId> {
        override val column1 get() = ID_PERSON
        override val column2 get() = ID_COUNTRY
        override val column3 get() = ID_WEIGHT

        val idPerson: Int get() = component1
        val idCountry: Int get() = component2
        val idWeight: Int get() = component3

        constructor(idPerson: Int, idCountry: Int, idWeight: Int) : super(idPerson, idCountry, idWeight)
        constructor(row: DbRow) : super(row)

        override val tableMetainfo get() = CompResult
    }

    companion object : DbTable<CompResult, Int>(TestSchema2, "comp_results", CompResult::class, Int::class) {
        val ID_ENTRY = b.nonNullInt("id_entry", INT(), CompResult::id, primaryKey = true, autoIncrement = true)

        val ID_PERSON = b.nonNullInt("id_person", INT(), CompResult::idPerson)
        val ID_WEIGHT = b.nonNullInt("id_weight", INT(), CompResult::idWeight)
        val ID_COUNTRY = b.nonNullInt("id_country", INT(), CompResult::idCountry)

        val PLACE = b.nullableInt("place", INT(), CompResult::place)

        val KEY_DATA = b.uniqueKey(::DataId, CompResult::dataId)

        val REF_PERSON = b.relToOne(ID_PERSON, Person::class)
        val REF_WEIGHT = b.relToOne(ID_WEIGHT, Weight::class)
        val REF_COUNTRY = b.relToOne(ID_COUNTRY, Country::class)

        val REF_ENTRY = b.relToOne(ID_PERSON, ID_WEIGHT, ID_COUNTRY) { CompEntry.KEY_DATA }

        init {
            b.build(::CompResult)
        }
    }
}
