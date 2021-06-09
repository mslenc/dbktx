package com.github.mslenc.dbktx.schemas.test2

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.*
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable


class Person(id: Int, val row: DbRow)
    : DbEntity<Person, Int>(id) {

    override val metainfo get() = Person

    val familyName: String get() = FAMILY_NAME(row)
    val givenName: String get() = GIVEN_NAME(row)
    val idCountry: Int get() = ID_COUNTRY(row)

    suspend fun country(): Country {
        return REF_COUNTRY(this)!!
    }

    companion object : DbTable<Person, Int>(TestSchema2, "persons", Person::class, Int::class) {
        val ID_PERSON = b.nonNullInt("id_person", INT(), Person::id, primaryKey = true, autoIncrement = true)
        val ID_COUNTRY = b.nonNullInt("id_country", INT(), Person::idCountry)
        val FAMILY_NAME = b.nonNullString("family_name", VARCHAR(255), Person::familyName)
        val GIVEN_NAME = b.nonNullString("given_name", VARCHAR(255), Person::givenName)

        val REF_COUNTRY = b.relToOne(ID_COUNTRY, Country::class)

        init {
            b.build(::Person)
        }
    }
}
