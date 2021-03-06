package com.github.mslenc.dbktx.schemas.test2

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.INT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import com.github.mslenc.dbktx.schema.RelToOne


class Competition(id: Int, val row: DbRow)
    : DbEntity<Competition, Int>(id) {

    override val metainfo get() = Competition

    val name: String get() = NAME(row)

    companion object : DbTable<Competition, Int>(TestSchema2, "competitions", Competition::class, Int::class) {
        val ID_COMPETITION = b.nonNullInt("id_competition", INT(), Competition::id, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("name", VARCHAR(255), Competition::name)

        val REF_LOCALNAME_EN = refToLocalName(ID_COMPETITION, "competition.name", "en")
        val REF_LOCALNAME_SL = refToLocalName(ID_COMPETITION, "competition.name", "sl")
        val REF_LOCALNAME_DE = refToLocalName(ID_COMPETITION, "competition.name", "de")

        val ENTRIES_SET = b.relToMany { CompEntry.REF_COMPETITION }

        fun REF_LOCALNAME(langCode: String): RelToOne<Competition, LocalName> {
            return when(langCode) {
                "en" -> REF_LOCALNAME_EN
                "sl" -> REF_LOCALNAME_SL
                "de" -> REF_LOCALNAME_DE
                else -> throw IllegalArgumentException("Unsupported language $langCode")
            }
        }

        init {
            b.build(::Competition)
        }
    }
}
