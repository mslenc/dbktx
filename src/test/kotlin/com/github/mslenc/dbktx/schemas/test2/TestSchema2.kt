package com.github.mslenc.dbktx.schemas.test2

import com.github.mslenc.dbktx.schema.*

fun <E: DbEntity<E, *>>
refToLocalName(sourceIdColumn: NonNullColumn<E, Int>, propName: String, langCode: String): RelToOne<E, LocalName> {
    val result = RelToOneImpl<E, LocalName, LocalName.Id>()

    val columnMappings = arrayOf(
        ColumnMappingActualColumn(sourceIdColumn, LocalName.ENTITY_ID),
        ColumnMappingLiteral(langCode, LocalName.LANG_CODE, isParameter = true),
        ColumnMappingLiteral<E, LocalName, String>(propName, LocalName.PROP_NAME, isParameter = false)
    )

    val info = ManyToOneInfo(sourceIdColumn.table, LocalName, LocalName.ID, columnMappings)

    result.init(info) { entity ->
        LocalName.Id(propName = propName, entityId = sourceIdColumn(entity), langCode = langCode)
    }

    return result
}


object TestSchema2 : DbSchema() {
    val WEIGHT = Weight
    val COUNTRY = Country
    val PERSON = Person
    val COMPETITION = Competition
    val COMP_ENTRY = CompEntry
    val COMP_RESULT = CompResult
    val LOCAL_NAME = LocalName

    init {
        finishInit()
    }
}
