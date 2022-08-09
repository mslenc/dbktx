package com.github.mslenc.dbktx.schema

import com.github.mslenc.dbktx.crud.EntityValues
import com.github.mslenc.dbktx.schemas.initSchemas
import com.github.mslenc.dbktx.schemas.test1.Brand
import org.junit.Test

import org.junit.Assert.*
import java.util.*

class MultiColumnKeyDefTest {
    init {
        initSchemas()
    }

    @Test
    fun testExtractMultiKeyID() {
        val companyId = UUID.randomUUID()

        val values = EntityValues<Brand>()
        values.set(Brand.COMPANY_ID, companyId)
        values.set(Brand.KEY, "someKey")

        val extractedId: Brand.Id? = Brand.ID.extract(values)
        if (extractedId == null)
            fail("Expected ID") as Nothing

        assertEquals(companyId, extractedId.companyId)
        assertEquals("someKey", extractedId.key)
    }
}