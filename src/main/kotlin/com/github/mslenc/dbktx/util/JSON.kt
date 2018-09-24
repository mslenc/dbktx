package com.github.mslenc.dbktx.util

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import kotlin.reflect.KClass

object JSON {
    private val jsonMapper = ObjectMapper()

    init {
        jsonMapper.registerModule(KotlinModule())
        jsonMapper.registerModule(ParameterNamesModule())
        jsonMapper.registerModule(Jdk8Module())
        jsonMapper.registerModule(JavaTimeModule())

        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true)

        jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
    }

    fun stringify(value: Any): String {
        return jsonMapper.writeValueAsString(value)
    }

    fun <T: Any> parse(value: String, klass: KClass<T>): T {
        return jsonMapper.readValue(value, klass.java) as T
    }
}