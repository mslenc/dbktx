package com.xs0.dbktx.util

import com.fasterxml.jackson.databind.ObjectMapper
import joptsimple.OptionParser
import joptsimple.OptionSpec
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
import java.sql.Connection
import java.sql.DriverManager
import kotlin.collections.ArrayList

class CodeGenConf {
    var jdbcUrl: String? = null
    var username: String? = null
    var password: String? = null

    val `package`: String? = null
    val schemaClassName: String? = null
    val baseDir: String? = null
}

object CodeGen {
    private val log = LoggerFactory.getLogger(CodeGen::class.java)

    private val optionParser: OptionParser
    private val configFileOpt: OptionSpec<File>
    private val helpOpt: OptionSpec<*>

    init {
        optionParser = OptionParser()
        configFileOpt = optionParser.accepts("config", "Path to JSON configuration file").withRequiredArg().ofType(File::class.java)
        helpOpt = optionParser.acceptsAll(listOf("h", "?", "help"), "Show help").forHelp()
    }

    fun fail(error: String): Nothing {
        log.error(error)
        System.exit(1)
        throw RuntimeException("Unreachable")
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val cmdLine = optionParser.parse(*args)
        if (cmdLine.has(helpOpt)) {
            optionParser.printHelpOn(System.out)
            return
        }

        val conf: CodeGenConf
        if (cmdLine.has(configFileOpt)) {
            val configFile = cmdLine.valueOf(configFileOpt)
            if (configFile.isFile && configFile.canRead()) {
                log.info("Reading config from {}", configFile)
                conf = ObjectMapper().readValue(configFile, CodeGenConf::class.java)
            } else {
                fail(configFile.toString() + " is not a readable file")
            }
        } else {
            conf = findConfFile()
        }

        if (conf.baseDir == null) fail("Missing baseDir in configuration")
        if (conf.jdbcUrl == null) fail("Missing jdbcUrl in configuration")
        if (conf.username == null) fail("Missing username in configuration")
        if (conf.password == null) fail("Missing password in configuration")
        if (conf.schemaClassName == null) fail("Missing schemaClassName in configuration")
        if (conf.`package` == null) fail("Missing package in configuration")

        val conn: Connection = DriverManager.getConnection(conf.jdbcUrl, conf.username, conf.password)
        val codeGen = CodeGenerator(conn, conf)

        println(codeGen.handleTable("vadbene_skupine_osebe"))
    }

    fun findConfFile(): CodeGenConf {
        val rootDir = Paths.get(".").toAbsolutePath().normalize()
        var dir = rootDir

        while (dir != null) {
            val filePath = dir.resolve("db-codegen-conf.json")
            if (Files.isRegularFile(filePath)) {
                return ObjectMapper().readValue(filePath.toFile(), CodeGenConf::class.java)
            } else {
                dir = dir.parent
            }
        }

        throw RuntimeException("Couldn't find db-codegen-conf.json in $rootDir or any of its parent directories")
    }
}

internal class CodeGenerator(private val conn: Connection, private val conf: CodeGenConf) {

    private class Col internal constructor(val name: String, nullable: String, colType: String, colKey: String, val comment: String, val extra: String) {
        val nullable: Boolean
        val colType: String
        val partOfPrimary: Boolean

        val fieldName: String
        val columnField: String
        val javaName: String
        val javaType: String
        val columnMaker: String
        val typeDef: String
        val imports = HashSet<String>()
        var primaryIndex: Int = 0
        val unsigned: Boolean
        val javaTypeWithNull: String
        val autoIncrement: Boolean

        init {
            var colType = colType
            this.nullable = "YES" == nullable
            this.unsigned = hasUnsigned(colType)
            colType = stripUnsigned(colType)
            this.colType = colType
            this.partOfPrimary = "PRI" == colKey
            this.autoIncrement = extra.toLowerCase().contains("auto_increment")

            val niceName = niceName(name)
            this.fieldName = niceName
            this.columnField = niceName.toUpperCase()
            this.javaName = javaName(niceName, false)


            if (colType.startsWith("char") || colType.startsWith("varchar") || colType.startsWith("varbinary") || colType.startsWith("binary") || colType.contains("text")) {
                if (fieldName.contains("id_") && colType.endsWith("(36)")) {
                    imports.add("java.util.UUID")
                    this.javaType = "UUID"
                    this.columnMaker = "UUID"
                    this.typeDef = colType.toUpperCase()
                } else {
                    this.javaType = "String"
                    this.columnMaker = "String"
                    this.typeDef = if (colType.endsWith(")")) colType.toUpperCase() else colType.toUpperCase() + "()"
                }
            } else if (colType.contains("blob")) {
                this.javaType = "ByteArray"
                this.columnMaker = "Bytes"
                this.typeDef = colType.toUpperCase() + "()"
            } else if (colType.startsWith("enum")) {
                this.javaType = "String"
                this.columnMaker = "StringEnum"
                this.typeDef = "setOf" + colType.substring(4).replace("'", "\"")
            } else if (colType.contains("int")) {
                this.javaType = "Int"
                this.columnMaker = "Int"
                this.typeDef = colType.toUpperCase()
            } else if (colType.startsWith("datetime")) {
                this.javaType = "LocalDateTime"
                imports.add("java.time.LocalDateTime")
                this.columnMaker = "DateTime"
                this.typeDef = colType.toUpperCase() + "()"
            } else if (colType.startsWith("date")) {
                this.javaType = "LocalDate"
                imports.add("java.time.LocalDate")
                this.columnMaker = "Date"
                this.typeDef = colType.toUpperCase() + "()"
            } else if (colType.startsWith("timestamp")) {
                this.javaType = "Instant"
                imports.add("java.time.Instant")
                this.columnMaker = "Instant"
                this.typeDef = colType.toUpperCase() + "()"
            } else if (colType.startsWith("time")) {
                this.javaType = "LocalTime"
                imports.add("java.time.LocalTime")
                this.columnMaker = "Time"
                this.typeDef = colType.toUpperCase() + "()"
            } else if (colType.startsWith("year")) {
                this.javaType = "Year"
                imports.add("java.time.Year")
                this.columnMaker = "Year"
                this.typeDef = if (colType.endsWith(")")) colType.toUpperCase() else colType.toUpperCase() + "()"
            } else if (colType.startsWith("decimal")) {
                this.javaType = "BigDecimal"
                imports.add("java.math.BigDecimal")
                this.columnMaker = "Decimal"
                this.typeDef = colType.toUpperCase()
            } else if (colType.startsWith("float")) {
                this.javaType = "Float"
                this.columnMaker = "Float"
                this.typeDef = colType.toUpperCase() + "()"
            } else if (colType.startsWith("double")) {
                this.javaType = "Double"
                this.columnMaker = "Double"
                this.typeDef = colType.toUpperCase() + "()"
            } else {
                this.javaType = "??? ($name)"
                this.columnMaker = "??? ($name)"
                this.typeDef = "??? ($name)"
            }
            this.javaTypeWithNull = this.javaType + (if (this.nullable) "?" else "")
        }

        companion object {

            internal fun hasUnsigned(s: String): Boolean {
                return s.toLowerCase().contains("unsigned")
            }

            internal fun stripUnsigned(s: String): String {
                return s.replace("unsigned", "").replace("UNSIGNED", "").trim { it <= ' ' }
            }
        }
    }

    fun handleTable(table: String): String {
        return createTableClass(table).code
    }

    private fun createTableClass(table: String): ClassRes {
        conn.prepareStatement("select * from information_schema.columns where table_schema=database() and table_name=?").use { stmt ->
            stmt.setString(1, table)
            stmt.executeQuery().use { res ->
                val cols = ArrayList<Col>()
                while (res.next()) {
                    cols.add(Col(
                        res.getString("COLUMN_NAME"),
                        res.getString("IS_NULLABLE"),
                        res.getString("COLUMN_TYPE"),
                        res.getString("COLUMN_KEY"),
                        res.getString("COLUMN_COMMENT"),
                        res.getString("EXTRA")

                    ))
                }
                return generate(table, cols, conf.`package`!!, conf.schemaClassName!!)
            }
        }
    }

    internal class ClassRes(val code: String, val className: String, val tableField: String, val idClass: String, val tableName: String)

    fun handleTables(generateFiles: Boolean, overwriteExisting: Boolean): String {
        val GEN_PACKAGE = conf.`package`!!
        val GEN_SCHEMA_CLASS = conf.schemaClassName!!

        val baseDir = Paths.get(conf.baseDir)
        var tmpDir = baseDir
        for (part in GEN_PACKAGE.split("[.]".toRegex()))
            tmpDir = tmpDir.resolve(part)
        val outDir = tmpDir
        Files.createDirectories(outDir)

        val results = ArrayList<ClassRes>()

        conn.prepareStatement("SHOW TABLES").use { stmt ->
            stmt.executeQuery().use { set ->
                while (set.next()) {
                    val tableName = set.getString(1)
                    results.add(createTableClass(tableName))
                }
            }
        }

        Files.createDirectories(outDir)

        val sb = StringBuilder()
        sb.append("package $GEN_PACKAGE\n\n")
        sb.append("import com.xs0.dbktx.schema.DbSchema\n")
        sb.append("\n")
        sb.append("object $GEN_SCHEMA_CLASS : DbSchema() {\n")

        for (res in results) {
            sb.append("    val " + res.tableField + " = " + res.className + "\n")
            if (generateFiles) {
                saveFile(outDir, res.className, res.code, overwriteExisting)
            }
        }

        sb.append("\n")
        sb.append("    init { this.finishInit() }\n")
        sb.append("}\n")

        if (generateFiles) {
            saveFile(outDir, GEN_SCHEMA_CLASS, sb.toString(), overwriteExisting)
        }

        return sb.toString()
    }

    private fun saveFile(outDir: Path, className: String, code: String, overwrite: Boolean) {
        val file = outDir.resolve("$className.kt")
        if (Files.isRegularFile(file) && !overwrite)
            return

        Files.write(file, code.toByteArray(StandardCharsets.UTF_8), CREATE, TRUNCATE_EXISTING)
    }

    private fun generate(table: String, cols: ArrayList<Col>, packageName: String, schemaClassName: String): ClassRes {
        val sb = StringBuilder()

        val imports = java.util.TreeSet<String>()
        val statics = java.util.TreeSet<String>()

        val className = "Db" + javaName(niceName(table), true)

        imports.add("com.xs0.dbktx.schema.*")
        imports.add("com.xs0.dbktx.conn.DbConn")
        imports.add("com.xs0.dbktx.fieldprops.*")

        val allNames = LinkedHashSet<String>()

        var numPrimary = 0
        var primaryCol: Col? = null
        for (col in cols) {
            allNames.add(col.fieldName)

            imports.addAll(col.imports)
            if (col.partOfPrimary) {
                col.primaryIndex = numPrimary++
                primaryCol = col
            }
        }

        var id = "id"
        var ID = "ID"
        for (bubu in arrayOf("id", "full_id", "row_id", "entire_id", "whole_id", "id_bobdammit")) {
            if (allNames.contains(bubu))
                continue

            id = bubu
            ID = bubu.toUpperCase()
            break
        }

        val idType: String
        val idTypeFull: String
        if (numPrimary == 1) {
            idType = primaryCol!!.javaType
            idTypeFull = idType
        } else {
            idTypeFull = className + "." + "Id"
            idType = "Id"
        }

        val tableField = niceName(table).toUpperCase()

        sb.appendln("class $className(db: DbConn, id: $idTypeFull, private val row: List<Any?>)")
        sb.appendln("    : DbEntity<$className, $idTypeFull>(db, id) {")
        sb.appendln()
        sb.appendln("    override val metainfo = TABLE")
        sb.appendln()

        for (col in cols) {
            if (col.partOfPrimary && numPrimary == 1)
                continue

            if (col.partOfPrimary) {
                sb.appendln("    val ${col.javaName}: ${col.javaTypeWithNull} get() = id.${col.javaName}")
            } else {
                sb.appendln("    val ${col.javaName}: ${col.javaTypeWithNull} get() = ${col.columnField}(row)")
            }
        }

        sb.appendln()

        val tableC = if (numPrimary > 1) "DbTableC" else "DbTable"

        sb.appendln("    companion object TABLE : $tableC<$className, $idTypeFull>($schemaClassName, \"$table\", $className::class, $idTypeFull::class) {")

        for (col in cols) {
            val maker = (if (col.nullable) "nullable" else "nonNull") + col.columnMaker
            val getter = if (col.partOfPrimary && numPrimary == 1) "id" else col.javaName

            sb.append("        val ${col.columnField} = b.$maker(\"${col.name}\", ${col.typeDef}, ${className}::${getter}")
            if (col.unsigned) {
                sb.append(", unsigned=true")
            }
            if (numPrimary == 1 && col.partOfPrimary) {
                sb.append(", primaryKey=true")
            }
            if (col.autoIncrement) {
                sb.append(", autoIncrement=true")
            }
            sb.append(")\n")
        }

        if (numPrimary > 1)
            sb.appendln("\n        val $ID = b.compositeId(::Id)")

        sb.appendln()
        sb.appendln("        init {")
        sb.appendln("            b.build(::$className)")
        sb.appendln("        }")
        sb.appendln("    }")
        sb.appendln()

        if (numPrimary > 1) {
            val primaryCols = cols.filter { it.partOfPrimary }

            imports.add("com.xs0.dbktx.composite.CompositeId" + numPrimary)
            sb.append("\n")
            sb.append("    class Id : CompositeId$numPrimary<$className, ")
            sb.append(primaryCols.joinToString { it.javaType })
            sb.append(", Id> {\n")
            sb.append("        constructor(")
            sb.append(primaryCols.joinToString { "${it.javaName}: ${it.javaType}" })
            sb.append(") : super(")
            sb.append(primaryCols.joinToString { it.javaName } )
            sb.append(")\n")
            sb.append("        constructor(row: List<Any?>) : super(row)\n")
            sb.append("\n")
            sb.append("        override val tableMetainfo get() = TABLE\n\n")

            for ((index, col) in primaryCols.withIndex())
                sb.append("        override val column${index + 1} get() = ${col.columnField}\n")

            sb.append("\n")

            for ((index, col) in primaryCols.withIndex())
                sb.appendln("        val ${col.javaName}: ${col.javaType} get() = component${index + 1}")

            sb.append("    }\n")
        }

        sb.append("}\n")


        val out = StringBuilder()
        out.append("package $packageName\n\n")
        for (i in imports)
            out.append("import $i\n")
        out.append("\n")
        for (i in statics)
            out.append("import $i\n")
        out.append("\n")

        out.append(sb)

        return ClassRes(out.toString(), className, tableField, idTypeFull, table)
    }

    companion object {

        fun niceName(name: String): String {
            var name = name
            name = name.replace("organizacije", "org")
            name = name.replace("organizacija", "org")
            name = name.replace("referencna", "ref")
            name = name.replace("referencni", "ref")
            name = name.replace("stevilka", "st")
            name = name.replace("ozanka", "oznaka")

            return name
        }

        fun javaName(table: String, capitalizeFirst: Boolean): String {
            val tmp = table.split("_".toRegex()).toTypedArray()
            for (i in tmp.indices) {
                if (capitalizeFirst || i > 0)
                    tmp[i] = Character.toUpperCase(tmp[i][0]) + tmp[i].substring(1)
            }
            return tmp.joinToString("")
        }
    }
}
