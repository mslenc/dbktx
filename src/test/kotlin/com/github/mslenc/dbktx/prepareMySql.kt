package com.github.mslenc.dbktx

import com.github.mslenc.asyncdb.DbConfig
import com.github.mslenc.asyncdb.DbTxIsolation
import com.github.mslenc.asyncdb.DbTxMode
import com.github.mslenc.asyncdb.DbType
import com.github.mslenc.asyncdb.jdbc.JdbcNonAsyncDbConnection
import com.github.mslenc.asyncdb.jdbc.JdbcSyncConnection
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.SimpleRequestTime
import com.github.mslenc.dbktx.util.DebuggingConnection
import kotlinx.coroutines.runBlocking
import java.sql.DriverManager
import java.time.Instant
import java.time.ZoneId

fun runMysqlTest(block: suspend (DbConn) -> Unit) {
    DriverManager.getConnection("jdbc:mysql://localhost:13306/testdb?useServerPrepStmts=true", "testUser", "testPass").use { conn ->
        for (statement in listOf(DbConfig.DEFAULT_MYSQL_INIT_SQL) + initMySql) {
            conn.prepareStatement(statement).use {
                it.execute()
            }
        }

        val config =
            DbConfig.newBuilder(DbType.MYSQL).
                setDefaultTxMode(DbTxMode.READ_WRITE).
                setDefaultTxIsolation(DbTxIsolation.REPEATABLE_READ).
            build()

        val db = DebuggingConnection(JdbcNonAsyncDbConnection(JdbcSyncConnection(config, conn) { it.close() }))
        val requestTime = SimpleRequestTime(Instant.now(), ZoneId.systemDefault())

        runBlocking {
            val loader = DbLoaderImpl(db, this, requestTime)
            block(loader)
        }
    }
}

val initMySql = listOf(
"DROP TABLE IF EXISTS contact_info_2",
"DROP TABLE IF EXISTS employee",
"DROP TABLE IF EXISTS purchase_items",
"DROP TABLE IF EXISTS purchases",
"DROP TABLE IF EXISTS items",
"DROP TABLE IF EXISTS contact_info",
"DROP TABLE IF EXISTS brands",
"DROP TABLE IF EXISTS companies",

"""
CREATE TABLE companies(
    id VARCHAR(36) NOT NULL PRIMARY KEY COLLATE utf8_bin,
    name VARCHAR(255) NOT NULL,
    t_created DATETIME NOT NULL,
    t_updated DATETIME NOT NULL
) engine=InnoDB
""",

"""
CREATE TABLE brands(
    company_id VARCHAR(36) NOT NULL COLLATE utf8_bin,
    "key" VARCHAR(255) NOT NULL,

    name VARCHAR(255) NOT NULL,
    tag_line VARCHAR(255) NULL,
    t_created DATETIME NOT NULL,
    t_updated DATETIME NOT NULL,

    PRIMARY KEY(company_id, "key"),
    FOREIGN KEY(company_id) REFERENCES companies(id)
) engine=InnoDB
""",

"""
CREATE TABLE contact_info(
    id VARCHAR(36) NOT NULL COLLATE utf8_bin,
    company_id VARCHAR(36) NULL COLLATE utf8_bin,
    address VARCHAR(255) NOT NULL,
    PRIMARY KEY(id),
    FOREIGN KEY(company_id) REFERENCES companies(id)
) engine=InnoDB
""",

"""
CREATE TABLE items(
company_id VARCHAR(36) NOT NULL COLLATE utf8_bin,
sku VARCHAR(255) NOT NULL,
brand_key VARCHAR(255) NOT NULL,
name VARCHAR(255) NOT NULL,
price DECIMAL(9,2) NOT NULL,
t_created DATETIME NOT NULL,
t_updated DATETIME NOT NULL,
PRIMARY KEY(company_id, sku),
FOREIGN KEY(company_id) REFERENCES companies(id),
FOREIGN KEY(company_id, brand_key) REFERENCES brands(company_id, "key")
)
""",

"""
CREATE TABLE purchases(
id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
company_id VARCHAR(36) NOT NULL COLLATE utf8_bin,
t_created DATETIME NOT NULL,
t_updated DATETIME NOT NULL,

FOREIGN KEY(company_id) REFERENCES companies(id)
);
""",

"""
CREATE TABLE purchase_items(
id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
company_id VARCHAR(36) NOT NULL COLLATE utf8_bin,
sku VARCHAR(255) NOT NULL,
purchase_id BIGINT NOT NULL,

price DECIMAL(9,2) NOT NULL,
t_created DATETIME NOT NULL,
t_updated DATETIME NOT NULL,

UNIQUE KEY(id, company_id, sku),
FOREIGN KEY(company_id, sku) REFERENCES items(company_id, sku),
FOREIGN KEY(purchase_id) REFERENCES purchases(id)
)
""",

"""
CREATE TABLE employee (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL
)
""",

"""
CREATE TABLE contact_info_2 (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(255) NULL,
    last_name VARCHAR(255) NULL,
    street_1 VARCHAR(255) NULL,
    street_2 VARCHAR(255) NULL,
    employee_id BIGINT NULL,
    
    FOREIGN KEY(employee_id) REFERENCES employee(id)
)
""",

"""
INSERT INTO employee(id, first_name, last_name) VALUES 
    (1, 'Johnny', 'Smith'),
    (2, 'Mary', 'Johnson')
""",

"""
INSERT INTO contact_info_2(id, first_name, last_name, street_1, street_2, employee_id) VALUES
    (100, 'John', 'Smith', null, null, 1)
""".trimIndent()
)
