package com.github.mslenc.dbktx.sqltypes

enum class SqlTypeKind {
    BIT,
    TINYINT,
    BOOLEAN,
    SMALLINT,
    MEDIUMINT,
    INT,
    BIGINT,
    DECIMAL,
    NUMERIC,
    FLOAT,
    DOUBLE,

    DATE,
    DATETIME,
    TIMESTAMP,
    TIMESTAMP_TZ,
    TIME,
    YEAR,

    CHAR,
    VARCHAR,
    TINYTEXT,
    TEXT,
    MEDIUMTEXT,
    LONGTEXT,

    BINARY,
    VARBINARY,
    TINYBLOB,
    BLOB,
    MEDIUMBLOB,
    LONGBLOB,

    ENUM,
    SET,

    JSON
}