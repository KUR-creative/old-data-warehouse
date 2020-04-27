CREATE TABLE IF NOT EXISTS file_source (
    name        TEXT    NOT NULL UNIQUE, 
    root_path   TEXT    NOT NULL,
    host        TEXT    NOT NULL
);

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE IF NOT EXISTS file (
    uuid    UUID     DEFAULT uuid_generate_v4 (),
    source  TEXT     REFERENCES file_source(name),
    relpath TEXT     NOT NULL, 
    abspath TEXT     NOT NULL, -- source.root/relpath
    type    TEXT,
    md5     BYTEA,
    size    INTEGER,
    PRIMARY KEY(uuid)
);

----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS image (
    uuid    UUID    REFERENCES file(uuid)
);

----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mask_scheme (
    name           TEXT   NOT NULL UNIQUE,
    description    TEXT   NOT NULL
);

CREATE TABLE IF NOT EXISTS mask_scheme_content (
    name           TEXT   NOT NULL REFERENCES mask_scheme(name),
    color          TEXT   NOT NULL,
    description    TEXT   NOT NULL
);

CREATE TABLE IF NOT EXISTS mask (
    uuid    UUID    REFERENCES file(uuid),
    scheme  TEXT    REFERENCES mask_scheme(name)
);

----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS image_metadata (
    uuid   UUID     REFERENCES file(uuid),
    size   INTEGER,
    height INTEGER,
    width  INTEGER,
    depth  INTEGER
);
