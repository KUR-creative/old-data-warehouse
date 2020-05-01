CREATE TABLE IF NOT EXISTS executed_command (
    command     TEXT       NOT NULL,
    timestamp   TIMESTAMP  WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    git_hash    TEXT,
    note        TEXT,
    UNIQUE(command, timestamp)
);

----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS file_source (
    name        TEXT    PRIMARY KEY,
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
    PRIMARY KEY(uuid),
    UNIQUE (source, relpath, abspath)
);

----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS image (
    uuid    UUID    NOT NULL UNIQUE     REFERENCES file(uuid)
);

----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mask_scheme (
    name           TEXT   PRIMARY KEY,
    description    TEXT   NOT NULL
);

CREATE TABLE IF NOT EXISTS mask_scheme_content (
    name           TEXT   NOT NULL REFERENCES mask_scheme(name),
    color          TEXT   NOT NULL,
    description    TEXT   NOT NULL
);

CREATE TABLE IF NOT EXISTS mask (
    uuid    UUID    NOT NULL UNIQUE     REFERENCES file(uuid),
    scheme  TEXT    REFERENCES mask_scheme(name)
);

----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS snet_annotation (
    input   UUID    REFERENCES image(uuid),
    output  UUID    REFERENCES mask(uuid)
);

CREATE TABLE IF NOT EXISTS dataset (
    name           TEXT,
    split          TEXT,
    train          INTEGER,
    valid          INTEGER,
    test           INTEGER,
    description    TEXT,
    PRIMARY KEY(name, split, train, valid, test)
);

CREATE TYPE Usage AS ENUM ('train', 'valid', 'test');
CREATE TABLE IF NOT EXISTS dataset_annotation (
    name           TEXT,
    split          TEXT,
    train          INTEGER,
    valid          INTEGER,
    test           INTEGER,
    input          UUID,
    output         UUID,
    usage          Usage,
    UNIQUE (name, split, train, valid, test, input, output),
    FOREIGN KEY (name, split, train, valid, test)
    REFERENCES dataset(name, split, train, valid, test)
);
COMMENT ON TABLE dataset_annotation
IS 'Relation of dataset and annotation';

----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS image_metadata (
    uuid   UUID     REFERENCES file(uuid),
    size   INTEGER,
    height INTEGER,
    width  INTEGER,
    depth  INTEGER
);
