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
    PRIMARY KEY (uuid),
    UNIQUE (source, relpath, abspath)
);

----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS image (
    uuid    UUID    REFERENCES file(uuid),
    y       INTEGER CHECK (0 <= y AND y <  full_h),
    x       INTEGER CHECK (0 <= x AND x <  full_w),
    h       INTEGER CHECK (0 <  h AND h <= full_h),
    w       INTEGER CHECK (0 <  w AND w <= full_w),
    full_h  INTEGER NOT NULL,
    full_w  INTEGER NOT NULL,
    UNIQUE (uuid, full_h, full_w),
    PRIMARY KEY (uuid, x, y, h ,w)
);
COMMENT ON COLUMN image.y IS 'y in top left (y,x)';
COMMENT ON COLUMN image.x IS 'x in top left (y,x)';

CREATE TABLE IF NOT EXISTS image_metadata (
    uuid   UUID,
    y      INTEGER,
    x      INTEGER,
    h      INTEGER,
    w      INTEGER,
    depth  INTEGER,
    hash   BYTEA, --hash .. what type? think..
    FOREIGN KEY (uuid, y,x, h,w)
    REFERENCES image (uuid, y,x, h,w)
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
CREATE TABLE IF NOT EXISTS annotation_type (
    name           TEXT   PRIMARY KEY,
    description    TEXT   NOT NULL
);

CREATE TABLE IF NOT EXISTS annotation (
    input   UUID,
    y       INTEGER,
    x       INTEGER,
    h       INTEGER,
    w       INTEGER,
    output  UUID    NOT NULL,
    type    TEXT    REFERENCES annotation_type(name),
    FOREIGN KEY (input, y,x, h,w)
    REFERENCES image (uuid, y,x, h,w)
);
COMMENT ON COLUMN annotation.output
IS 'output could be mask, file, or just integer...';

CREATE TABLE IF NOT EXISTS dataset (
    name           TEXT,
    split          TEXT,
    train          INTEGER,
    valid          INTEGER,
    test           INTEGER,
    description    TEXT,
    PRIMARY KEY (name, split, train, valid, test)
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
