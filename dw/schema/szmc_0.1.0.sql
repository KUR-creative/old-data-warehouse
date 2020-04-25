CREATE TABLE IF NOT EXISTS file (
    uuid   UUID     uuid DEFAULT uuid_generate_v4 (),
    path   TEXT     NOT NULL,
    md5    BYTEA,
    size   INTEGER
);

CREATE TABLE IF NOT EXISTS image (
    uuid   UUID     REFERENCES file(uuid),
    path   TEXT     NOT NULL,
    size   INTEGER,
    height INTEGER,
    width  INTEGER
);
