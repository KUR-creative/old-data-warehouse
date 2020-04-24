CREATE TABLE IF NOT EXISTS manga109_raw (
    title           TEXT    NOT NULL,
    no              INTEGER NOT NULL,
    path            TEXT    NOT NULL,
    PRIMARY KEY (title, no)
);

CREATE TABLE IF NOT EXISTS manga109_xml (
    title           TEXT    NOT NULL UNIQUE,
    xml             XML     NOT NULL
);

CREATE TABLE IF NOT EXISTS old_snet_data_raw (
    id              INTEGER NOT NULL UNIQUE,
    img_name        TEXT    NOT NULL,
    img_path        TEXT    NOT NULL,
    rbk_mask_path   TEXT    NOT NULL,
    wk_mask_path    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_table_root (
    raw_table_name  TEXT    NOT NULL UNIQUE, 
    root            TEXT    NOT NULL
);
