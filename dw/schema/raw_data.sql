CREATE TABLE IF NOT EXISTS manga109(
    title TEXT NOT NULL,
    no INTEGER NOT NULL,
    path TEXT NOT NULL,
    xml XML NOT NULL,
    PRIMARY KEY (title, no)
);

CREATE TABLE IF NOT EXISTS metadata(
    tablename TEXT NOT NULL,
    root TEXT NOT NULL
);
