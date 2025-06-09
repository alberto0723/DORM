INSERT INTO <sourcesch>.books_table VALUES (1, 'The Lord of the Rings', 'HarperCollins', 101, 'J.R.R. Tolkien', 133, 'M', 'U.K.');
INSERT INTO <sourcesch>.books_table VALUES (2, 'The Goods Themselves', 'Galaxy', 102, 'Isaac Asimov', 105, 'M', 'New York City, U.S.A.');

DO $$
DECLARE
    metadata JSONB;
BEGIN
    SELECT d.description::JSONB INTO metadata
    FROM pg_namespace n JOIN pg_description d ON d.objoid = n.oid
    WHERE n.nspname = 'source';

    EXECUTE format('COMMENT ON SCHEMA <sourcesch> IS %L', metadata || '{"has_data": true}');
END $$