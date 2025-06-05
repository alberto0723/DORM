INSERT INTO source2.books_table VALUES (1, 'The Lord of the Rings', 'HarperCollins');
INSERT INTO source2.books_table VALUES (2, 'The Goods Themselves', 'Galaxy');
INSERT INTO source2.books_table VALUES (3, 'Data Warehouse Systems', 'Springer');
INSERT INTO source2.books_table VALUES (4, 'Data Warehouse Design', 'McGraw Hill');
INSERT INTO source2.books_table VALUES (5, 'Database Systems', 'Pearson');

INSERT INTO source2.authors_table VALUES (101, 'J.R.R. Tolkien', 133, 'M', 'U.K.');
INSERT INTO source2.authors_table VALUES (102, 'Isaac Asimov', 105, 'M', 'U.S.A.');
INSERT INTO source2.authors_table VALUES (103, 'Alejandro Vaisman', NULL, 'M', 'Argentina');
INSERT INTO source2.authors_table VALUES (104, 'Esteban Zimanyi', NULL, 'M', 'Belgium');
INSERT INTO source2.authors_table VALUES (105, 'Matteo Golfarelli', NULL, 'M', 'Italy');
INSERT INTO source2.authors_table VALUES (106, 'Stefano Rizzi', NULL, 'M', 'Italy');
INSERT INTO source2.authors_table VALUES (107, 'Hector Garcia-Molina', 70, 'M', 'U.S.A.');
INSERT INTO source2.authors_table VALUES (108, 'Jeffrey D. Ullman', 83, 'M', 'U.S.A.');
INSERT INTO source2.authors_table VALUES (109, 'Jennifer Widom', 64, 'F', 'U.S.A.');

INSERT INTO source2.topics_table VALUES (201, 'Data Warehousing');
INSERT INTO source2.topics_table VALUES (202, 'Data Base Management Systems');
INSERT INTO source2.topics_table VALUES (203, 'Science-Fiction');
INSERT INTO source2.topics_table VALUES (204, 'Fantasy');

INSERT INTO source2.writes_table VALUES (101, 1);
INSERT INTO source2.writes_table VALUES (102, 2);
INSERT INTO source2.writes_table VALUES (103, 3);
INSERT INTO source2.writes_table VALUES (104, 3);
INSERT INTO source2.writes_table VALUES (105, 4);
INSERT INTO source2.writes_table VALUES (106, 4);
INSERT INTO source2.writes_table VALUES (107, 5);
INSERT INTO source2.writes_table VALUES (108, 5);
INSERT INTO source2.writes_table VALUES (109, 5);

INSERT INTO source2.touches_table VALUES (201, 3);
INSERT INTO source2.touches_table VALUES (201, 4);
INSERT INTO source2.touches_table VALUES (202, 3);
INSERT INTO source2.touches_table VALUES (202, 4);
INSERT INTO source2.touches_table VALUES (202, 5);
INSERT INTO source2.touches_table VALUES (203, 1);
INSERT INTO source2.touches_table VALUES (203, 2);
INSERT INTO source2.touches_table VALUES (204, 1);

DO $$
DECLARE
    metadata JSONB;
BEGIN
    SELECT d.description::JSONB INTO metadata
    FROM pg_namespace n JOIN pg_description d ON d.objoid = n.oid
    WHERE n.nspname = 'source2';

    EXECUTE format('COMMENT ON SCHEMA source2 IS %L', metadata || '{"data_migrated": true}');
END $$