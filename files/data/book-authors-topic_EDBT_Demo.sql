DELETE FROM writes_table;
DELETE FROM touches_table;
DELETE FROM authors_table;
DELETE FROM books_table;
DELETE FROM topics_table;

DO $$
DECLARE
    i INT;
    v_author_id INT; v_topic_id INT;
BEGIN
    RAISE NOTICE 'Starting data insertion for 100 records...';

    -- 1. Insert rows into Authors_table
    FOR i IN 0..999 LOOP
        INSERT INTO Authors_table (A_ID, A_name, A_age, A_gender, A_address)
        VALUES (i, 'Author ' || LPAD(i::TEXT, 3, '0'),  FLOOR(RANDOM() * 60) + 20,  CASE WHEN (i % 2) = 0 THEN 'M' ELSE 'F' END, 'Address ' || i || ', City' );
    END LOOP;
    RAISE NOTICE 'Inserted 1000 rows into Authors_table.';

    -- 2. Insert rows into Topics_table
    FOR i IN 0..99 LOOP
        INSERT INTO Topics_table (T_ID, T_description) VALUES (i, 'Topic Description for Subject ' || LPAD(i::TEXT, 2, '0') );
    END LOOP;
    RAISE NOTICE 'Inserted 100 rows into Topics_table.';

    -- 3. Insert rows into Books_table
    FOR i IN 0..9999 LOOP
        INSERT INTO Books_table (B_ID, B_title, B_publisher) VALUES (i, 'Book Title ' || LPAD(i::TEXT, 5, '0'), 'Publisher ' || (FLOOR(RANDOM() * 5) + 1) );
    		-- 4. Insert up to 3 rows into Writes_table (Book -> Author relationship)
				FOR j IN 0..FLOOR(RANDOM() * 3) LOOP
            v_author_id := FLOOR(RANDOM() * 1000);
            BEGIN
                INSERT INTO Writes_table (Writes_Book, Writes_Author) VALUES (i, v_author_id);
            EXCEPTION WHEN unique_violation THEN END;
        END LOOP;
    		-- 5. Insert up to 2 rows into Touches_table (Book -> Topic relationship)
				FOR j IN 0..FLOOR(RANDOM() * 2) LOOP
            v_topic_id := FLOOR(RANDOM() * 100);
            BEGIN
                INSERT INTO Touches_table (Touches_Book, Touches_Topic) VALUES (i, v_topic_id);
            EXCEPTION WHEN unique_violation THEN END;
        END LOOP;
    END LOOP;
    RAISE NOTICE 'Inserted 10000 rows into Books_table, +20000 into Writes_table, and ~15000 into Touches_table.';
    RAISE NOTICE 'All data successfully inserted.';
END
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    metadata JSONB;
BEGIN
    SELECT d.description::JSONB INTO metadata
    FROM pg_namespace n JOIN pg_description d ON d.objoid = n.oid
    WHERE n.nspname = 'source2';

    EXECUTE format('COMMENT ON SCHEMA baseline IS %L', metadata || '{"has_data": true}');
END $$