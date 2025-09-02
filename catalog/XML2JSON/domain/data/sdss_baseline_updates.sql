UPDATE dorm_edbt_baseline.photoobjall_table
SET photoobjall_discrG1 = CASE WHEN photoobjall_mode = 1 AND photoobjall_clean = 1 THEN 'photoobj'
													ELSE 'photoobjcompl' END,
		photoobj_discrG2 = CASE WHEN photoobjall_mode = 1 AND photoobjall_clean = 1 AND (photoobjall_resolveStatus & 0x01) != 0 THEN 'photoprimary'
													ELSE 'photoprimarycompl' END,
		photoobjall_discrG6 = CASE WHEN photoobjall_objid IN (SELECT photoobjall_objid FROM dorm_edbt_baseline.photoz_table) THEN 'photoz'
													ELSE 'photozcompl' END;													


UPDATE dorm_edbt_baseline.specobjall_table
SET specobjall_discrG3 = CASE WHEN specobjall_scienceprimary = 1 THEN 'specobj'
												 ELSE 'specobjcompl' END;

DO $$
DECLARE
    metadata JSONB;
BEGIN
    SELECT d.description::JSONB INTO metadata
    FROM pg_namespace n JOIN pg_description d ON d.objoid = n.oid
    WHERE n.nspname = 'dorm_edbt_baseline';

    EXECUTE format('COMMENT ON SCHEMA dorm_edbt_baseline IS %L', metadata || '{"has_data": true}');
END $$
