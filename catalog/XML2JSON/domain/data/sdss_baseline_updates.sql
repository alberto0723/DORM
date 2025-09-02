UPDATE dorm_edbt_baseline.photoobjall
SET photoobjall_discrG1 = CASE WHEN photoobjall_mode = 1 AND photoobjall_clean = 1 THEN 'photoobj'
													ELSE 'photoobjcompl',
		photoobjall_discrG2 = CASE WHEN photoobjall_mode = 1 AND photoobjall_clean = 1 AND (photoobjall_resolveStatus & 0x01) != 0 THEN 'photoprimary'
													ELSE 'photoprimarycompl',
		photoobjall_discrG6 = CASE WHEN photoobjall_objid IN (SELECT photoobjall_objid FROM dorm_edbt_baseline.photoz) THEN 'photoz'
													ELSE 'photozcompl';													


UPDATE dorm_edbt_baseline.photospecall
SET specobjall_discrG3 = CASE WHEN specobjall_bestSpec = 1 THEN 'specobj'
												 ELSE 'specobjcompl';

										DO $$
DECLARE
    metadata JSONB;
BEGIN
    SELECT d.description::JSONB INTO metadata
    FROM pg_namespace n JOIN pg_description d ON d.objoid = n.oid
    WHERE n.nspname = 'dorm_edbt_baseline';

    EXECUTE format('COMMENT ON SCHEMA dorm_edbt_baseline IS %L', metadata || '{"has_data": true}');
END $$
