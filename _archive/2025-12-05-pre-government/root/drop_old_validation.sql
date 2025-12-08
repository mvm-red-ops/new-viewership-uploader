-- Drop old validation procedure before deploying new one
DROP PROCEDURE IF EXISTS UPLOAD_DB.public.validate_viewership_for_insert(VARCHAR, VARCHAR);
