create or replace function UPLOAD_DB_PROD.public.fuzzy_score(a String, b String)
returns number
strict immutable
COMMENT = 'Takes two strings and returns a similarity score between 1 and 0'
as 'select 1.0-(editdistance(a, b)/greatest(length(a),length(b)))';