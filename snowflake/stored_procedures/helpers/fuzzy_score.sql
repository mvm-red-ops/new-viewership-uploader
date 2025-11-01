create or replace function upload_db.public.fuzzy_score(a String, b String)
returns number
strict immutable
COMMENT = 'Takes two strings and returns a similarity score between 1 and 0'
as 'select 1.0-(editdistance(a, b)/greatest(length(a),length(b)))';