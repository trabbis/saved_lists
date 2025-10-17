---------------------------------------
-- Getting saved lists table
---------------------------------------

/*
SELECT * FROM public.lists
ORDER BY id ASC LIMIT 100


SELECT
  id,
  COUNT(*) AS count
FROM public.lists
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY id;

SELECT count(*) FROM public.lists  --64228

--if requires for integer values, this might help
SELECT ROW_NUMBER() OVER (ORDER BY l.id ASC) as row_number,
    l.id, 
    l.user_id,
    l.*,
    COUNT(il.item_id) as item_count
FROM public.lists l
    INNER JOIN item_lists il on l.id = il.list_id
GROUP BY l.id, l.user_id, l.*
ORDER BY l.id ASC 
LIMIT 100
*/


--Character encoding issue fixed (UTF-8)
--Exporting into multiples files because of the limited size in cvs/excel files
--Cut-off day/time??? Data keep growing

SELECT  --64,243
   	id AS list_id,
   	user_id AS borrower_id,
   	REGEXP_REPLACE(name, '[,\\s]+', ' ', 'g') AS name,
	REGEXP_REPLACE(
	    REPLACE(description, U&'\200B', ''), E'[\r\n]+', ' ', 'g') AS description,
   	created_at AS date_created,
   	modified_at AS date_updated,
	CASE WHEN share_token IS NOT NULL THEN 1 ELSE 0 END AS public_list

FROM public.lists
--LIMIT 100

	
	



---------------------------------------
-- Getting saved items table
---------------------------------------
	/*
	SELECT count(*) FROM public.items --5,952,027

	SELECT count(i.*) FROM public.items i --1,150,921
	LEFT JOIN item_lists il on i.id = il.item_id
	LEFT JOIN lists l on l.id = il.list_id

SELECT count(*)   --6,131,103
	FROM public.items i
	LEFT JOIN item_lists il on i.id = il.item_id
	LEFT JOIN lists l on l.id = il.list_id
*/	


--Getting only over one milion records, so much 6 times
SELECT 
	il.list_id AS list_id,
    i.record_id AS bib_id,
    i.created_at AS date_added
--            i.id AS item_id,
--	l.*, i.* 
 	FROM public.items i --taking 3 mins --> now around 20 secs
	LEFT JOIN item_lists il on i.id = il.item_id
	LEFT JOIN lists l on l.id = il.list_id
WHERE MOD( ('x'||SUBSTR(MD5(i.record_id::text),1,8))::bit(32)::int, 6 ) = 0; -- 0..5

--	WHERE i.user_id = '4824671'  --Doug
--ORDER BY i.id ASC LIMIT 100



---------------------------------------
--psql commands
---------------------------------------
show server_encoding;
