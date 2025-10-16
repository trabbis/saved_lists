-- Getting save lists
/* if requires for integer values, this might help
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

SELECT 
   	id AS list_id,
   	user_id AS borrower_id,
   	REGEXP_REPLACE(name, '[,\\s]+', ' ', 'g') AS name,
   	description,
   	created_at AS date_created,
   	modified_at AS date_updated,
	CASE WHEN share_token IS NOT NULL THEN 1 ELSE 0 END AS public_list

FROM public.lists
LIMIT 100




--TODO Getting saved items

SELECT 
	il.list_id AS list_id,
    i.record_id AS bib_id,
    i.created_at AS date_added
--            i.id AS item_id,
--	l.*, i.* 
	FROM public.items i --taking 3 mins
	LEFT JOIN item_lists il on i.id = il.item_id
	LEFT JOIN lists l on l.id = il.list_id
    WHERE MOD( ('x'||SUBSTR(MD5(i.record_id::text),1,8))::bit(32)::int, 6 ) = 0; -- 0..5

--	WHERE i.user_id = '4824671'  --Doug
--ORDER BY i.id ASC LIMIT 100



