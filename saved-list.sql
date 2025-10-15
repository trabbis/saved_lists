-- Getting save lists

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


