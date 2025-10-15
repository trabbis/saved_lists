SELECT 
    ROW_NUMBER() OVER (ORDER BY id ASC) as row_number,
    id, 
    user_id 
FROM public.lists
ORDER BY id ASC 
LIMIT 100