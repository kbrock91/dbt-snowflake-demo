with source_data as 

(
    select
        1 as site_id,
        1 as account_nbr,
        'segment 1' as segment_name,
        123 as val,  
        to_date('2023-10-12') as update_timestamp
 
    union 
    select
        2 as site_id,
        11 as account_nbr,
        'segment 2' as segment_name,
        1234 as val,  
        to_date('2023-10-12') as update_timestamp

            union 
    select
        2 as site_id,
        33 as account_nbr,
        'segment 2' as segment_name,
        1234 as val,  
        to_date('2023-10-12') as update_timestamp

)


select * from source_data
