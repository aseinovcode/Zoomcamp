with trips as (
    select 
    to_hex(md5(
        concat(
            coalesce(cast(vendor_id        as string), ''),
            '|', coalesce(cast(pickup_datetime  as string), ''),
            '|', coalesce(cast(dropoff_datetime as string), ''),
            '|', coalesce(cast(pulocation_id    as string), ''),
            '|', coalesce(cast(dolocation_id    as string), ''),
            '|', coalesce(cast(trip_distance    as string), ''),
            '|', coalesce(cast(total_amount     as string), ''),
            '|', coalesce(cast(trip_type        as string), '')
        ))) as trip_id,
        vendor_id,Ratecode_id,PULocation_id,DOLocation_id,
        
        pickup_datetime,dropoff_datetime,store_and_fwd_flag,
        passenger_count,trip_distance,trip_type,

        fare_amount,extra,mta_tax,tip_amount,tolls_amount,
        ehail_fee,improvement_surcharge,total_amount,payment_type

    from {{ref('int_trips_unioned')}}
), 
unique_trips as (
    select row_number() over (partition by trip_id), * from trips
), 
final_table as (
    select 
    trip_id, vendor_id,Ratecode_id,PULocation_id,DOLocation_id,
        
    pickup_datetime,dropoff_datetime,store_and_fwd_flag,
    passenger_count,trip_distance,trip_type,

    fare_amount,extra,mta_tax,tip_amount,tolls_amount,
    ehail_fee,improvement_surcharge, ut.payment_type, pt.description,total_amount
    from unique_trips as ut left join {{ref('payment_type_lookup')}} as pt on ut.payment_type = pt.payment_type
)
select * from final_table