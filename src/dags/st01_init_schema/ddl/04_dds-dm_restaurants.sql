create table if not exists dds.dm_restaurants
(
    id              serial,
    restaurant_id   varchar not null,
    restaurant_name varchar not null
);
