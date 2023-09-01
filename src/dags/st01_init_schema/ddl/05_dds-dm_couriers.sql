create table if not exists dds.dm_couriers
(
    id           serial,
    courier_id   varchar not null,
    courier_name varchar not null,
    constraint dm_couriers_pk
        primary key (id),
    constraint courier_id
        unique (courier_id)
);

