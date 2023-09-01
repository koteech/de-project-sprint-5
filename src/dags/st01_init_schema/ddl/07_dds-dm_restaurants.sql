create table if not exists dds.dm_deliveries
(
    id          serial,
    order_id    varchar                  not null,
    order_ts    timestamp                not null,
    delivery_id varchar                  not null,
    courier_id  varchar                  not null,
    address     varchar                  not null,
    delivery_ts timestamp                not null,
    rate        smallint                 not null,
    sum         numeric(14, 2) default 0 not null,
    tip_sum     numeric(14, 2) default 0 not null,
    constraint deliveries_pkey
        primary key (id),
    constraint fk_courier
        foreign key (courier_id) references dds.dm_couriers (courier_id)
            on delete cascade
);

