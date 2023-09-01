create table if not exists cdm.dm_courier_ledger
(
    courier_id           varchar                  not null,
    courier_name         varchar                  not null,
    settlement_year      integer                  not null,
    settlement_month     integer                  not null,
    orders_count         integer                  not null,
    orders_total_sum     numeric(14, 2) default 0 not null,
    rate_avg             numeric(14, 2) default 0 not null,
    order_processing_fee numeric(14, 2) default 0 not null,
    courier_tips_sum     numeric(14, 2) default 0 not null,
    courier_order_sum    numeric(14, 2) default 0 not null,
    courier_reward_sum   numeric(14, 2) default 0 not null,
    constraint courier_ledger
        unique (courier_id, settlement_year, settlement_month)
);

