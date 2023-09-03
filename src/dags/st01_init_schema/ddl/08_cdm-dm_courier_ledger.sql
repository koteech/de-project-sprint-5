create table if not exists cdm.dm_courier_ledger
(
    id                   serial,
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
        unique (courier_id, settlement_year, settlement_month),
    constraint dm_courier_ledger_settlement_year_check
        check ((settlement_year >= 2020) AND (settlement_year < 2500)),
    constraint dm_courier_ledger_settlement_month_check
        check ((settlement_month >= 0) AND (settlement_month <= 12)),
    constraint cdm_dm_courier_ledger_orders_count_check
        check (orders_count >= 0),
    constraint cdm_dm_courier_ledger_orders_total_sum_check
        check (orders_total_sum >= 0.00),
    constraint cdm_dm_courier_ledger_rate_avg_check
        check (rate_avg >= 0.00),
    constraint cdm_dm_courier_ledger_order_processing_fee_check
        check (order_processing_fee >= 0.00),
    constraint cdm_dm_courier_ledger_courier_order_sum_check
        check (courier_order_sum >= 0.00),
    constraint cdm_dm_courier_ledger_courier_tips_sum_check
        check (courier_tips_sum >= 0.00),
    constraint cdm_dm_courier_ledger_courier_reward_sum_check
        check (courier_reward_sum >= 0.00)
);

