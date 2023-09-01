create table if not exists dds.dm_timestamps
(
    id    serial,
    ts    timestamp not null,
    year  integer   not null,
    month integer   not null,
    day   integer   not null,
    time  time      not null,
    date  date      not null,
    constraint dm_timestamps_pkey
        primary key (id),
    constraint dm_timestamps_ts_key
        unique (ts),
    constraint dm_timestamps_year_check
        check ((year >= 2020) AND (year < 2500)),
    constraint dm_timestamps_month_check
        check ((month >= 0) AND (month <= 12)),
    constraint dm_timestamps_day_check
        check ((day >= 0) AND (day <= 31))
);

