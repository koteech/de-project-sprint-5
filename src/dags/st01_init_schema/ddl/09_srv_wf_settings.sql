create table if not exists stg.srv_wf_settings
(
    id                integer generated always as identity,
    workflow_key      varchar not null,
    workflow_settings json    not null,
    constraint srv_wf_settings_pkey
        primary key (id),
    constraint srv_wf_settings_workflow_key_key
        unique (workflow_key)
);


create table if not exists dds.srv_wf_settings
(
    id                integer generated always as identity,
    workflow_key      varchar not null,
    workflow_settings json    not null,
    constraint srv_wf_settings_pkey
        primary key (id),
    constraint srv_wf_settings_workflow_key_key
        unique (workflow_key)
);



create table if not exists cdm.srv_wf_settings
(
    id                integer generated always as identity,
    workflow_key      varchar not null,
    workflow_settings json    not null,
    constraint srv_wf_settings_pkey
        primary key (id),
    constraint srv_wf_settings_workflow_key_key
        unique (workflow_key)
);


