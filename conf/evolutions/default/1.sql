# --- !Ups

create table cluster (
  id                            bigint auto_increment not null,
  ref_id                        varchar(255),
  name                          varchar(255),
  project_id                    bigint,
  constraint pk_cluster primary key (id)
);

create table cluster_device (
  cluster_id                    bigint not null,
  device_id                     bigint not null,
  constraint pk_cluster_device primary key (cluster_id,device_id)
);

create table cluster_participant (
  cluster_id                    bigint not null,
  participant_id                bigint not null,
  constraint pk_cluster_participant primary key (cluster_id,participant_id)
);

create table cluster_wearable (
  cluster_id                    bigint not null,
  wearable_id                   bigint not null,
  constraint pk_cluster_wearable primary key (cluster_id,wearable_id)
);

create table collaboration (
  id                            bigint auto_increment not null,
  collaborator_id               bigint,
  project_id                    bigint,
  status                        integer,
  created                       timestamp,
  constraint ck_collaboration_status check ( status in (0,1,2)),
  constraint pk_collaboration primary key (id)
);

create table dataset (
  id                            bigint auto_increment not null,
  name                          varchar(255),
  ref_id                        varchar(255),
  api_token                     varchar(255),
  ds_type                       integer,
  collector_type                varchar(255),
  configuration                 clob,
  open_participation            boolean default false not null,
  description                   varchar(255),
  target_object                 varchar(255),
  start                         timestamp,
  end                           timestamp,
  project_id                    bigint,
  creation                      timestamp not null,
  constraint ck_dataset_ds_type check ( ds_type in (0,1,2,3,4,5,6,7,8)),
  constraint pk_dataset primary key (id)
);

create table device (
  id                            bigint auto_increment not null,
  name                          varchar(255),
  ref_id                        varchar(255),
  category                      varchar(255),
  subtype                       varchar(255),
  ip_address                    varchar(255),
  location                      varchar(255),
  configuration                 varchar(255),
  project_id                    bigint,
  public_parameter1             varchar(255),
  public_parameter2             varchar(255),
  public_parameter3             varchar(255),
  constraint pk_device primary key (id)
);

create table lab_notes_entry (
  id                            bigint auto_increment not null,
  source                        varchar(255),
  log_type                      integer,
  message                       varchar(255),
  timestamp                     timestamp,
  project_id                    bigint,
  dataset_id                    bigint,
  constraint ck_lab_notes_entry_log_type check ( log_type in (0,1,2,3,4,5,6)),
  constraint pk_lab_notes_entry primary key (id)
);

create table participant (
  id                            bigint auto_increment not null,
  ref_id                        varchar(255),
  firstname                     varchar(255),
  lastname                      varchar(255),
  gender                        integer not null,
  email                         varchar(255),
  age_range                     integer not null,
  career                        varchar(255),
  status                        integer,
  project_id                    bigint,
  creation                      timestamp,
  password_hash                 varchar(255),
  public_parameter1             varchar(255),
  public_parameter2             varchar(255),
  public_parameter3             varchar(255),
  constraint ck_participant_status check ( status in (0,1,2)),
  constraint pk_participant primary key (id)
);

create table person (
  id                            bigint auto_increment not null,
  user_id                       varchar(255),
  identity                      varchar(255),
  firstname                     varchar(255),
  lastname                      varchar(255),
  email                         varchar(255),
  website                       varchar(255),
  accesscode                    varchar(255),
  creation                      timestamp,
  password_hash                 varchar(255),
  constraint pk_person primary key (id)
);

create table project (
  id                            bigint auto_increment not null,
  name                          varchar(255),
  ref_id                        varchar(255),
  intro                         varchar(255),
  start                         timestamp,
  end                           timestamp,
  owner_id                      bigint,
  creation                      timestamp,
  archived_project              boolean default false not null,
  public_project                boolean default false not null,
  shareable_project             boolean default false not null,
  signup_open                   boolean default false not null,
  license                       varchar(255),
  constraint pk_project primary key (id)
);

create table subscription (
  id                            bigint auto_increment not null,
  subscriber_id                 bigint,
  project_id                    bigint,
  created                       timestamp,
  constraint pk_subscription primary key (id)
);

create table wearable (
  id                            bigint auto_increment not null,
  name                          varchar(255),
  ref_id                        varchar(255),
  brand                         varchar(255),
  configuration                 varchar(255),
  api_token                     varchar(255),
  api_key                       varchar(255),
  project_id                    bigint,
  public_parameter1             varchar(255),
  public_parameter2             varchar(255),
  public_parameter3             varchar(255),
  constraint pk_wearable primary key (id)
);

create index ix_cluster_project_id on cluster (project_id);
alter table cluster add constraint fk_cluster_project_id foreign key (project_id) references project (id) on delete restrict on update restrict;

create index ix_cluster_device_cluster on cluster_device (cluster_id);
alter table cluster_device add constraint fk_cluster_device_cluster foreign key (cluster_id) references cluster (id) on delete restrict on update restrict;

create index ix_cluster_device_device on cluster_device (device_id);
alter table cluster_device add constraint fk_cluster_device_device foreign key (device_id) references device (id) on delete restrict on update restrict;

create index ix_cluster_participant_cluster on cluster_participant (cluster_id);
alter table cluster_participant add constraint fk_cluster_participant_cluster foreign key (cluster_id) references cluster (id) on delete restrict on update restrict;

create index ix_cluster_participant_participant on cluster_participant (participant_id);
alter table cluster_participant add constraint fk_cluster_participant_participant foreign key (participant_id) references participant (id) on delete restrict on update restrict;

create index ix_cluster_wearable_cluster on cluster_wearable (cluster_id);
alter table cluster_wearable add constraint fk_cluster_wearable_cluster foreign key (cluster_id) references cluster (id) on delete restrict on update restrict;

create index ix_cluster_wearable_wearable on cluster_wearable (wearable_id);
alter table cluster_wearable add constraint fk_cluster_wearable_wearable foreign key (wearable_id) references wearable (id) on delete restrict on update restrict;

create index ix_collaboration_collaborator_id on collaboration (collaborator_id);
alter table collaboration add constraint fk_collaboration_collaborator_id foreign key (collaborator_id) references person (id) on delete restrict on update restrict;

create index ix_collaboration_project_id on collaboration (project_id);
alter table collaboration add constraint fk_collaboration_project_id foreign key (project_id) references project (id) on delete restrict on update restrict;

create index ix_dataset_project_id on dataset (project_id);
alter table dataset add constraint fk_dataset_project_id foreign key (project_id) references project (id) on delete restrict on update restrict;

create index ix_device_project_id on device (project_id);
alter table device add constraint fk_device_project_id foreign key (project_id) references project (id) on delete restrict on update restrict;

create index ix_lab_notes_entry_project_id on lab_notes_entry (project_id);
alter table lab_notes_entry add constraint fk_lab_notes_entry_project_id foreign key (project_id) references project (id) on delete restrict on update restrict;

create index ix_lab_notes_entry_dataset_id on lab_notes_entry (dataset_id);
alter table lab_notes_entry add constraint fk_lab_notes_entry_dataset_id foreign key (dataset_id) references dataset (id) on delete restrict on update restrict;

create index ix_participant_project_id on participant (project_id);
alter table participant add constraint fk_participant_project_id foreign key (project_id) references project (id) on delete restrict on update restrict;

create index ix_project_owner_id on project (owner_id);
alter table project add constraint fk_project_owner_id foreign key (owner_id) references person (id) on delete restrict on update restrict;

create index ix_subscription_subscriber_id on subscription (subscriber_id);
alter table subscription add constraint fk_subscription_subscriber_id foreign key (subscriber_id) references person (id) on delete restrict on update restrict;

create index ix_subscription_project_id on subscription (project_id);
alter table subscription add constraint fk_subscription_project_id foreign key (project_id) references project (id) on delete restrict on update restrict;

create index ix_wearable_project_id on wearable (project_id);
alter table wearable add constraint fk_wearable_project_id foreign key (project_id) references project (id) on delete restrict on update restrict;


# --- !Downs

alter table cluster drop constraint if exists fk_cluster_project_id;
drop index if exists ix_cluster_project_id;

alter table cluster_device drop constraint if exists fk_cluster_device_cluster;
drop index if exists ix_cluster_device_cluster;

alter table cluster_device drop constraint if exists fk_cluster_device_device;
drop index if exists ix_cluster_device_device;

alter table cluster_participant drop constraint if exists fk_cluster_participant_cluster;
drop index if exists ix_cluster_participant_cluster;

alter table cluster_participant drop constraint if exists fk_cluster_participant_participant;
drop index if exists ix_cluster_participant_participant;

alter table cluster_wearable drop constraint if exists fk_cluster_wearable_cluster;
drop index if exists ix_cluster_wearable_cluster;

alter table cluster_wearable drop constraint if exists fk_cluster_wearable_wearable;
drop index if exists ix_cluster_wearable_wearable;

alter table collaboration drop constraint if exists fk_collaboration_collaborator_id;
drop index if exists ix_collaboration_collaborator_id;

alter table collaboration drop constraint if exists fk_collaboration_project_id;
drop index if exists ix_collaboration_project_id;

alter table dataset drop constraint if exists fk_dataset_project_id;
drop index if exists ix_dataset_project_id;

alter table device drop constraint if exists fk_device_project_id;
drop index if exists ix_device_project_id;

alter table lab_notes_entry drop constraint if exists fk_lab_notes_entry_project_id;
drop index if exists ix_lab_notes_entry_project_id;

alter table lab_notes_entry drop constraint if exists fk_lab_notes_entry_dataset_id;
drop index if exists ix_lab_notes_entry_dataset_id;

alter table participant drop constraint if exists fk_participant_project_id;
drop index if exists ix_participant_project_id;

alter table project drop constraint if exists fk_project_owner_id;
drop index if exists ix_project_owner_id;

alter table subscription drop constraint if exists fk_subscription_subscriber_id;
drop index if exists ix_subscription_subscriber_id;

alter table subscription drop constraint if exists fk_subscription_project_id;
drop index if exists ix_subscription_project_id;

alter table wearable drop constraint if exists fk_wearable_project_id;
drop index if exists ix_wearable_project_id;

drop table if exists cluster;

drop table if exists cluster_device;

drop table if exists cluster_participant;

drop table if exists cluster_wearable;

drop table if exists collaboration;

drop table if exists dataset;

drop table if exists device;

drop table if exists lab_notes_entry;

drop table if exists participant;

drop table if exists person;

drop table if exists project;

drop table if exists subscription;

drop table if exists wearable;

