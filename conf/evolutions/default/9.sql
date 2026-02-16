-- add analytics tables

# --- !Ups
create table analytics (
id                            bigint auto_increment not null,
timestamp                     timestamp,
project_views                 integer not null,
project_updates               integer not null,
project_dataset_downloads     integer not null,
project_dataset_updates       integer not null,
project_website_views         integer not null,
project_participant_interactions integer not null,
project_script_invocations    integer not null,
project_id                    bigint,
constraint pk_analytics primary key (id)
);
create index ix_analytics_project_id on analytics (project_id);
alter table analytics add constraint fk_analytics_project_id foreign key (project_id) references project (id) on delete restrict on update restrict;
alter table person alter column identity TEXT;
alter table project alter column description TEXT;
alter table project alter column intro TEXT;

# --- !Downs
alter table project alter column intro varchar(255);
alter table project alter column description varchar(255);
alter table person alter column identity varchar(255);
alter table analytics drop constraint fk_analytics_project_id;
drop index if exists ix_analytics_project_id;
drop table if exists analytics;
