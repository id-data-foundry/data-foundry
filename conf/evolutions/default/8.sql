-- change participant and person tables; add telegram_session table

# --- !Ups
ALTER TABLE participant ADD COLUMN identity VARCHAR(1000);
ALTER TABLE person ALTER COLUMN identity VARCHAR(1000);
create table telegram_session (
  id                            bigint auto_increment not null,
  state                         varchar(255),
  action                        varchar(255),
  last_action                   timestamp,
  active_project_id             bigint not null,
  chat_id                       bigint not null,
  username                      varchar(255),
  email                         varchar(255),
  token                         varchar(255),
  messages                      clob,
  constraint pk_telegram_session primary key (id)
);

# --- !Downs
ALTER TABLE participant DROP COLUMN identity;
ALTER TABLE person ALTER COLUMN identity VARCHAR(255);
drop table if exists telegram_session;

