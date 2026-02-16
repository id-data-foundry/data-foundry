-- change project table, added dataset types

# --- !Ups
ALTER TABLE dataset MODIFY COLUMN description VARCHAR(1000);
ALTER TABLE project MODIFY COLUMN intro VARCHAR(1000);
ALTER TABLE dataset DROP CONSTRAINT ck_dataset_ds_type;
ALTER TABLE dataset ADD CONSTRAINT ck_dataset_ds_type check ( ds_type in (0,1,2,3,4,5,6,7,8,9,10,11,12));

# --- !Downs
ALTER TABLE dataset MODIFY COLUMN description VARCHAR(255);
ALTER TABLE project MODIFY COLUMN intro VARCHAR(255);
ALTER TABLE dataset DROP CONSTRAINT ck_dataset_ds_type;
ALTER TABLE dataset ADD CONSTRAINT ck_dataset_ds_type check ( ds_type in (0,1,2,3,4,5,6,7,8,9,10));
