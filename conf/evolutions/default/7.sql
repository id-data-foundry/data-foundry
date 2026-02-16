-- added dataset type ENTITY

# --- !Ups
ALTER TABLE dataset DROP CONSTRAINT ck_dataset_ds_type;
ALTER TABLE dataset ADD CONSTRAINT ck_dataset_ds_type check ( ds_type in (0,1,2,3,4,5,6,7,8,9,10,11,12,13));

# --- !Downs
ALTER TABLE dataset DROP CONSTRAINT ck_dataset_ds_type;
ALTER TABLE dataset ADD CONSTRAINT ck_dataset_ds_type check ( ds_type in (0,1,2,3,4,5,6,7,8,9,10,11,12));
