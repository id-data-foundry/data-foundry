-- change wearable table

# --- !Ups
ALTER TABLE wearable ADD COLUMN scopes VARCHAR(255);
ALTER TABLE wearable ADD COLUMN expiry BIGINT;
ALTER TABLE wearable ADD COLUMN user_id VARCHAR(255);
ALTER TABLE dataset DROP CONSTRAINT ck_dataset_ds_type;
ALTER TABLE dataset ADD CONSTRAINT ck_dataset_ds_type check ( ds_type in (0,1,2,3,4,5,6,7,8,9,10));

# --- !Downs
ALTER TABLE wearable DROP COLUMN scopes;
ALTER TABLE wearable DROP COLUMN expiry;
ALTER TABLE wearable DROP COLUMN user_id;
ALTER TABLE dataset DROP CONSTRAINT ck_dataset_ds_type;
ALTER TABLE dataset ADD CONSTRAINT ck_dataset_ds_type check ( ds_type in (0,1,2,3,4,5,6,7,8));
