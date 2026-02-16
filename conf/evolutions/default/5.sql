-- change project and dataset tables

# --- !Ups
ALTER TABLE project ADD COLUMN description VARCHAR(1000);
ALTER TABLE project ADD COLUMN keywords VARCHAR(255);
ALTER TABLE project ADD COLUMN doi VARCHAR(255);
ALTER TABLE project ADD COLUMN relation VARCHAR(255);
ALTER TABLE project ADD COLUMN organization VARCHAR(255);
ALTER TABLE project ADD COLUMN remarks VARCHAR(1000);
ALTER TABLE dataset ADD COLUMN keywords VARCHAR(255);
ALTER TABLE dataset ADD COLUMN doi VARCHAR(255);
ALTER TABLE dataset ADD COLUMN relation VARCHAR(255);
ALTER TABLE dataset ADD COLUMN organization VARCHAR(255);
ALTER TABLE dataset ADD COLUMN remarks VARCHAR(1000);
ALTER TABLE dataset ADD COLUMN license VARCHAR(1000);

# --- !Downs
ALTER TABLE project DROP COLUMN description;
ALTER TABLE project DROP COLUMN keywords;
ALTER TABLE project DROP COLUMN doi;
ALTER TABLE project DROP COLUMN relation;
ALTER TABLE project DROP COLUMN organization;
ALTER TABLE project DROP COLUMN remarks;
ALTER TABLE dataset DROP COLUMN keywords;
ALTER TABLE dataset DROP COLUMN doi;
ALTER TABLE dataset DROP COLUMN relation;
ALTER TABLE dataset DROP COLUMN organization;
ALTER TABLE dataset DROP COLUMN remarks;
ALTER TABLE dataset DROP COLUMN license;
