-- changed labnotes, added dataset types

# --- !Ups
ALTER TABLE lab_notes_entry MODIFY COLUMN message TEXT;
ALTER TABLE lab_notes_entry DROP CONSTRAINT ck_lab_notes_entry_log_type;
ALTER TABLE lab_notes_entry ADD CONSTRAINT ck_lab_notes_entry_log_type check ( log_type in (0,1,2,3,4,5,6,7,8));



# --- !Downs
ALTER TABLE lab_notes_entry MODIFY COLUMN message VARCHAR(255);
ALTER TABLE lab_notes_entry DROP CONSTRAINT ck_lab_notes_entry_log_type;
ALTER TABLE lab_notes_entry ADD CONSTRAINT ck_lab_notes_entry_log_type check ( log_type in (0,1,2,3,4,5,6));
