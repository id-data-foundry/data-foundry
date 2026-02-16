-- change person table

# --- !Ups
ALTER TABLE person ADD COLUMN last_action timestamp;

# --- !Downs
ALTER TABLE person DROP COLUMN last_action;
