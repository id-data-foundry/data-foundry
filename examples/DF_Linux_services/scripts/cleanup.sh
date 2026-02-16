#! /bin/sh

cd /home/data/scripts

# move the first backup of month to backup
find ./??????01*.zip -maxdepth 1 -mtime +30 -type f -exec mv "{}" backup \;

# delete files older than 30 days
find ./??????*.zip -maxdepth 1 -mtime +30 -type f -delete

