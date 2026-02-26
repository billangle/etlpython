#! /bin/sh

aws glue get-tables --database-name sss-farmrecords \
  --query 'TableList[].Name' --output table --region us-east-1

aws glue list-crawlers --region us-east-1 --output table