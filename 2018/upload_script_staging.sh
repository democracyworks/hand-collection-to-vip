#!/bin/bash

BUCKET="data-dashboard-staging-unprocessed"
FILE=$1
~/.local/bin/aws s3 cp $FILE s3://$BUCKET/$FILE
~/.local/bin/aws sqs send-message --queue-url https://sqs.us-east-1.amazonaws.com/858394542481/data-dashboard-staging --message-body '{:filename "'$FILE'" :bucket "'$BUCKET'"}'
