# Datadog module Flydata
Datadog module for monitoring replication between MySQLd and Amazon Redshift database.
Module is counting rows in each database.

## Usage

In Datadog create multimetric alert that is separated by table. Under "Define the Metric" add two rows, one for the MySQLd and other for the Flydata.
Express this queries can be ```a - b``` or ```( ( a - b ) / ( ( a + b ) / 2 ) ) * 100``` depending on what kind alert do you want to create.
