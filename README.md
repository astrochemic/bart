# BART
The Broadcast Anomaly Review Tool (BART) is a Smart Integrated Method Periodically Scanning Our Network for signs of missed revenue.

BART performs an automated check once a week on all of our billing attempts (a.k.a. "broadcasts"). It finds the active users on each of our two platforms (per country, gateway, operator, service_identifier1 and service_identifier2), finds the number of transactions per user in the past week, and prints all of this to an Excel file (copied as a Google Spreadsheet). This allows us to identify connections with unbilled users or other problems.

The script can be configured through a Google Spreadsheet, where we can define the billing frequency and the expected minimum and maximum number of transactions per user per week.
