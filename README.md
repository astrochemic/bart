# BART
The Broadcast Anomaly Review Tool (BART) is a Smart Integrated Method Periodically Scanning Our Network for signs of missed revenue. I wrote this code at Sam Media after we ran into several cases of subscription fees not being collected correctly for large groups of subscribers.

BART performs an automated check once a week on all of our billing attempts (a.k.a. "broadcasts"). It looks up the active users per country, gateway, operator and service; finds the number of transactions per user in the past week; and prints a summary of the analysis to an Excel file (copied as a Google Spreadsheet). The Excel file is emailed to the data team for manual followup as needed. This process has proven to be helpful in identifying connections with unbilled users or other problems.

The script can be configured through a Google Spreadsheet, where we can define the billing frequency and the expected minimum and maximum number of transactions per user per week.
