# What is this?
------------------------------------------------------------------------

It is a python module to generate dummy logs for known application and devices
This app sends generated logs to syslog/udp on given host and port. Configure
the syslog host and port in `setup.cfg`


# Includes
------------------------------------------------------------------------
1. Sonicwall firewall logs generator
2. AWS log generator
3. MSSQL log generator
4. Fortigate firewall log generator
5. Checkpoint firewall log generator

# How to use
-------------------------------------------------------------------------
1. Every module has parameter support, use  `--help` for possible option list
2. Set the required parameters such log count you need, output file path
3. run individual modules at root level to start generating log.
4. Log will be generated according to start and end date you provided
5. At the end, the log file will be gzipped. It can be found in the same destination folder
