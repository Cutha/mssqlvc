# mssqlvc

This tool scripts the following objects from a SQL Server database:

- Tables
- Views
- Stored Procedures
- Functions

Instead of a simple scripter, the main purpose of this tool is to help keep the scripts under version control.

Every time after the program is run, it will write the current timestamp into the config file `config.json`.
When it is run next time, it will only script updated objects since the last timestamp.

## Quick Start ##

1. Create a `config.json` with the following structure:

```json
{
    "connection": {
        "server": "192.168.1.1",
        "database": "dbname",
        "user": "admin",
        "password": "password"
    },
    "lastSyncTime": "2019-01-01T13:27:54.883"
}
```

Fill in your connection info. For first-time run, leave `lastSyncTime` blank.

2. Run `mssqlvc` in shell, under the same directory as `config.json`.

## Remarks ##

1. The `lastSyncTime` does not contain time zone. It simply uses the time zone of the database server.
