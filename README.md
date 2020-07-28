# ![Tractor](tractor.png) Tractor

Plugin based cross platform data transfer utility.

Tractor is extendable data transfer utility between various source and target systems.
It utilizes input and output  plugins to move data between providers and consumers.

## Installation

`pip install tractor`

## Configuration

Tractor stores the data transfer definitions, which are called **mapping** in YAML file.
Here is an example of a configuration file.
```yml
mappings:
- Demo:
    input:
        plugin: Oracle
        host: 192.168.1.196
        username: tractor
        password: tractor
        service_name: orcl
        columns: "*"
        table: table_3
    output:
        plugin: Oracle
        host: 192.168.1.196
        username: tractor
        password: tractor
        table: table_5
        columns:
            - name: A
              type: number
            - name: B
              type: date
            - name: C
              type: varchar2(100)
        service_name: orcl
        truncate: True
- DemoCsv:
    input:
        plugin: Oracle
        host: 192.168.1.196
        username: tractor
        password: tractor
        service_name: orcl
        columns: "*"
        table: table_3
    output:
        plugin: Csv
        file: /home/ceyhun/projects/lab/tractor/play/table_1.csv
```

The configuration file location is controlled by the `TRACTOR_CONFIG_FILE` environment variable. It defaults to
`tractor.yml` in the current working directory.
For example in linux you can change the config file location with;
```sh
export TRACTOR_CONFIG_FILE=/path/to/config.yml
```


# Usage
Running mappings:
```sh
tractor run <mapping name>
# or
tractor run <mapping file>
```

### Logging
Logging is controlled by `TRACTOR_LOG_LEVEL` environment variable.
[See log levels](https://docs.python.org/3/library/logging.html#logging-levels)

### Plugins

- **Oracle** `pip install cx_Oracle`
- **MsSql** `pip install pymssql`
- **Hana** `pip install hdbcli`

#### Input Plugins

**Hana**

Requirements:
- `hdbcli`

You can install requirements with:
- `pip install hdbcli`

- **host**: host name or ip address
- **port**: (optional) default 30015
- **username**: connection user
- **username**: connection password

Source can be specified with 3 different ways:
- **table**: name of the source table
- **columns**: list of comma seperated columns

or

- **query**: select query to execute on source

or

- **query**: path to query file on the server where your run *tractor*


```yml
    input:
        host: 192.168.68.102
        port: 30044
        username: XPRIMEIT
        password: xPrimeiT4384

        table: owner.table
        columns: col_1, col_2
        # or
        query: select col_1, col_2 from owner.table
        # or
        query: /path/to/query.sql
```

