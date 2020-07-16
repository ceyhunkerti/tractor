# ![Tractor](tractor.png) Tractor

Plugin based cross platfor data transfer utility.

Tractor is extendable data transfer utility between various source and target systems.
It utilizes input/output/and solo plugins to move data between providers and consumers.

### Installation

`pip install tractor`

Tractor stores the data transfer definitions, which are called **mapping** in YAML file.
Here is an example of a configuration file.
```yml
mappings:
- DemoLink:
    solo:
    - host: 192.168.1.123
      link: LNK_VM2
      password: tractor
      plugin: Oracle DB Link
      port: 1521
      service_name: orcl
      source: tractor.table_1
      source_columns: '*'
      target: tractor.table_1
      username: tractor
- DemoCSV:
    input:
    - columns: '*'
      fetch_size: 1000
      hint: ''
      host: 192.168.1.123
      password: tractor
      plugin: Oracle
      port: 1521
      service_name: orcl
      source_type: table
      table: table_3
      username: tractor
    output:
    - batch_size: 1000
      delimiter: ','
      file: /home/tractor/data/table_1.csv
      lineterminator: \r\n
      plugin: Csv
```


