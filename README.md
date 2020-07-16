# ![Tractor](tractor.png) Tractor

Plugin based cross platform data transfer utility.

Tractor is extendable data transfer utility between various source and target systems.
It utilizes input/output/and solo plugins to move data between providers and consumers.

## Installation

`pip install tractor`

## Configuration

Tractor stores the data transfer definitions, which are called **mapping** in YAML file.
Here is an example of a configuration file.
```yml
mappings:
- DemoLink: # mapping name
    solo:   # plugin type
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

The configuration file location is controlled by the `TRACTOR_CONFIG_FILE` environment variable. It defaults to
`tractor.yml` in the current working directory.
For example in linux you can change the config file location with;
```sh
export TRACTOR_CONFIG_FILE=/path/to/config.yml
```

### Configuration file format
```yml
# the skeleton of config file is like this;
# mappings: { [
    # { mapping_name: { mapping_body } },
    # { mapping_name: { mapping_body } },
    # ...
# ] }

# "mappings" : root of the mapping definitions
# "mapping_name" : Unique name of mapping
# "mapping_body" : contains input/output and solo plugin definitions
#  mapping_body is defined like this:
#  mapping_body is defined like this:
#       input:  [list of input definitions]
#       output: [list of output definitions]
#      *solo:   [list of solo definitions]

# transfer definition is called "mapping"
# list of mapping definitions are kept under "mappings" key
mappings:
# each mapping have the mapping name as the key and an further config details as value.
- Mapiing Name:
    input:
    - prop 1: value 1
      prop 2: value 2
      ...
    - prop 1: value 1
      ...
      prop n: value n
    output:
    - prop 1: value 1
      prop 2: value 2
      ...
    - prop 1: value 1
      ...
      prop n: value n
    solo:
    - prop 1: value 1
      prop 2: value 2
      ...
    - prop 1: value 1
      ...
      prop n: value n
```

#### Input/Output and Solo
Each mapping definition contains the combination of `input` `output` and `solo`
config transfer definitions.
```yml
mappings:
    - Mapping name:
        input:
        - ...
        output:
        - ...
        solo:
        - ...
```


**input** key contains the list of source definitions for producing data
**output** key contains the list of source definitions for consuming the data
produced by the sources that are defined in `input` section
**solo** is different from input and output and does not depend any input and does not
provide any output to other consumers. It is a single step that runs a transfer definition.
A good example for this maybe `oracle_dblink`. It does not depend any outside data and runs the
task defined in `solo`



