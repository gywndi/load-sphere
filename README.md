# load-sphere

1. Tool for data migration
2. Convert SQL result set to several queries
3. ID generation for target table


## build & run
    mvn install
    java -jar target/load-sphere-0.0.1-jar-with-dependencies.jar --config-file="config-mysql.yml"

## Usage
    Missing required option: '--confi-file=<configFile>'
    Usage: <main class> --confi-file=<configFile> [--export-query=<exportQuery>]
                        [--source-sharding-config=<sourceShardingConfig>]
                        [--target-columns=<targetColumns>]
                        [--target-delete-query=<targetDeleteQuery>]
                        [--target-sharding-config=<targetShardingConfig>]
                        [--target-table=<targetTable>] [--workers=<workers>]
          --confi-file=<configFile>
                                Config file
          --export-query=<exportQuery>
                                Export query for source database
          --source-sharding-config=<sourceShardingConfig>
                                Source sharding datasource config file
          --target-columns=<targetColumns>
                                Target columns name(Seperated by comma)
          --target-delete-query=<targetDeleteQuery>
                                Target delete query
          --target-sharding-config=<targetShardingConfig>
                                Target sharding datasource config file
          --target-table=<targetTable>
                                Target table name
          --workers=<workers>   Wokers for loading target


## configuration (config-mysql.yml)
The lower setting is an example of the following operation.
1. Fetch from sourceDS
2. Generate new ID with idGenerator
3. Split result to uldra and uldra_part
(sourceDS! **useCursorFetch=true** is important to avoid OOM) 

    workers: 8
    sourceDS: !!org.apache.commons.dbcp2.BasicDataSource
      url: jdbc:mysql://127.0.0.1:3306/origin?autoReconnect=true&useCursorFetch=true
      username: origin
      password: origin
    
    targetDS: !!org.apache.commons.dbcp2.BasicDataSource
      url: jdbc:mysql://127.0.0.1:3306/shard?autoReconnect=true&useCursorFetch=true
      username: shard
      password: shard
      maxTotal: 30
      maxWaitMillis: 100
      validationQuery: SELECT 1
      testOnBorrow: false
      testOnReturn: false
      testWhileIdle: true
      timeBetweenEvictionRunsMillis: 60000
      minEvictableIdleTimeMillis : 1200000
      numTestsPerEvictionRun : 10
    
    exportQuery: "select * from uldra where 1 = 1"
    idGenerator:
      className: "net.gywn.algorithm.IDGeneratorHandlerImpl"
      params: ["dttm"]
      columnName: "guid"
    upsert: true
    targetTables:
    - name: "uldra"
      deleteQuery: "delete from uldra limit 10000"
    - name: "uldra_part"
      columns: ["id", "name", "dttm"]
      deleteQuery: "delete from uldra_part limit 10"
      

## result
    ###########################################
    # SOURCE
    ###########################################
    mysql> select * from origin.uldra limit 5;
    +----+-----------------+----------------------------------+---------------------+
    | id | name            | cont                             | dttm                |
    +----+-----------------+----------------------------------+---------------------+
    |  1 | 06b1ddef755c6e9 | 63c9b12308740990e1636c87c09d76f4 | 2020-08-21 13:10:48 |
    |  2 | ff4d1852ff185d6 | b29cc559540451e93d205cbf2b0be578 | 2020-08-21 13:10:48 |
    |  3 | 0bb23b3546ccc37 | 977d181fddae1d76d0fe2ca538e1aa56 | 2020-08-21 13:10:48 |
    |  4 | b1204426127afec | b720224b3f782a370952fc0939768e98 | 2020-08-21 13:10:48 |
    |  6 | 8070fa93540632d | 428e572b9b4d9bd5d7f8f18411687768 | 2020-08-21 13:10:48 |
    +----+-----------------+----------------------------------+---------------------+
    5 rows in set (0.00 sec)
    
    ###########################################
    # TARGET
    ###########################################
    mysql> select * from shard.uldra limit 5;
    +----+---------------------+-----------------+----------------------------------+---------------------+
    | id | guid                | name            | cont                             | dttm                |
    +----+---------------------+-----------------+----------------------------------+---------------------+
    |  1 | 1228168111325184005 | 06b1ddef755c6e9 | 63c9b12308740990e1636c87c09d76f4 | 2020-08-21 13:10:48 |
    |  2 | 1228168111325184003 | ff4d1852ff185d6 | b29cc559540451e93d205cbf2b0be578 | 2020-08-21 13:10:48 |
    |  3 | 1228168111325184004 | 0bb23b3546ccc37 | 977d181fddae1d76d0fe2ca538e1aa56 | 2020-08-21 13:10:48 |
    |  4 | 1228168111325184007 | b1204426127afec | b720224b3f782a370952fc0939768e98 | 2020-08-21 13:10:48 |
    |  6 | 1228168111325184008 | 8070fa93540632d | 428e572b9b4d9bd5d7f8f18411687768 | 2020-08-21 13:10:48 |
    +----+---------------------+-----------------+----------------------------------+---------------------+
    5 rows in set (0.00 sec)
    
    mysql> select * from shard.uldra_part limit 5;
    +----+---------------------+-----------------+---------------------+
    | id | guid                | name            | dttm                |
    +----+---------------------+-----------------+---------------------+
    |  1 | 1228168111325184005 | 06b1ddef755c6e9 | 2020-08-21 13:10:48 |
    |  2 | 1228168111325184003 | ff4d1852ff185d6 | 2020-08-21 13:10:48 |
    |  3 | 1228168111325184004 | 0bb23b3546ccc37 | 2020-08-21 13:10:48 |
    |  4 | 1228168111325184007 | b1204426127afec | 2020-08-21 13:10:48 |
    |  6 | 1228168111325184008 | 8070fa93540632d | 2020-08-21 13:10:48 |
    +----+---------------------+-----------------+---------------------+
    5 rows in set (0.01 sec)

Enjoy!