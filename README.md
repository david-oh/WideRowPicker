# WideRowPicker
- Region Server를 region 단위로 scan하면서 widerow 및 tombstone 등의 정보를 수집하는 Tool

## Usage
1. Download jars from [releases](../../releases) page
 - JDK7 required
2. Run downloaded jar with JRE

```
Usage: java -jar WideRowPicker-xxx-jar-with-dependencies.jar 
                -zk=zk1.xxx.com:2181,zk2.xxx.com:2181,zk3.xxx.com:2181 
                -rs=rs1.xxx.com 
                --jaas=jaas file path
                --krb5=krb5.conf path
                --realm=kerberos realm
                --table=table-name 
                --limit=10000

zookeeper quorum (required):
	-zk=zk1.xxx.com:2181,zk2.xxx.com:2181,zk3.xxx.com:2181

region server (required):
	-rs=rs1.xxx.com

kerberos option (optional):
	--jaas=jaas file path
	--krb5=krb5.conf path
	--realm=realm

table name (optional):
	--table=table-name

scan limit (optional):
	--limit=10000
```

## Example
```            
====================================================================================================================
* Region Server : rs1.xxx.com,60020,1458636229828
* table name : test-tb
* scan limit : 0
====================================================================================================================

------------------------------------------------------------------------------------------------------------------
# encoded region name : test-tb,,1468204622090.8873107bcbc9e7a51123ce113e5ea0c8.
  start key, end key  : , 12892
------------------------------------------------------------------------------------------------------------------
* max row size (bytes) : 29
  ㄴrowkey : 1
* max rowkey size : 6
* max value size : 24
* max column count : 6
  ㄴrowkey : 1
* row count (+tomb) : 38571

* [tomb] max row's rowkey : null
* [tomb] row count : 0
------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------
# encoded region name : test-tb,12892,1468204622090.39a7987a53b5c8a710cd894edcac3f71.
  start key, end key  : 12892, 16268
------------------------------------------------------------------------------------------------------------------
* max row size (bytes) : 23
  ㄴrowkey : 128920
* max rowkey size : 6
* max value size : 13
* max column count : 3
  ㄴrowkey : 12892
* row count (+tomb) : 45013

* [tomb] max row's rowkey : null
* [tomb] row count : 0
------------------------------------------------------------------------------------------------------------------

...
...

====================================================================================================================
		TOTAL RESULT
====================================================================================================================
* max row size (bytes) : 29
  ㄴrowkey : 1
  ㄴregion name : test-tb,,1468204622090.8873107bcbc9e7a51123ce113e5ea0c8.

* max column count : 6
  ㄴregion name : test-tb,,1468204622090.8873107bcbc9e7a51123ce113e5ea0c8.
  ㄴrowkey : 1

* row count (+tomb) : 400012

* [tomb] max row size (bytes) : 9
  ㄴregion name : test-tb,20607,1468204625745.83da08160d47b30eb19debd72f089278.
  ㄴrowkey : 20607
  ㄴrow count : 2
====================================================================================================================

```
