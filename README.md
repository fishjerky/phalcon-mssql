phalcon-mssql
=============

## Description :

A Phalcon PHP Framework MSSQL pdo db adapter.

## Requirements :
    - Phalcon >= 2.0

## Installation Instructions :
1. copy "test/app/library/db" folder to your library folder
2. Add the namespace to "folder path" setting
--// Register some namespaces
--array(
--	"Twm\Db\Adapter\Pdo" => "library/db/adapter/",
--	"Twm\Db\Dialect"     => "library/db/dialect/"
--);
3. Change the following :
--$descriptor['pdoType']}:host={$descriptor['host']};dbname={$descriptor['dbname']}
to :
--"{$descriptor['pdoType']}:server={$descriptor['host']};database={$descriptor['dbname']}"

Also you need to make sure 'sqlsrv' is set for $config->database->pdoType. 

## Upcoming Release :
- Rewrite in Zephir ...

---

Please Note :
The adapter works but a few problems still exist.

Issues:
1. scaling?
2. transaction
single transaction works, but ...
3.about nolock hint
Every "select" query will add nolock hint, You may change change at [dialect/Mssql.php](https://github.com/fishjerky/phalcon-mssql/blob/2.0.x/test/app/library/db/dialect/Mssql.php#L198)	
--$nolockTokens = array('id');   //token to trigger nolock hint`

---
Unit Test:
Some test cases did not pass
-ModelsQueryExecuteTest.php
  -group by 1
  -table Abonnes does not exist - Line 781

