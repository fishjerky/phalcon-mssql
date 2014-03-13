phalcon-mssql
=============

Description :

A Phalcon PHP Framework MSSQL pdo db adapter.

Requirements :
    - Phalcon >= 1.2.0 and <=1.2.4

Installation Instructions :
    1. copy "test/app/library/db" folder to your library folder
    2. Add the namespace to "folder path" setting

    // Register some namespaces
    array(
			"Twm\Db\Adapter\Pdo" => "library/db/adapter/",

			"Twm\Db\Dialect"     => "library/db/dialect/"

			)
		);
    3. Change the following :
	$descriptor['pdoType']}:host={$descriptor['host']};dbname={$descriptor['dbname']}

	to :

    "{$descriptor['pdoType']}:server={$descriptor['host']};database={$descriptor['dbname']}"

Also you need to make sure 'sqlsrv' is set for $config->database->pdoType. 

Upcoming Release :
    - Phalcon 2.0 support
    
Please Note :

The adapter works but a few problems still exist.

Issues
    1. scaling?
    2. transaction
	can only run single transaction
    3.about nolock hint
	I have no idea how PDO using nolock hint, so I add a trigger while ordering with guid or id, it will add nolock inside the sql statement. You may change the keyword at #273 test/app/library/db/dialect/Mssql.php:
	$nolockTokens = array('guid','dev_id');   

Unit Test
    Some test cases did not pass
    -ModelsQueryExecuteTest.php
	-group by 1
        -table Abonnes does not exist - Line 781

