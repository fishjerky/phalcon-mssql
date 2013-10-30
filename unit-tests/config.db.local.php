<?php
	$configMysql = array(
			'host' => 'localhost',
			'username' => 'mc2',
			'password' => 'ok1234',
			'dbname' => 'phalcon_test'
			);

	$configPostgresql = array(
			'host' => '127.0.0.1',
			'username' => 'postgres',
			'password' => '',
			'dbname' => 'phalcon_test',
			'schema' => 'public'
			);

	$configSqlite = array(
			'dbname' => '/tmp/phalcon_test.sqlite',
			);


	$configMssql = array(               
			'host'          => 'McDev',  
			'username'      => 'apnewmc',
			'password'      => 'Cm!2212@12', 
			'dbname'        => 'CCCMCDEV2',
			'dialectClass'  => '\Twm\Db\Dialect\Mssql',       
			'pdoType'       => 'dblib'
			);   
