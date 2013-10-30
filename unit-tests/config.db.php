<?php

if (!file_exists('unit-tests/config.db.local.php')) {
	$configMysql = array(
			'host' => 'localhost',
			'username' => 'root',
			'password' => 'ok123456',
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
			'host'          => 'phalcon',  
			'username'      => 'mssql',
			'password'      => '', 
			'dbname'        => 'phalcon_test',
			'dialectClass'  => '\Twm\Db\Dialect\Mssql',       
			'pdoType'       => 'dblib'
			);                                                                                                                                           
}
else {
	require 'unit-tests/config.db.local.php';
}
