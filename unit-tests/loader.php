<?php

// Creates the autoloader
$loader = new \Phalcon\Loader();

$loader->registerDirs(
		array(
			'models/'
			,'/var/www/html/phpunit-3.7/'
			)
		);

//Register some namespaces
$loader->registerNamespaces(
		array(
			"Twm\Db\Adapter\Pdo"    => "unit-tests/db/adapter/",
			"Twm\Db\Dialect"    => "unit-tests/db/dialect/"
			)
		);

// register autoloader
$loader->register();

