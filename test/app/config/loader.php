<?php

$loader = new \Phalcon\Loader();

/**
 * We're a registering a set of directories taken from the configuration file
 */
 $loader->registerNamespaces(
		array(
			"Twm\Db\Adapter\Pdo"    => "../app/library/db/adapter/",
			"Twm\Db\Dialect"    => "../app/library/db/dialect/"
			)
		)->register();
		
$loader->registerDirs(
	array(
		$config->application->controllersDir,
		$config->application->modelsDir
	)
)->register();
