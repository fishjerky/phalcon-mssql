<?php

/*
   +------------------------------------------------------------------------+
   | Phalcon Framework                                                      |
   +------------------------------------------------------------------------+
   | Copyright (c) 2011-2012 Phalcon Team (http://www.phalconphp.com)       |
   +------------------------------------------------------------------------+
   | This source file is subject to the New BSD License that is bundled     |
   | with this package in the file docs/LICENSE.txt.                        |
   |                                                                        |
   | If you did not receive a copy of the license and are unable to         |
   | obtain it through the world-wide-web, please send an email             |
   | to license@phalconphp.com so we can send you a copy immediately.       |
   +------------------------------------------------------------------------+
   | Authors: Andres Gutierrez <andres@phalconphp.com>                      |
   |          Eduar Carvajal <eduar@phalconphp.com>                         |
   +------------------------------------------------------------------------+
 */

use Phalcon\Mvc\Model\Query as Query;
include "loader.php";

class PlaygroundTest extends PHPUnit_Framework_TestCase
{

    public function __construct()
    {
        spl_autoload_register(array($this, 'modelsAutoloader'));
    }

    public function __destruct()
    {
        spl_autoload_unregister(array($this, 'modelsAutoloader'));
    }

    public function modelsAutoloader($className)
    {
        $className = str_replace("\\", DIRECTORY_SEPARATOR, $className);
        $path = 'unit-tests/models/'.$className.'.php';
        if (file_exists($path)) {
            require $path;
        }
    }

    protected function _getDI()
    {

        Phalcon\DI::reset();

        $di = new Phalcon\DI();

        $di->set('modelsManager', function(){
                return new Phalcon\Mvc\Model\Manager();
                });

        $di->set('modelsMetadata', function(){
                return new Phalcon\Mvc\Model\Metadata\Memory();
                });

        $di->set('db', function(){
                require 'unit-tests/config.db.php';
                //return new Twm\Db\Adapter\Pdo\Mssql($configMssql);
                $connection = new Phalcon\Db\Adapter\Pdo\Mysql($configMysql);


                $eventsManager = new Phalcon\Events\Manager();


                //Listen all the database events
                $eventsManager->attach('db', function($event, $connection){
                    if ($event->getType() == 'beforeQuery') {
                    echo ($connection->getSQLStatement());
                    }
                    });


                //Assign the eventsManager to the db adapter instance
                $connection->setEventsManager($eventsManager);
                return $connection;
                });

        return $di;
    }

    public function testSelectParsing()
    {
        $di = $this->_getDI();
       // $di->get('db')->execute('select 1');
/*
        $robots = Robots::query()->forUpdate(true)->execute();
        return;
        $robots = Robots::query()
            ->where("type = :type:")
            ->andWhere("year < 2000")
            ->sharedLock(true)
            ->bind(array("type" => "mechanical"))
            ->order("name")
            ->execute();
            */
        
           $personnes = Personnes::find(array(
           "conditions" => "cedula >=:d1:",
           "bind" => array("d1" => '1'),
           "shared_lock" => true,
           "for_update" => true,
        /*        "order" => "cedula, nombres",
        'limit'  =>  array(
        'offset'  =>  1,
        'number'  => 2
         )*/)
        );
         
        /*
           $builder = $di['modelsManager']->createBuilder()
           ->columns('cedula, nombres')
           ->from('Personnes')
           ->orderBy('cedula');
        //->limit(500, 0);

        $paginator = new Phalcon\Paginator\Adapter\QueryBuilder(array(
        "builder" => $builder,
        "limit"=> 10,
        "page" => 2
        ));

        $page = $paginator->getPaginate();

         */
    }

}
