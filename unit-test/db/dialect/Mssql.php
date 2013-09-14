<?php
namespace Twm\Db\Dialect;

class Mssql extends \Phalcon\Db\Dialect implements \Phalcon\Db\DialectInterface
{

	public function limit($sqlQuery, $number){}
	public function forUpdate($sqlQuery){}
	public function shareLock($sqlQuery){}
	public function select($definition){}
	public function getColumnList($columnList){}
	public function getColumnDefinition($column){}
	
	public function addColumn($tableName, $schemaName, $column){}
	public function modifyColumn($tableName, $schemaName, $column){}

	public function dropColumn($tableName, $schemaName, $column){}
	public function addIndex($tableName, $schemaName, $index){}
	public function dropIndex($tableName, $schemaName, $indexName){}

	public function addPrimaryKey($tableName, $schemaName, $index){}
	public function dropPrimaryKey($tableName, $schemaName){}

	public function addForeignKey($tableName, $schemaName, $reference){}
	public function dropForeignKey($tableName, $schemaName, $referenceName){}

	public function createTable($tableName, $schemaName, $definition){}
	public function dropTable($tableName, $schemaName){}


	public function tableExists($tableName, $schemaName = null){
return "select * from mc2.sys.tables where name = '$tableName'";

	}

	public function describeColumns($table, $schema = null){
		die('dialect - describeColumns');
	}

	public function listTables($schemaName = null){
		die('dialect - listTables');
	}

	public function describeIndexes($table, $schema = null){}

	public function describeReferences($table, $schema = null){}

	public function tableOptions($table, $schema = null){}

	public function supportsSavepoints(){}
	public function supportsReleseSavepoints(){}

	public function createSavepoint($name){}
	public function releaseSavepoint($name){}
	public function rollbackSavepoint($name){}
}
