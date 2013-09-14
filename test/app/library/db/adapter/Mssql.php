<?php
namespace Twm\Db\Adapter\Pdo;

use Phalcon,
	Phalcon\Db\Column,
	Phalcon\Db\Adapter\Pdo as AdapterPdo,
	Phalcon\Events\EventsAwareInterface,
	Phalcon\Db\AdapterInterface;

class Mssql extends AdapterPdo implements EventsAwareInterface, AdapterInterface
{

	protected $_type = 'mssql';
	//	protected $_dialectType = 'sqlsrv';

	public function  __construct($descriptor){
		$this->connect($descriptor);
	}

	//public function escapeIdentifier(){}

	public function describeColumns($table, $schema = null){
		$describe; $columns; $columnType; $field; $definition;
		$oldColumn; $dialect; $sizePattern; $matches; $matchOne; $columnName;

		/**
		 * Get the SQL to describe a table
		 * We're using FETCH_NUM to fetch the columns
		 * Get the describe
		 */

		//1. get pk 
		$primaryKeys = array();
		$describeKeys = $this->fetchAll("exec sp_pkeys @table_name = '$table'");
		foreach ($describeKeys as $field) {
			$primaryKeys[$field['COLUMN_NAME']] = true;
		}

		//2.get column description
		$dialect = $this->_dialect;
		$describe = $this->fetchAll($dialect->describeColumns($table, $schema), Phalcon\Db::FETCH_ASSOC);

		$oldColumn = null;
		$sizePattern = "#\\(([0-9]+)(,[0-9]+)*\\)#";

		$columns = array();

		/**
		 * Field Indexes: 0:name, 1:type, 2:not null, 3:key, 4:default, 5:extra
		 */
		foreach ($describe as $field) {

			/**
			 * By default the bind types is two
			 */
			$definition = array(
					"bindType"	=> 2,
					"unsigned"	=> false,
					);

			/**
			 * By checking every column type we convert it to a Phalcon\Db\Column
			 */
			$columnType = $field['TYPE_NAME'];

			$autoIncrement = false;
			do {

				/**
				 * int identity are int and auto increment
				 */
				if (stristr($columnType, "int identity")) {
					$definition["type"] = 0;
					$definition["isNumeric"] = true;
					$definition["bindType"] = Phalcon\Db\Column::BIND_PARAM_INT;
					$autoIncrement = true;
					break;
				}


				/**
				 * Tinyint/Smallint/Bigint/Integers/Int are int
				 */
				if (stristr($columnType, "int")) {
					$definition["type"] = 0;
					$definition["isNumeric"] = true;
					$definition["bindType"] = Phalcon\Db\Column::BIND_PARAM_INT;
					break;
				}

				/**
				 * varchar are varchars
				 */
				if (stristr($columnType, "varchar")) {
					$definition["type"] = Phalcon\Db\Column::TYPE_VARCHAR;
					$definition["isNumeric"] = false;
					$definition["bindType"] = Phalcon\Db\Column::BIND_PARAM_STR;
					break;
				}

				/**
				 * nchar are varchars
				 */
				if (stristr($columnType, "nchar")) {
					$definition["type"] = Phalcon\Db\Column::TYPE_CHAR;
					$definition["isNumeric"] = false;
					$definition["bindType"] = Phalcon\Db\Column::BIND_PARAM_STR;
					break;
				}


				/**
				 * Special type for datetime
				 */
				if (stristr($columnType, "datetime")) {
					$definition["type"] = Phalcon\Db\Column::TYPE_DATETIME;
					break;
				}

				/**
				 * Decimals are floats
				 */
				if (stristr($columnType, "decimal")) {
					$definition["type"] = Phalcon\Db\Column::TYPE_DECIMAL;
					$definition["isNumeric"] = true;
					$definition["bindType"] = Phalcon\Db\Column::BIND_PARAM_DECIMAL;
					break;
				}

				/**
				 * Chars are chars
				 */
				if (stristr($columnType, "char")){
					$definition["type"] = Phalcon\Db\Column::TYPE_CHAR;
					break;
				}

				/**
				 * Date/Datetime are varchars
				 */
				if (stristr($columnType, "date")) {
					$definition["type"] = Phalcon\Db\Column::TYPE_DATE;
					break;
				}

				/**
				 * Text are varchars
				 */
				if (stristr($columnType, "text")) {
					$definition["type"] = Phalcon\Db\Column::TYPE_TEXT;
					break;
				}

				/**
				 * Float/Smallfloats/Decimals are float
				 */
				if (stristr($columnType, "float")) {
					$definition["type"] = Phalcon\Db\Column::TYPE_FLOAT;
					$definition["isNumeric"] = true;
					$definition["bindType"] = Phalcon\Db\Column::TYPE_DECIMAL;
					break;
				}

				/**
				 * numeric are treated as float
				 */
				if (stristr($columnType, "numeric")) {
					$definition["type"] = Phalcon\Db\Column::TYPE_FLOAT;
					$definition["isNumeric"] = true;
					$definition["bindType"] = Phalcon\Db\Column::TYPE_DECIMAL;
					break;
				}

				/**
				 * By default is string
				 */
				$definition["type"] = Phalcon\Db\Column::TYPE_VARCHAR;
				break;
			}while(1);
//			echo "column: {$field['COLUMN_NAME']} type: {$columnType} type: {$definition['type']}}" .PHP_EOL;

			/**
			 * If the column type has a parentheses we try to get the column size from it
			 */
			$definition["size"] = (int)$field['LENGTH'];
			$definition["precision"] = (int)$field['PRECISION'];

			/**
			 * Positions
			 */
			if (!$oldColumn) {
				$definition["first"] = true;
			} else {
				$definition["after"] = $oldColumn;
			}

			/**
			 * Check if the field is primary key
			 */
			if (isset($primaryKeys[$field['COLUMN_NAME']])) {
				$definition["primary"] = true;
			}

			/**
			 * Check if the column allows null values
			 */
			$definition["notNull"] = ($field['NULLABLE'] == 0);

			if($field['SCALE']){
				$definition["scale"] = (int)$field['SCALE'];
				$definition["size"] = $definition['precision'];
			}
			if($field['SCALE'] == '0'){
				//$definition["scale"] = (int)$field['SCALE']; //i have no idea why this statement fail
				$definition["size"] = $definition['precision'];
			}
			/**
			 * Check if the column is auto increment
			 */
			if ($autoIncrement) {
				$definition["autoIncrement"] = true;
			}

			/**
			 * Every route is stored as a Phalcon\Db\Column
			 */
			$columnName = $field['COLUMN_NAME'];
			$columns[] = new Phalcon\Db\Column($columnName, $definition);
			$oldColumn = $columnName;
		}

		return $columns;
	}

	public function describeColumnsi1($table, $schema = null){
		$primaryKeys = array();

		$describeKeys = $this->fetchAll("exec sp_pkeys @table_name = '$table'");
		foreach ($describeKeys as $field) {
			$primaryKeys[$field['COLUMN_NAME']] = true;
		}

		$describeTable = $this->fetchAll("exec sp_columns @table_name = '$table'");

		$finalDescribe = array();

		foreach ($describeTable as $field) {

			$type = null;
			$bindType = Column::BIND_PARAM_STR;
			$autoIncrement = false;

			switch ($field['TYPE_NAME']) {
				case 'int identity':
					$type = Column::TYPE_INTEGER;
					$bindType = Column::BIND_PARAM_INT;
					$autoIncrement = true;
					break;
				case 'int':
					$type = Column::TYPE_INTEGER;
					$bindType = Column::BIND_PARAM_INT;
					break;
				case 'nchar':
					$type = Column::TYPE_VARCHAR;
					break;
				case 'char':
					$type = Column::TYPE_CHAR;
					break;
				case 'smallint':
					$type = Column::TYPE_INTEGER;
					$bindType = Column::BIND_PARAM_INT;
					break;
				case 'float':
					$type = Column::TYPE_DECIMAL;
					$bindType = Column::BIND_SKIP;
					break;
				default:
					echo '[Mssql Dialect] can not find this type:' . $field['TYPE_NAME'];
					$type = Column::TYPE_VARCHAR;
			}

			$columnDefinition = array(
					"type" => $type,
					"size" => $field['LENGTH'],
					"unsigned" => false,
					"notNull" => $field['NULLABLE'] == 1
					);

			if ($autoIncrement) {
				$columnDefinition['autoIncrement'] = $autoIncrement;
			}

			if (isset($primaryKeys[$field['COLUMN_NAME']])) {
				$columnDefinition['primary'] = true;
			}

			$finalDescribe[$field['COLUMN_NAME']] = new Column($field['COLUMN_NAME'], $columnDefinition);
		}

		return $finalDescribe;
	}

	public function connect($descriptor = null ){
		$this->_pdo = new \PDO(
				"{$descriptor['pdoType']}:host={$descriptor['host']};dbname={$descriptor['dbname']}",
				$descriptor['username'], 
				$descriptor['password'],
				array(
					\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
					\PDO::ATTR_STRINGIFY_FETCHES => true
					)
				);

		//$this->_connection->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );
		//$query = $this->_connection->prepare('SET QUOTED_IDENTIFIER ON'); 
		//$query->execute();

		$this->_dialect = new \Twm\Db\Dialect\Mssql();
	}

public function query($sql, $bindParams=null, $bindTypes=null)
	{
				if (strpos($sql, 'SELECT COUNT(*) "numrows"') !== false) {
								$sql .= ' dt ';
										}
						return parent::query($sql, $bindParams, $bindTypes);
	}

	//insert miss parameters, need to do this
	public function executePrepared(\PDOStatement $statement, $placeholders = array(), $dataTypes = array()){
		return $this->_pdo->prepare($statement->queryString, $placeholders);
	}

	/*
	   public function query($sqlStatement, $bindParams = array(), $bindTypes = array()){
	   $query = $this->_connection->prepare($sqlStatement); 
	   $result = new \Phalcon\Db\Result\Pdo($this->_connection, $query, $sqlStatement, $bindParams, $bindTypes);
	   $result->execute();
	   return $result;

	   }
	 */

	/**
	 * Creates a PDO DSN for the adapter from $this->_config settings.
	 *
	 * @return string
	 */
	protected function _dsn()
	{
		// baseline of DSN parts
		$dsn = $this->_config;

		// don't pass the username and password in the DSN
		unset($dsn['username']);
		unset($dsn['password']);
		unset($dsn['options']);
		unset($dsn['persistent']);
		unset($dsn['driver_options']);

		if (isset($dsn['port'])) {
			$seperator = ':';
			if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
				$seperator = ',';
			}
			$dsn['host'] .= $seperator . $dsn['port'];
			unset($dsn['port']);
		}

		// this driver supports multiple DSN prefixes
		// @see http://www.php.net/manual/en/ref.pdo-dblib.connection.php
		if (isset($dsn['pdoType'])) {
			switch (strtolower($dsn['pdoType'])) {
				case 'freetds':
				case 'sybase':
					$this->_pdoType = 'sybase';
					break;
				case 'mssql':
					$this->_pdoType = 'mssql';
					break;
				case 'dblib':
				default:
					$this->_pdoType = 'dblib';
					break;
			}
			unset($dsn['pdoType']);
		}

		// use all remaining parts in the DSN
		foreach ($dsn as $key => $val) {
			$dsn[$key] = "$key=$val";
		}

		$dsn = $this->_pdoType . ':' . implode(';', $dsn);
		return $dsn;
	}

	/**
	 * @return void
	 */
	protected function _connect()
	{
		if ($this->_connection) {
			return;
		}
		parent::_connect();
		$this->_connection->exec('SET QUOTED_IDENTIFIER ON');
	}

	/**
	 * Begin a transaction.
	 *
	 * It is necessary to override the abstract PDO transaction functions here, as
	 * the PDO driver for MSSQL does not support transactions.
	 */
	protected function _beginTransaction()
	{
		$this->_connect();
		$this->_connection->exec('BEGIN TRANSACTION');
		return true;
	}

	/**
	 * Commit a transaction.
	 *
	 * It is necessary to override the abstract PDO transaction functions here, as
	 * the PDO driver for MSSQL does not support transactions.
	 */
	protected function _commit()
	{
		$this->_connect();
		$this->_connection->exec('COMMIT TRANSACTION');
		return true;
	}

	/**
	 * Roll-back a transaction.
	 *
	 * It is necessary to override the abstract PDO transaction functions here, as
	 * the PDO driver for MSSQL does not support transactions.
	 */
	protected function _rollBack() {
		$this->_connect();
		$this->_connection->exec('ROLLBACK TRANSACTION');
		return true;
	}

}
