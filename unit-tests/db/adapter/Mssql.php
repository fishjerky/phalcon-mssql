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
			switch ($field['TYPE_NAME']) {
				case 'int identity':
					$definition['type'] = Column::TYPE_INTEGER;
					$definition["isNumeric"] = true;
					$definition['bindType'] = Column::BIND_PARAM_INT;
					$autoIncrement = true;
					break;
				case 'int':
					$definition['type'] = Column::TYPE_INTEGER;
					$definition["isNumeric"] = true;
					$definition['bindType'] = Column::BIND_PARAM_INT;
					break;
				case 'nchar':
					$definition['type'] = Column::TYPE_VARCHAR;
					break;
				case 'char':
					$definition['type'] = Column::TYPE_CHAR;
					break;
				case 'smallint':
					$definition['type'] = Column::TYPE_INTEGER;
					$definition["isNumeric"] = true;
					$definition['bindType'] = Column::BIND_PARAM_INT;
					break;
				case 'float':
					$definition['type'] = Column::TYPE_DECIMAL;
					$definition["isNumeric"] = true;
					$definition['bindType'] = Column::BIND_SKIP;
					break;
				case 'datetime':
					$definition["type"] = Column::TYPE_DATETIME;
					break;
				case 'date':
					$definition["type"] = Column::TYPE_DATE;
					break;
				case 'decimal':
					$definition["type"] = Column::TYPE_DECIMAL;
					$definition["isNumeric"] = true;
					$definition["bindType"] = Column::BIND_PARAM_DECIMAL;
					break;
				case 'text':
					$definition["type"] = Column::TYPE_TEXT;
					break;
				case 'numeric':
					$definition["type"] = Column::TYPE_FLOAT;
					$definition["isNumeric"] = true;
					$definition["bindType"] = Column::TYPE_DECIMAL;
					break;
				default:
					//echo $field['COLUMN_NAME'] . 'has no match type: ' .  $field['TYPE_NAME'] . PHP_EOL;
					$definition['type'] = Column::TYPE_VARCHAR;
					//$definition['bindType'] = Column::BIND_PARAM_STR;
			}

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

		//	$this->_connection->exec('SET QUOTED_IDENTIFIER ON');

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

	public function insert($table, $values, $fields=null, $dataTypes=null)
	{
		$placeholders; $insertValues; $bindDataTypes; $bindType;
		$position; $value; $escapedTable; $joinedValues; $escapedFields;
		$field; $insertSql;

		if (!is_array($values)) {
			throw new Phalcon\Db\Exception("The second parameter for insert isn't an Array");
		}

		/**
		 * A valid array with more than one element is required
		 */
		if (!count($values)) {
			throw new Phalcon\Db\Exception("Unable to insert into " . $table . " without data");
		}

		$placeholders = array();
		$insertValues = array();

		if (!is_array($dataTypes)) {
			$bindDataTypes = array();
		} else {
			$bindDataTypes = $dataTypes;
		}

		/**
		 * Objects are casted using __toString, null values are converted to string "null", everything else is passed as "?"
		 */
		//echo PHP_EOL;	var_dump($dataTypes);
		foreach ( $values as $position => $value){
			if (is_object($value)) {
				//let placeholders[] = (string) value;
			} else {
				if ( $value == null ) {
					$placeholders[] = "null";
				} else {
					$placeholders[] = "?";
					$insertValues[] = $value;
					if (is_array($dataTypes)) {
						if( !isset($dataTypes[$position])){
							throw new Phalcon\Db\Exception("Incomplete number of bind types");
						}
						$bindType = $dataTypes[$position];
						$bindDataTypes[] = $bindType;
					}
				}
			}
		}

		if (false) { //globals_get("db.escape_identifiers") {
			$escapedTable = $this->escapeIdentifier($table);
		} else {
			$escapedTable = $table;
		}

		/**
		 * Build the final SQL INSERT statement
		 */
		$joinedValues = join(", ", $placeholders);
		if ( is_array($fields) ) {

			if (false ) {//globals_get("db.escape_identifiers") {
				$escapedFields = array();
				foreach ($fields as $field) {
					$escapedFields[] = $this->escapeIdentifier($field);
				}
			} else {
				$escapedFields = $fields;
			}
			$insertSql = "INSERT INTO " . $escapedTable . " (" . join(", ", $escapedFields) . ") VALUES (" . $joinedValues . ")";
			} else {
				$insertSql = "INSERT INTO " . $escapedTable . " VALUES (" . $joinedValues . ")";
			}

			/**
			 * Perform the execution via PDO::execute
			 */
			return $this->execute($insertSql, $insertValues, $bindDataTypes);
		}


		public function lastInsertId($tableName = null, $primaryKey = null)
		{
			$sql = 'SELECT SCOPE_IDENTITY()';		
			return (int)$this->fetchOne($sql);
		}


		public function delete($table, $whereCondition=null, $placeholders=null, $dataTypes=null)
		{
			$sql; $escapedTable;

			if (false) { // globals_get("db.escape_identifiers") {
				$escapedTable = $this->escapeIdentifier($table);
			} else {
				$escapedTable = $table;
			}


			if (!empty($whereCondition)) {
				$sql = "DELETE FROM " . $escapedTable . " WHERE " . $whereCondition;
			} else {
				$sql = "DELETE FROM " . $escapedTable;
			}

			/**
			 * Perform the update via PDO::execute
			 */
			return $this->execute($sql, $placeholders, $dataTypes);
			}

			/**
			 * Lists table indexes
			 *
			 *<code>
			 *	print_r($connection->describeIndexes('robots_parts'));
			 *</code>
			 *
			 * @param	string table
			 * @param	string schema
			 * @return	Phalcon\Db\Index[]
			 */
			public function describeIndexes($table, $schema=null)
			{

				$dialect = $this->_dialect;

				$indexes = array();
				$temps = $this->fetchAll($dialect->describeIndexes($table, $schema), Phalcon\Db::FETCH_ASSOC);
				foreach ($temps as $index) {
					$keyName = $index['index_id'];
					if (!isset($indexes[$keyName])) {
						$indexes[$keyName] = array();
					}

					//let indexes[keyName][] = index[4];
				}

				$indexObjects = array();
				foreach ($indexes as  $name => $indexColumns) {

					/**
					 * Every index is abstracted using a Phalcon\Db\Index instance
					 */
					$indexObjects[$name] = new Phalcon\Db\Index($name, $indexColumns);
				}

				return $indexObjects;
			}

			/**
			 * Lists table references
			 *
			 *<code>
			 * print_r($connection->describeReferences('robots_parts'));
			 *</code>
			 *
			 * @param	string table
			 * @param	string schema
			 * @return	Phalcon\Db\Reference[]
			 */
			public function describeReferences($table, $schema=null)
			{

				$dialect = $this->_dialect;

				$emptyArr = array();
				$references = array();

				$temps = $this->fetchAll($dialect->describeReferences($table, $schema), Phalcon\Db::FETCH_NUM);
				foreach ($temps as $reference ){

					$constraintName = $reference[2];
					if (!isset($references[$constraintName])) {
						$references[$constraintName] = array(
							"referencedSchema"  => $reference[3],
							"referencedTable"   => $reference[4],
							"columns"           => $emptyArr,
							"referencedColumns" => $emptyArr
								);
					}

					//let references[constraintName]["columns"][] = reference[1],
					//	references[constraintName]["referencedColumns"][] = reference[5];
				}

				$referenceObjects = array();
				foreach ($references as $name => $arrayReference) {
					$referenceObjects[$name] = new Phalcon\Db\Reference($name, array(
							"referencedSchema"	=> $arrayReference["referencedSchema"],
							"referencedTable"	=> $arrayReference["referencedTable"],
							"columns"			=> $arrayReference["columns"],
							"referencedColumns" => $arrayReference["referencedColumns"]
							));
				}

				return $referenceObjects;
			}

			/**
			 * Gets creation options from a table
			 *
			 *<code>
			 * print_r($connection->tableOptions('robots'));
			 *</code>
			 *
			 * @param	string tableName
			 * @param	string schemaName
			 * @return	array
			 */
			public function tableOptions($tableName, $schemaName=null)
			{
				$dialect = $this->_dialect;
				$sql = $dialect->tableOptions($tableName, $schemaName);
				if ($sql) {
					$describe = $this->fetchAll($sql, Phalcon\DB::FETCH_NUM);
					return $describe[0];
				}
				return array();
			}

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
