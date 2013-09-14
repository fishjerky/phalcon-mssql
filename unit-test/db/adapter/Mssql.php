<?php
namespace Twm\Db\Adapter\Pdo;

use Phalcon\Db\Column,
	Phalcon\Db\Adapter\Pdo as AdapterPdo,
	Phalcon\Events\EventsAwareInterface,
	Phalcon\Db\AdapterInterface;

class Mssql extends AdapterPdo implements EventsAwareInterface, AdapterInterface
{

	protected $_type = 'mssql';
	protected $_dialectType = 'sqlsrv';

	public function  __construct($descriptor){
		$this->connect($descriptor);
	}

	//public function escapeIdentifier(){}

	public function describeColumns($table, $schema = null){
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
					echo $field['TYPE_NAME'];
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

	/**
	 * Returns a list of the tables in the database.
	 *
	 * @return array
	 */
	public function listTables($schemaName = '')
	{
		$sql = "SELECT name FROM sysobjects WHERE type = 'U' ORDER BY name";
		return $this->fetchCol($sql);
	}

	/**
	 * Returns the column descriptions for a table.
	 *
	 * The return value is an associative array keyed by the column name,
	 * as returned by the RDBMS.
	 *
	 * The value of each array element is an associative array
	 * with the following keys:
	 *
	 * SCHEMA_NAME      => string; name of database or schema
	 * TABLE_NAME       => string;
	 * COLUMN_NAME      => string; column name
	 * COLUMN_POSITION  => number; ordinal position of column in table
	 * DATA_TYPE        => string; SQL datatype name of column
	 * DEFAULT          => string; default expression of column, null if none
	 * NULLABLE         => boolean; true if column can have nulls
	 * LENGTH           => number; length of CHAR/VARCHAR
	 * SCALE            => number; scale of NUMERIC/DECIMAL
	 * PRECISION        => number; precision of NUMERIC/DECIMAL
	 * UNSIGNED         => boolean; unsigned property of an integer type
	 * PRIMARY          => boolean; true if column is part of the primary key
	 * PRIMARY_POSITION => integer; position of column in primary key
	 * PRIMARY_AUTO     => integer; position of auto-generated column in primary key
	 *
	 * @todo Discover column primary key position.
	 * @todo Discover integer unsigned property.
	 *
	 * @param string $tableName
	 * @param string $schemaName OPTIONAL
	 * @return array
	 */
	public function describeTable($tableName, $schemaName = null)
	{
		if ($schemaName != null) {
			if (strpos($schemaName, '.') !== false) {
				$result = explode('.', $schemaName);
				$schemaName = $result[1];
			}
		}
		/**
		 * Discover metadata information about this table.
		 */
		$sql = "exec sp_columns @table_name = " . $this->quoteIdentifier($tableName, true);
		if ($schemaName != null) {
			$sql .= ", @table_owner = " . $this->quoteIdentifier($schemaName, true);
		}

		$stmt = $this->query($sql);
		$result = $stmt->fetchAll(Zend_Db::FETCH_NUM);

		$table_name  = 2;
		$column_name = 3;
		$type_name   = 5;
		$precision   = 6;
		$length      = 7;
		$scale       = 8;
		$nullable    = 10;
		$column_def  = 12;
		$column_position = 16;

		/**
		 * Discover primary key column(s) for this table.
		 */
		$sql = "exec sp_pkeys @table_name = " . $this->quoteIdentifier($tableName, true);
		if ($schemaName != null) {
			$sql .= ", @table_owner = " . $this->quoteIdentifier($schemaName, true);
		}

		$stmt = $this->query($sql);
		$primaryKeysResult = $stmt->fetchAll(Zend_Db::FETCH_NUM);
		$primaryKeyColumn = array();
		$pkey_column_name = 3;
		$pkey_key_seq = 4;
		foreach ($primaryKeysResult as $pkeysRow) {
			$primaryKeyColumn[$pkeysRow[$pkey_column_name]] = $pkeysRow[$pkey_key_seq];
		}

		$desc = array();
		$p = 1;
		foreach ($result as $key => $row) {
			$identity = false;
			$words = explode(' ', $row[$type_name], 2);
			if (isset($words[0])) {
				$type = $words[0];
				if (isset($words[1])) {
					$identity = (bool) preg_match('/identity/', $words[1]);
				}
			}

			$isPrimary = array_key_exists($row[$column_name], $primaryKeyColumn);
			if ($isPrimary) {
				$primaryPosition = $primaryKeyColumn[$row[$column_name]];
			} else {
				$primaryPosition = null;
			}

			$desc[$this->foldCase($row[$column_name])] = array(
					'SCHEMA_NAME'      => null, // @todo
					'TABLE_NAME'       => $this->foldCase($row[$table_name]),
					'COLUMN_NAME'      => $this->foldCase($row[$column_name]),
					'COLUMN_POSITION'  => (int) $row[$column_position],
					'DATA_TYPE'        => $type,
					'DEFAULT'          => $row[$column_def],
					'NULLABLE'         => (bool) $row[$nullable],
					'LENGTH'           => $row[$length],
					'SCALE'            => $row[$scale],
					'PRECISION'        => $row[$precision],
					'UNSIGNED'         => null, // @todo
					'PRIMARY'          => $isPrimary,
					'PRIMARY_POSITION' => $primaryPosition,
					'IDENTITY'         => $identity
					);
		}
		return $desc;
	}

	/**
	 * Adds an adapter-specific LIMIT clause to the SELECT statement.
	 *
	 * @link http://lists.bestpractical.com/pipermail/rt-devel/2005-June/007339.html
	 *
	 * @param string $sql
	 * @param integer $count
	 * @param integer $offset OPTIONAL
	 * @throws Zend_Db_Adapter_Exception
	 * @return string
	 */
	public function limit($sql, $count, $offset = 0)
	{
		$count = intval($count);
		if ($count <= 0) {
			require_once 'Zend/Db/Adapter/Exception.php';
			throw new Zend_Db_Adapter_Exception("LIMIT argument count=$count is not valid");
		}

		$offset = intval($offset);
		if ($offset < 0) {
			/** @see Zend_Db_Adapter_Exception */
			require_once 'Zend/Db/Adapter/Exception.php';
			throw new Zend_Db_Adapter_Exception("LIMIT argument offset=$offset is not valid");
		}

		if ($offset == 0) {
			$sql = preg_replace('/^SELECT\s/i', 'SELECT TOP ' . $count . ' ', $sql);
		} else {
			$orderby = stristr($sql, 'ORDER BY');

			if (!$orderby) {
				$over = 'ORDER BY (SELECT 0)';
			} else {
				$over = preg_replace('/\"[^,]*\".\"([^,]*)\"/i', '"inner_tbl"."$1"', $orderby);
			}

			// Remove ORDER BY clause from $sql
			$sql = preg_replace('/\s+ORDER BY(.*)/', '', $sql);

			// Add ORDER BY clause as an argument for ROW_NUMBER()
			$sql = "SELECT ROW_NUMBER() OVER ($over) AS \"ZEND_DB_ROWNUM\", * FROM ($sql) AS inner_tbl";

			$start = $offset + 1;
			$end = $offset + $count;

			$sql = "WITH outer_tbl AS ($sql) SELECT * FROM outer_tbl WHERE \"ZEND_DB_ROWNUM\" BETWEEN $start AND $end";
		}

		return $sql;
	}

	/**
	 * Gets the last ID generated automatically by an IDENTITY/AUTOINCREMENT column.
	 *
	 * As a convention, on RDBMS brands that support sequences
	 * (e.g. Oracle, PostgreSQL, DB2), this method forms the name of a sequence
	 * from the arguments and returns the last id generated by that sequence.
	 * On RDBMS brands that support IDENTITY/AUTOINCREMENT columns, this method
	 * returns the last value generated for such a column, and the table name
	 * argument is disregarded.
	 *
	 * Microsoft SQL Server does not support sequences, so the arguments to
	 * this method are ignored.
	 *
	 * @param string $tableName   OPTIONAL Name of table.
	 * @param string $primaryKey  OPTIONAL Name of primary key column.
	 * @return string
	 * @throws Zend_Db_Adapter_Exception
	 */
	public function lastInsertId($tableName = null, $primaryKey = null)
	{
		$sql = 'SELECT SCOPE_IDENTITY()';
		return (int)$this->fetchOne($sql);
	}

	/**
	 * Retrieve server version in PHP style
	 * Pdo_Mssql doesn't support getAttribute(PDO::ATTR_SERVER_VERSION)
	 * @return string
	 */
	public function getServerVersion()
	{
		try {
			$stmt = $this->query("SELECT SERVERPROPERTY('productversion')");
			$result = $stmt->fetchAll(Zend_Db::FETCH_NUM);
			if (count($result)) {
				return $result[0][0];
			}
			return null;
		} catch (PDOException $e) {
			return null;
		}
	}
}
