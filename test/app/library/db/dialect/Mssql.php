<?php
namespace Twm\Db\Dialect;

class Mssql extends \Phalcon\Db\Dialect //implements \Phalcon\Db\DialectInterface
{

	public function limit($sqlQuery, $number){
		$sql = preg_replace('/^SELECT\s/i', 'SELECT TOP ' . $number . ' ', $sqlQuery);

		return $sql;
	}
	public function forUpdate($sqlQuery){}
	public function shareLock($sqlQuery){}
	Public function select($definition){

		$tables; $columns; $escapeChar; $columnItem; $column;
		$selectedColumns; $columnSql; $columnDomainSql; $columnAlias;
		$selectedTables; $sqlJoin; $joinExpressions; $joinCondition;
		$joinConditionsArray; $tablesSql; $columnDomain; $columnAliasSql;
		$columnsSql; $table; $sql; $joins; $join; $sqlTable; $whereConditions;
		$groupFields; $groupField; $groupItems; $havingConditions;
		$orderFields; $orderItem; $orderItems; $orderSqlItem; $sqlOrderType;
		$orderSqlItemType; $limitValue; $number; $offset;

		if (!is_array($definition)){
			throw new Phalcon\Db\Exception("Invalid SELECT definition");
		}

		if (isset($definition['tables'])) 
			$tables = $definition["tables"];
		else
			throw new Phalcon\Db\Exception("The index 'tables' is required in the definition array");
		

		if (isset($definition['columns'])) 
			$columns = $definition["columns"];
		else
			throw new Phalcon\Db\Exception("The index 'columns' is required in the definition array");

		/*		if globals_get("db.escape_identifiers") {
				let escapeChar = this->_escapeChar;
				} else {
				let escapeChar = null;
				}*/
		$escapeChar = null;

		if (is_array($columns)){
			$selectedColumns = array();
			foreach ($columns as $column) {
				/**
				 * Escape column name
				 */
				$columnItem = $column[0];
				if (is_array($columnItem)) {
					$columnSql = $this->getSqlExpression($columnItem, $escapeChar);
				} else {
					if ($columnItem == "*") {
						$columnSql = $columnItem;
					} else {
						/*if globals_get("db.escape_identifiers") {
						  let columnSql = escapeChar . columnItem . escapeChar;
						  } else {
						  let columnSql = columnItem;
						  }*/

						$columnSql = $columnItem;
					}
				}

				/**
				 * Escape column domain
				 */
				if (isset($column[1])) {
					$columnDomain = $column[1];
					if ($columnDomain) {
						/*if globals_get("db.escape_identifiers") {
						  let columnDomainSql = escapeChar . columnDomain . escapeChar . "." . columnSql;
						  } else {
						  let columnDomainSql = columnDomain . "." . columnSql;
						  }*/

						$columnDomainSql = $columnDomain . "." . $columnSql;
					} else {
						$columnDomainSql = $columnSql;
					}
				} else {
					$columnDomainSql = $columnSql;
				}

				/**
				 * Escape column alias
				 */
				if (isset($column[2])) {
					$columnAlias = $column[2];
					if ($columnAlias) {
						/*if globals_get("db.escape_identifiers") {
						  let columnAliasSql = columnDomainSql . " AS " . escapeChar . columnAlias . escapeChar;
						  } else {
						  let columnAliasSql = columnDomainSql . " AS " . columnAlias;
						  }*/
						$columnAliasSql = $columnDomainSql . " AS " . $columnAlias;
					} else {
						$columnAliasSql = $columnDomainSql;
					}
				} else {
					$columnAliasSql = $columnDomainSql;
				}
				$selectedColumns[] = $columnAliasSql;
			}
			$columnsSql = join(", ", $selectedColumns);
		} else {
			$columnsSql = $columns;
		}			

		/**
		 * Check and escape tables
		 */
		if (is_array($tables)) {
			$selectedTables = array();
			foreach ($tables as $table) {
				$selectedTables[] = $this->getSqlTable($table, $escapeChar);
			}
			$tablesSql = join(", ", $selectedTables);
		} else {
			$tablesSql = $tables;
		}

		$sql = "SELECT " . $columnsSql . " FROM " . $tablesSql;


		/**
		 * Check for joins
		 */
		if (isset($definition['joins'])) {
			$joins = $definition['joins'];
			foreach ( $joins as $join ) {

				$sqlTable = $this->getSqlTable($join["source"], $escapeChar);
					$selectedTables[] = $sqlTable;
					$sqlJoin = " " . $join["type"] . " JOIN " . $sqlTable;

				/**
				 * Check if the join has conditions
				 */
				$joinConditionsArray = $join['conditions'];
				if (isset($joinConditionsArray)) {
					if (count($joinConditionsArray)) {
						$joinExpressions = array();
						foreach ( $joinConditionsArray as $joinCondition) {
							$joinExpressions[] = $this->getSqlExpression($joinCondition, $escapeChar);
						}
						$sqlJoin .= " ON " . join(" AND ", $joinExpressions) . " ";
					}
				}
				$sql .= $sqlJoin;
			}
		}

		/**
		 * Check for a WHERE clause
		 */
		if (isset($definition['where']) ){
			$whereConditions = $definition['where'];
			if (is_array($whereConditions)) {
				$sql .= " WHERE " . $this->getSqlExpression($whereConditions, $escapeChar);
			} else {
				$sql .= " WHERE " . $whereConditions;
			}
		}

		/**
		 * Check for a GROUP clause
		 */
		if (isset($definition['group'])) {
			$groupFields = $definition['group']; 

			$groupItems = array();
			foreach ($groupFields as $groupField) {
				$groupItems[] = $this->getSqlExpression($groupField, $escapeChar);
			}
			$sql .= " GROUP BY " . join(", ", $groupItems);

			/**
			 * Check for a HAVING clause
			 */
			$havingConditions = $definition['having'];
			if (isset($havingConditions)) {
				$sql .= " HAVING " . $this->getSqlExpression($havingConditions, $escapeChar);
			}
		}

		/**
		 * Check for a ORDER clause
		 */
		if (isset($definition['order'])){
			$orderFields = $definition['order'];
			$orderItems = array();
			foreach ($orderFields as $orderItem) {
				$orderSqlItem = $this->getSqlExpression($orderItem[0], $escapeChar);

				/**
				 * In the numeric 1 position could be a ASC/DESC clause
				 */
				 $sqlOrderType = $orderItem[1];
				if (isset($sqlOrderType)) {
					$orderSqlItemType = $orderSqlItem . " " . $sqlOrderType;
				} else {
					$orderSqlItemType = $orderSqlItem;
				}
				$orderItems[] = $orderSqlItemType;
			}
			$sql .= " ORDER BY " . join(", ", $orderItems);
		}

		/**
		 * Check for a LIMIT condition
		 */
		 $limitValue = $definition["limit"];
		if (isset($limitValue)) {
			if (is_array($limitValue)) {

				$number = $limitValue["number"];

				/**
				 * Check for a OFFSET condition
				 */
				if (isset($limitValue['offset'])) {
					$offset = $limitValue['offset'];
					//$sql .= " LIMIT " . $number . " OFFSET " . $offset;
					$sql = $this->limit($sql, $number);
				} else {
					$sql = $this->limit($sql, $number);
				}
			} else {
				$sql = $this->limit($sql, $number);
			}
		}

		return $sql;
	}

	//public function getColumnList($columnList){}

	/**
	 * Gets the column name in MsSQL
	 *
	 * @param Phalcon\Db\ColumnInterface column
	 * @return string
	 */
	public function getColumnDefinition($column){

		$columnSql; $size; $scale;
		if ( !is_object($column )){
			throw new \Phalcon\Db\Exception("Column definition must be an object compatible with Phalcon\\Db\\ColumnInterface");
		}

		switch ((int)$column->getType()) {
			case \Phalcon\Db\Column::TYPE_INTEGER:
				$columnSql = "INT";
				break;
			case \Phalcon\Db\Column::TYPE_DATE:
				$columnSql = "DATE";
				break;
			case \Phalcon\Db\Column::TYPE_VARCHAR:
				$columnSql = "NCHAR(" . $column->getSize() . ")";
				break;
			case \Phalcon\Db\Column::TYPE_DECIMAL:
				$columnSql = "DECIMAL(" . $column->getSize() . "," . $column->getScale() . ")";
				break;
			case \Phalcon\Db\Column::TYPE_DATETIME:
				$columnSql = "DATETIME";
				break;
			case \Phalcon\Db\Column::TYPE_CHAR:
				$columnSql = "CHAR(" . $column->getSize() . ")";
				break;
			case \Phalcon\Db\Column::TYPE_TEXT:
				$columnSql = "TEXT";
				break;
			case \Phalcon\Db\Column::TYPE_FLOAT:
				$columnSql = "NUMERIC";	//FLOAT can't have range 
				$size = $column->getSize();
				if ($size) {
					$scale = $column->getScale();
					$columnSql .= "(" . $size;
					if ($scale) {
						$columnSql .= "," . $scale . ")";
					} else {
						$columnSql .= ")";
					}
				}
				break;
			default:
				throw new \Phalcon\Db\Exception("Unrecognized MsSQL data type: " . $column->getType());
		}
		return $columnSql;
	}

	public function addColumn($tableName, $schemaName, $column){
		$afterPosition; $sql;

		if (!is_object($column)) {
			throw new \Phalcon\Db\Exception("Column definition must be an object compatible with Phalcon\\Db\\ColumnInterface");
		}

		if ($schemaName) {
			$sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] ADD ";
		} else {
			$sql = "ALTER TABLE [" . $tableName . "] ADD ";
		}

		$sql .= "[" . $column->getName() . "] " . $this->getColumnDefinition($column);

		/* NOT NULL  alter with not ll is not allowed in mssql  
		   if ($column->isNotNull()) {
		   $sql .= " NOT NULL";
		   }
		 */

		if ($column->isFirst()) {
			$sql .= " FIRST";
		} else {
			$afterPosition = $column->getAfterPosition();
			if ($afterPosition) {
				$sql .=  " AFTER " . $afterPosition;
			}
		}
		return $sql;
	}
	public function modifyColumn($tableName, $schemaName, $column){
		$sql;

		if (!is_object($column)) {
			throw new \Phalcon\Db\Exception("Column definition must be an object compatible with Phalcon\\Db\\ColumnInterface");
		}

		if ($schemaName) {
			$sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] ALTER COLUMN ";
		} else {
			$sql = "ALTER TABLE [" . $tableName . "] ALTER COLUMN ";
		}

		$sql .= "[" . $column->getName() . "] " . $this->getColumnDefinition($column);

		/* NOT NULL  alter with not ll is not allowed in mssql  
		   if ($column->isNotNull()) {
		   $sql .= " NOT NULL";
		   }
		 */
		return $sql;
	}

	public function dropColumn($tableName, $schemaName, $columnName){
		$sql;

		if ($schemaName) {
			$sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] DROP COLUMN ";
		} else {
			$sql = "ALTER TABLE [" . $tableName . "] DROP COLUMN ";
		}

		$sql .= "[$columnName]";
		return $sql;
	}

	/*
	 * not done yet

	 CREATE UNIQUE NONCLUSTERED INDEX (indexname)
	 ON dbo.YourTableName(columns to include)
	 */
	public function addIndex($tableName, $schemaName, $index){
		$sql;
		if (!is_object($index)) {
			throw new Phalcon\Db\Exception("Index parameter must be an object compatible with Phalcon\\Db\\IndexInterface");
		}

		if ($schemaName) {
			$sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] ADD INDEX ";
		} else {
			$sql = "ALTER TABLE [" . $tableName . "] ADD INDEX ";
		}

		$sql .= "[" . $index->getName() . "] " . $this->getColumnDefinition($index->getColumns());

		return $sql;		
	}

	/*
	 * not done yet
	 */
	public function dropIndex($tableName, $schemaName, $indexName){
		$sql;

		if ($schemaName) {
			$sql = "DROP INDEX ($indexName) on [" . $schemaName . "].[" . $tableName . "] ";
		} else {
			$sql = "DROP INDEX ($indexName) on  [" . $tableName . "] ";
		}

		return $sql;	
	}

	public function addPrimaryKey($tableName, $schemaName, $index){

		$sql;
		if (!is_object($index)) {
			throw new Phalcon\Db\Exception("Index parameter must be an object compatible with Phalcon\\Db\\IndexInterface");
		}

		if ($schemaName) {
			$sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] ADD PRIMARY KEY ";
		} else {
			$sql = "ALTER TABLE [" . $tableName . "] ADD PRIMARY KEY ";
		}

		$sql .= "(" . $this->getColumnList($index->getColumns()) . ")";
		return $sql;
	}
	public function dropPrimaryKey($tableName, $schemaName){
		$sql;
		if ($schemaName) {
			$sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] DROP PRIMARY KEY ";
		} else {
			$sql = "ALTER TABLE [" . $tableName . "] DROP PRIMARY KEY ";
		}

		return $sql;

	}



	public function tableExists($tableName, $schemaName = null){
		$sql = "SELECT COUNT(*) FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_NAME] = '$tableName' ";

		if ($schemaName) {
			$sql = $sql . "AND TABLE_SCHEMA = '$schemaName'";
		}
		return $sql;
	}

	public function describeColumns($table, $schema = null){
		/* missing information for auto increment
		   $sql = "select * from [INFORMATION_SCHEMA].[COLUMNS] where [TABLE]_NAME='$table' ";

		   if ($schemaName) {
		   $sql = $sql . "AND TABLE_SCHEMA = '$schemaName'";
		   }
		 */
		$sql = "exec sp_columns [$table]";
		return $sql;
	}

	/**
	 * Returns a list of the tables in the database.
	 *
	 * @return array
	 */
	public function listTables($schemaName = null){
		//$sql =  "SELECT name FROM sysobjects WHERE type = 'U' ORDER BY name";
		$sql = "SELECT table_name FROM [INFORMATION_SCHEMA].[TABLES] ";
		if ($schemaName) {
			$sql = $sql . " WHERE TABLE_SCHEMA = '$schemaName'";
		}
		return $sql; 
	}
	/*
	   public function addForeignKey($tableName, $schemaName, $reference){}
	   public function dropForeignKey($tableName, $schemaName, $referenceName){}

	   public function createTable($tableName, $schemaName, $definition){}
	   public function dropTable($tableName, $schemaName){}

	   public function describeIndexes($table, $schema = null){}

	   public function describeReferences($table, $schema = null){}

	   public function tableOptions($table, $schema = null){}

	   public function supportsSavepoints(){}
	   public function supportsReleseSavepoints(){}

	   public function createSavepoint($name){}
	   public function releaseSavepoint($name){}
	   public function rollbackSavepoint($name){}
	 */
}
