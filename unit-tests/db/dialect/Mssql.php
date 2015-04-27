<?php
namespace Twm\Db\Dialect;

class Mssql extends \Phalcon\Db\Dialect //implements \Phalcon\Db\DialectInterface
{

    public function limit($sqlQuery, $number)
    {
        $sql = preg_replace('/^SELECT\s/i', 'SELECT TOP ' . $number . ' ', $sqlQuery);
        return $sql;
    }

    public function forUpdate($sqlQuery)
    {
        $sql = $sqlQuery . ' WITH (UPDLOCK) ';
        return $sql;
    }

    /*
    public function shareLock($sqlQuery)
    {
    }

    public function getSqlTable($tables, $escapeChar = "\"")
    {
        if (!is_array($tables))
            return  $this->escaping($tables, $escapeChar);

        $result = array();
        foreach ($tables as $table) {
            $result[] = $this->escaping($table, $escapeChar);
        }
        return $result;
    }
    public function getSqlExpression($expressions, $escapeChar = "\"")
    {
        $domain = $this->escaping($expressions['domain'], $escapeChar);
        $name = $this->escaping($expressions['name'], $escapeChar);
        $result = "$domain.$name";
        return $result;
    }
    */

    protected function escaping($item, $escapeChar)
    {
        if (is_array($escapeChar)) {
            return $escapeChar[0] . $item . $escapeChar[1];
        } else {
            return $escapeChar . $item . $escapeChar;
        }
    }

    public function select($definition)
    {
        $tables;
        $columns;
        $escapeChar;
        $columnItem;
        $column;
        $selectedColumns;
        $columnSql;
        $columnDomainSql;
        $columnAlias;
        $selectedTables;    //global
        $sqlJoin;
        $joinExpressions;
        $joinCondition;
        $joinConditionsArray;
        $tablesSql;
        $columnDomain;
        $columnAliasSql;
        $columnsSql;
        $table;
        $sql;
        $joins;
        $join;
        $sqlTable;
        $whereConditions;
        $groupFields;
        $groupField;
        $groupItems;
        $havingConditions;
        $orderFields;
        $orderItem;
        $orderItems;
        $orderSqlItem;
        $sqlOrderType;
        $orderSqlItemType;
        $limitValue;
        $number;
        $offset;

        if (!is_array($definition)) {
            throw new Phalcon\Db\Exception("Invalid SELECT definition");
        }

        if (isset($definition['tables'])) {
            $tables = $definition["tables"];
        } else {
            throw new Phalcon\Db\Exception("The index 'tables' is required in the definition array");
        }

        if (isset($definition['columns'])) {
            $columns = $definition["columns"];
        } else {
            throw new Phalcon\Db\Exception("The index 'columns' is required in the definition array");
        }

        /*      if globals_get("db.escape_identifiers") {
                let escapeChar = this->_escapeChar;
                } else {
                let escapeChar = null;
                }*/
        //$escapeChar = array('[',']');
        $escapeChar = "\"";

        if (is_array($columns)) {
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

        $sql = "SELECT $columnsSql FROM $tablesSql with (NOLOCK) ";
        

        /**
         * Check for joins
         */
         $sqlJoins = '';
        if (isset($definition['joins'])) {
            $joins = $definition['joins'];
            foreach ($joins as $join) {

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
                        foreach ($joinConditionsArray as $joinCondition) {
                            $joinExpressions[] = $this->getSqlExpression($joinCondition, $escapeChar);
                        }
                        $sqlJoin .= " ON " . join(" AND ", $joinExpressions) . " ";
                    }
                }
                $sqlJoins .= $sqlJoin;

            }
        }

        /**
         * Check for a WHERE clause
         */
        $sqlWhere = '';
        if (isset($definition['where'])) {
            $whereConditions = $definition['where'];
            if (is_array($whereConditions)) {
                $sqlWhere .= " WHERE " . $this->getSqlExpression($whereConditions, $escapeChar);
            } else {
                $sqlWhere .= " WHERE " . $whereConditions;
            }
        }

        /**
         * Check for a GROUP clause
         */
         $sqlGroup = '';
        if (isset($definition['group'])) {
            $groupFields = $definition['group'];

            $groupItems = array();
            foreach ($groupFields as $groupField) {
                $groupItems[] = $this->getSqlExpression($groupField, $escapeChar);
            }
            $sqlGroup = " GROUP BY " . join(", ", $groupItems);

            /**
             * Check for a HAVING clause
             */
            if (isset($definition['having'])) {
                $havingConditions = $definition['having'];
                $sqlGroup .= " HAVING " . $this->getSqlExpression($havingConditions, $escapeChar);
            }
        }

        /**
         * Check for a ORDER clause
         */
        $sqlOrder = '';
        $nolockTokens = array('guid');
        if (isset($definition['order'])) {
            $nolock = false;
            $orderFields = $definition['order'];
            $orderItems = array();
            foreach ($orderFields as $orderItem) {
                $orderSqlItem = $this->getSqlExpression($orderItem[0], $escapeChar);

                /**
                 * In the numeric 1 position could be a ASC/DESC clause
                 */
                if (isset($orderItem[1])) {
                    $sqlOrderType = $orderItem[1];
                    $orderSqlItemType = $orderSqlItem . " " . $sqlOrderType;
                } else {
                    $orderSqlItemType = $orderSqlItem;
                }

                //check nolock
                if (in_array(strtolower($orderItem[0]['name']), $nolockTokens)) {
                    $nolock = true;
                } else {
                    $orderItems[] = $orderSqlItemType;
                }

            }
            if (count($orderItems)) {
                $sqlOrder =  " ORDER BY " . join(", ", $orderItems);
            }

            /*
            if ($nolock) {
                $sql .= " with (nolock) ";
            }
            */

        }


        $sql .= $sqlJoins . $sqlWhere . $sqlGroup . $sqlOrder;

        if (empty($sqlOrder)) {
            $sqlOrder == null;  //side effect, limit clause need =>  if (isset($sqlOrder) && !empty($sqlOrder))
        }

        /**
         * Check for a LIMIT condition
         */

        if (isset($definition['limit'])) {
            $limitValue = $definition["limit"];
            if (is_array($limitValue)) {
                //1.2 is a integer, but 1.3 is a array
                $number = $limitValue["number"];
                if (is_array($number)) {
                    $number = $number['value'];
                }

                if (isset($limitValue['offset'])) {
                    $offset = $limitValue["offset"];
                    if (is_array($offset)) {
                        $offset = $offset['value'];
                    }

                    $sql = $this->limit($sql, '100 PERCENT');
                    $startIndex = $offset + 1;//index start from 1
                    $endIndex = $startIndex + $number - 1;

                    $pos = strpos($sql, 'FROM');
                    $table = substr($sql, $pos + 4); //4 = FROM
                    $countPos = strpos($sql, 'COUNT');
                    if ($countPos) {
                        //if COUNT, take 'id' as default column, unless you have 'order'
                        if (isset($sqlOrder) && !empty($sqlOrder)) {
                            $sql = substr($sql, 0, $countPos) .  " *, ROW_NUMBER() OVER ($sqlOrder) AS rownum FROM $table";
                        } else {
                            $sql = substr($sql, 0, $countPos) . " *, ROW_NUMBER() OVER (Order By (SELECT COL_NAME(OBJECT_ID('{$selectedTables[0]}'), 1))) AS rownum FROM $table";
                        }
                    } else {
                        if (isset($sqlOrder) && !empty($sqlOrder)) {
                            $sql = substr($sql, 0, $pos) .  ", ROW_NUMBER() OVER ($sqlOrder) AS rownum FROM $table";
                        } else {
                            //if order is not giving, it will take first selected column for order.
                            $sql = substr($sql, 0, $pos) .  ", ROW_NUMBER() OVER (ORDER BY {$selectedColumns[0]}) AS rownum FROM $table";
                        }
                    }

                    //remove all column domain
                    $pureColumns = array();
                    foreach ($selectedColumns as $column) {
                        $pureColumn = substr($column, ($pos = strpos($column, '.')) !== false ? $pos + 1 : 0);
                        $pureColumns[] = $pureColumn;
                    }
                    $pureColumns = join(", ", $pureColumns);

                    $sql = "SELECT $pureColumns FROM ( $sql ) AS t WHERE t.rownum BETWEEN $startIndex AND $endIndex"; //don't break line

                } else {
                    $sql = $this->limit($sql, $number);
                }
            } else {
                $sql = $this->limit($sql, $number);
            }
        }
        return $sql;
    }


    public function getColumnList($columnList)
    {
        //exec sp_columns 'table name'
    }

    /**
     * Gets the column name in MsSQL
     *
     * @param Phalcon\Db\ColumnInterface column
     * @return string
     */
    public function getColumnDefinition($column)
    {
        $columnSql;
        $size;
        $scale;
        if (!is_object($column)) {
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
                $columnSql = "NUMERIC"; //FLOAT can't have range
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
                throw new \Phalcon\Db\Exception("Unrecognized Mssql data type: " . $column->getType());
        }
        return $columnSql;
    }

    public function addColumn($tableName, $schemaName, $column)
    {
        $afterPosition;
        $sql;

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
    public function modifyColumn($tableName, $schemaName, $column)
    {
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

    public function dropColumn($tableName, $schemaName, $columnName)
    {
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
    public function addIndex($tableName, $schemaName, $index)
    {
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
    public function dropIndex($tableName, $schemaName, $indexName)
    {
        $sql;

        if ($schemaName) {
            $sql = "DROP INDEX ($indexName) on [" . $schemaName . "].[" . $tableName . "] ";
        } else {
            $sql = "DROP INDEX ($indexName) on  [" . $tableName . "] ";
        }

        return $sql ;
    }

    public function addPrimaryKey($tableName, $schemaName, $index)
    {
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
    public function dropPrimaryKey($tableName, $schemaName)
    {
        $sql;
        if ($schemaName) {
            $sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] DROP PRIMARY KEY ";
        } else {
            $sql = "ALTER TABLE [" . $tableName . "] DROP PRIMARY KEY ";
        }

        return $sql;

    }



    public function tableExists($tableName, $schemaName = null)
    {
        $sql = "SELECT COUNT(*) FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_NAME] = '$tableName' ";

        if ($schemaName) {
            $sql = $sql . "AND TABLE_SCHEMA = '$schemaName'";
        }
        return $sql;
    }

    /**
     * Generates SQL checking for the existence of a schema.view
     *
     * @param string viewName
     * @param string schemaName
     * @return string
     */
    public function viewExists($viewName, $schemaName = null)
    {
        if ($schemaName) {
            return "SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE table_name = '$viewName' and table_schema = '$schemaName'";
        }
        return "SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE table_name = '$viewName'";
    }

    public function describeColumns($table, $schema = null)
    {
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
    public function listTables($schemaName = null)
    {
        //$sql =  "SELECT name FROM sysobjects WHERE type = 'U' ORDER BY name";
        $sql = "SELECT table_name FROM [INFORMATION_SCHEMA].[TABLES] ";
        if ($schemaName) {
            $sql = $sql . " WHERE TABLE_SCHEMA = '$schemaName'";
        }
        return $sql;
    }

    /**
     * Generates the SQL to list all views of a schema or user
     *
     * @param string schemaName
     * @return array
     */
    public function listViews($schemaName = null)
    {
        if ($schemaName) {
            return "SELECT [TABLE_NAME] AS view_name FROM [INFORMATION_SCHEMA].[VIEWS] WHERE `TABLE_SCHEMA` = '" . $schemaName . "' ORDER BY view_name";
        }
        return "SELECT [TABLE_NAME] AS view_name FROM [INFORMATION_SCHEMA].[VIEWS] ORDER BY view_name";
    }

    /**
     * Generates SQL to create a view
     *
     * @param string viewName
     * @param array definition
     * @param string schemaName
     * @return string
     */
    public function createView($viewName, $definition, $schemaName)
    {
        $view;
        $viewSql;

        if (!isset($definition['sql'])) {
            throw new Phalcon\Db\Exception("The index 'sql' is required in the definition array");
        }
        $viewSql = $definition['sql'];

        if ($schemaName) {
            $view = "[$schemaName].[$viewName]";
        } else {
            $view = "[$viewName]";
        }

        return "CREATE VIEW $view AS $viewSql";
    }

    /**
     * Generates SQL to drop a view
     *
     * @param string viewName
     * @param string schemaName
     * @param boolean ifExists
     * @return string
     */
    public function dropView($viewName, $schemaName, $ifExists = true)
    {
        $sql="";
        $view;

        if ($schemaName) {
            $view = "$schemaName.$viewName";
        } else {
            $view = "$viewName";
        }

        if ($ifExists) {
            if ($schemaName) {
                $sql = "IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = '$viewName' AND TABLE_SCHEMA = '$schemaName') ";
            } else {
                $sql = "IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = '$view') ";
            }
        }
        $sql .= "DROP VIEW " .  $view;
        return $sql;
    }


    /**
     * Generates SQL to query indexes on a table
     *
     * @param   string table
     * @param   string schema
     * @return  string
     * TODO schema not finish yet
     */
    public function describeIndexes($table, $schema = null)
    {
        $sql = "SELECT * FROM sys.indexes ind INNER JOIN sys.tables t ON ind.object_id = t.object_id WHERE t.name = '$table' ";
        if ($schema) {
            //$sql .= "AND t."
        }
        return $sql;
    }

    /**
     * Generates SQL to query foreign keys on a table
     *
     * @param   string table
     * @param   string schema
     * @return  string
     */
    public function describeReferences($table, $schema = null)
    {
        $sql = "SELECT TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME,REFERENCED_TABLE_SCHEMA,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_NAME IS NOT NULL AND ";
        if ($schema) {
            $sql .= "CONSTRAINT_SCHEMA = '" . $schema . "' AND TABLE_NAME = '" . $table . "'";
        } else {
            $sql .= "TABLE_NAME = '" . $table . "'";
        }
        return $sql;
    }


    /**
     * Generates the SQL to describe the table creation options
     *
     * @param   string table
     * @param   string schema
     * @return  string
     */
    public function tableOptions($table, $schema = null)
    {
        $sql = "SELECT TABLES.TABLE_TYPE AS table_type,TABLES.AUTO_INCREMENT AS auto_increment,TABLES.ENGINE AS engine,TABLES.TABLE_COLLATION AS table_collation FROM INFORMATION_SCHEMA.TABLES WHERE ";
        if ($schema) {
            $sql .= "TABLES.TABLE_SCHEMA = '" . $schema . "' AND TABLES.TABLE_NAME = '" . $table . "'";
        } else {
            $sql .= "TABLES.TABLE_NAME = '" . $table . "'";
        }
        return $sql;
    }

    public function addForeignKey($tableName, $schemaName, $reference)
    {
    }

    public function dropForeignKey($tableName, $schemaName, $referenceName)
    {
    }

    public function createTable($tableName, $schemaName, $definition)
    {
    }

    public function dropTable($tableName, $schemaName)
    {
    }

    public function supportsSavepoints()
    {
    }

    public function supportsReleseSavepoints()
    {
    }

    public function createSavepoint($name)
    {
    }

    public function releaseSavepoint($name)
    {
    }

    public function rollbackSavepoint($name)
    {
    }
}
