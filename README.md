phalcon-mssql
=============

phalcon mssql pdo db adapter


the adapter works, but stille exist some issues
    1. scale?
    2. transaction
	can only run single transaction

some tese case did not pass
    -ModelsQueryExecuteTest.php
	-group by 1
        -table Abonnes is not exist - Line 781
