phalcon-mssql
=============

phalcon mssql pdo db adapter

the adapter works, but stille exist some issues
    1. scale?
    2. transaction
	can only run single transaction
    3. offset
	paging works, but need "order" claues, if not existing, it will choose "id" as default, so hope your table has column "id" 

some tese case did not pass
    -ModelsQueryExecuteTest.php
	-group by 1
        -table Abonnes is not exist - Line 781
