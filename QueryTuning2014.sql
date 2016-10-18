--Chapter 2


SELECT  dopc.cntr_value,
        dopc.cntr_type
FROM    sys.dm_os_performance_counters AS dopc
WHERE   dopc.object_name =  'SQLServer:General Statistics' 
        AND dopc.counter_name =  'Logins/sec';




SELECT  TOP (10) dows.*
FROM    sys.dm_os_wait_stats AS dows
ORDER BY dows.wait_time_ms DESC;










EXEC sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
GO
EXEC sp_configure  'min server memory'; 
EXEC sp_configure  'max server memory';




USE master;
EXEC sp_configure  'show advanced option',   1;
RECONFIGURE;
exec sp_configure  'min server memory (MB)',  5000;
exec sp_configure  'max server memory (MB)',  10000;
RECONFIGURE WITH OVERRIDE;



DBCC MEMORYSTATUS;





select * FROM sys.dm_db_xtp_table_memory_stats;






SELECT * FROM Sys.dm_xtp_system_memory_consumers;




--Chapter 3

SELECT  *
FROM    sys.dm_io_virtual_file_stats(DB_ID('AdventureWorks2012'), 2) AS divfs;







SELECT  *
FROM    sys.dm_os_wait_stats AS dows
WHERE   wait_type LIKE 'PAGEIOLATCH%';





--Chapter 5


ALTER DATABASE AdventureWorks2012 ADD FILEGROUP Indexes ;
ALTER DATABASE AdventureWorks2012 ADD FILE (NAME = AdventureWorks2012_Data2,
        FILENAME =  'C:\DATA\AdventureWorks2012_2.ndf',
        SIZE = 1mb,
        FILEGROWTH = 10%) TO FILEGROUP Indexes;




USE AdventureWorks2012;
GO

SELECT  jc.JobCandidateID,
        e.ModifiedDate
FROM    HumanResources.JobCandidate AS jc
        INNER JOIN HumanResources.Employee AS e
        ON jc.BusinessEntityID = e.BusinessEntityID;




USE master;
GO
sp_detach_db 'AdventureWorks2012';
GO
USE master;
GO
sp_attach_db 'AdventureWorks2008AdventureWorks2012R2'
, 'R:\DATA\AdventureWorks2008AdventureWorks2012.mdf'
, 'F:\DATA\AdventureWorks2008AdventureWorks2012_2.ndf'
, 'S:\LOG\AdventureWorks2008AdventureWorks2012.1df ';
GO
USE Adventureworks2012;
GO
SELECT * FROM sys.database_files;
GO







CREATE INDEX IndexBirthDate 
ON HumanResources.Employee (BirthDate) 
ON Indexes;






--Chapter 6

CREATE EVENT SESSION [Query Performance Metrics] ON SERVER 
ADD EVENT sqlserver.rpc_completed
GO


ALTER EVENT SESSION [Query Performance Metrics]
ON SERVER STATE = START;




SELECT  dxs.name,
        dxs.create_time
FROM    sys.dm_xe_sessions AS dxs;






ALTER EVENT SESSION [Query Performance Metrics]
ON SERVER
STATE = STOP;





---Chapter 7

SELECT  *
FROM    sys.fn_xe_file_target_read_file('C:\Sessions\QueryPerformanceMetrics*.xel',
                                        NULL, NULL, NULL);






WITH    xEvents
          AS (SELECT    object_name AS xEventName,
                        CAST (event_data AS XML) AS xEventData
              FROM      sys.fn_xe_file_target_read_file('C:\Metrics\QueryPerformanceMetrics*.xel',
                                                        NULL, NULL, NULL)
             )
    SELECT  xEventName,
            xEventData.value('(/event/data[@name=''duration'']/value)[1]',
                             'bigint') Duration,
            xEventData.value('(/event/data[@name=''physical_reads'']/value)[1]',
                             'bigint') PhysicalReads,
            xEventData.value('(/event/data[@name=''logical_reads'']/value)[1]',
                             'bigint') LogicalReads,
            xEventData.value('(/event/data[@name=''cpu_time'']/value)[1]',
                             'bigint') CpuTime,
            CASE xEventName
              WHEN 'sql_batch_completed'
              THEN xEventData.value('(/event/data[@name=''batch_text'']/value)[1]',
                                    'varchar(max)')
              WHEN 'rpc_completed'
              THEN xEventData.value('(/event/data[@name=''statement'']/value)[1]',
                                    'varchar(max)')
            END AS SQLText,
            xEventData.value('(/event/data[@name=''query_hash'']/value)[1]',
                             'binary(8)') QueryHash
    INTO    #Session_Table
    FROM    xEvents;


SELECT  st.xEventName,
        st.Duration,
        st.PhysicalReads,
        st.LogicalReads,
        st.CpuTime,
        st.SQLText,
        st.QueryHash
FROM    #Session_Table AS st
ORDER BY st.LogicalReads DESC;

--DROP TABLE dbo.#Session_Table;




SELECT COUNT(*) AS TotalExecutions,
	st.xEventName,
	st.SQLText,
	SUM(st.Duration) AS DurationTotal,
	SUM(st.CpuTime) AS CpuTotal,
	SUM(st.LogicalReads) AS LogicalReadTotal,
	SUM(st.PhysicalReads) AS PhysicalReadTotal
FROM Session_Table AS st
GROUP BY st.xEventName, st.SQLText
ORDER BY LogicalReadTotal DESC;



SELECT  s.totalexecutioncount,
        t.text,
        s.TotalExecutionCount,
        s.TotalElapsedTime,
        s.TotalLogicalReads,
        s.TotalPhysicalReads
FROM    (SELECT deqs.plan_handle,
                SUM(deqs.execution_count) AS TotalExecutionCount,
                SUM(deqs.total_elapsed_time) AS TotalElapsedTime,
                SUM(deqs.total_logical_reads) AS TotalLogicalReads,
                SUM(deqs.total_physical_reads) AS TotalPhysicalReads
         FROM   sys.dm_exec_query_stats AS deqs
         GROUP BY deqs.plan_handle
        ) AS s
        CROSS APPLY sys.dm_exec_sql_text(s.plan_handle) AS t
ORDER BY s.TotalLogicalReads DESC;



SELECT  s.TotalExecutionCount,
        t.text,
        s.TotalExecutionCount,
        s.TotalElapsedTime,
        s.TotalLogicalReads,
        s.TotalPhysicalReads
FROM    (SELECT deqs.query_plan_hash,
                SUM(deqs.execution_count) AS TotalExecutionCount,
                SUM(deqs.total_elapsed_time) AS TotalElapsedTime,
                SUM(deqs.total_logical_reads) AS TotalLogicalReads,
                SUM(deqs.total_physical_reads) AS TotalPhysicalReads
         FROM   sys.dm_exec_query_stats AS deqs
         GROUP BY deqs.query_plan_hash
        ) AS s
        CROSS APPLY (SELECT plan_handle
                     FROM   sys.dm_exec_query_stats AS deqs
                     WHERE  s.query_plan_hash = deqs.query_plan_hash
                    ) AS p
        CROSS APPLY sys.dm_exec_sql_text(p.plan_handle) AS t
ORDER BY TotalLogicalReads DESC;



WITH xEvents AS
(SELECT object_name AS xEventName,
	CAST (event_data AS xml) AS xEventData
FROM sys.fn_xe_file_target_read_file
('C:\Sessions\QueryPerformanceMetrics*.xel', NULL, NULL, NULL)
)
SELECT 
	xEventName,
	xEventData.value('(/event/data[@name=''duration'']/value)[1]','bigint') Duration,
	xEventData.value('(/event/data[@name=''physical_reads'']/value)[1]','bigint') PhysicalReads,
	xEventData.value('(/event/data[@name=''logical_reads'']/value)[1]','bigint') LogicalReads,
	xEventData.value('(/event/data[@name=''cpu_time'']/value)[1]','bigint') CpuTime,
	xEventData.value('(/event/data[@name=''batch_text'']/value)[1]','varchar(max)') BatchText,
	xEventData.value('(/event/data[@name=''statement'']/value)[1]','varchar(max)') StatementText,
	xEventData.value('(/event/data[@name=''query_plan_hash'']/value)[1]','binary(8)') QueryPlanHash
FROM xEvents
ORDER BY Duration DESC;




USE AdventureWorks2012;
GO
SET SHOWPLAN_XML ON
GO
SELECT  soh.AccountNumber,
        sod.LineTotal,
        sod.OrderQty,
        sod.UnitPrice,
        p.Name
FROM    Sales.SalesOrderHeader soh
JOIN    Sales.SalesOrderDetail sod
        ON soh.SalesOrderID = sod.SalesOrderID
JOIN    Production.Product p
        ON sod.ProductID = p.ProductID
WHERE   sod.LineTotal > 20000 ;

GO
SET SHOWPLAN_XML OFF
GO








SELECT  p.*
FROM    Production.Product p
        JOIN Production.ProductCategory pc
        ON p.ProductSubcategoryID = pc.ProductCategoryID;






SELECT  pm.*
FROM    Production.ProductModel pm
JOIN    Production.ProductModelProductDescriptionCulture pmpd
        ON pm.ProductModelID = pmpd.ProductModelID ;






SELECT  pm.*
FROM    Production.ProductModel pm
JOIN    Production.ProductModelProductDescriptionCulture pmpd
        ON pm.ProductModelID = pmpd.ProductModelID 
		WHERE pm.Name = 'HL Mountain Front Wheel';





IF (SELECT  OBJECT_ID('p1')
   ) IS NOT NULL
    DROP PROC p1 
GO
CREATE PROC p1
AS
    CREATE TABLE t1 (c1 INT);
    INSERT  INTO t1
            SELECT  ProductID
            FROM    Production.Product;
    SELECT  *
    FROM    t1;
    DROP TABLE t1; 
GO


SET SHOWPLAN_XML ON;
GO
EXEC p1 ;
GO
SET SHOWPLAN_XML OFF;
GO




SET STATISTICS XML ON;
GO
EXEC p1;
GO
SET STATISTICS XML OFF
GO






SELECT  p.query_plan,
        t.text
FROM    sys.dm_exec_cached_plans r
        CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) p
        CROSS APPLY sys.dm_exec_sql_text(r.plan_handle) t;




SELECT TOP 100
        p.*
FROM    Production.Product p;



DBCC freeproccache();

SET STATISTICS TIME ON
GO
SELECT  soh.AccountNumber,
        sod.LineTotal,
        sod.OrderQty,
        sod.UnitPrice,
        p.Name
FROM    Sales.SalesOrderHeader soh
        JOIN Sales.SalesOrderDetail sod
        ON soh.SalesOrderID = sod.SalesOrderID
        JOIN Production.Product p
        ON sod.ProductID = p.ProductID
WHERE   sod.LineTotal > 1000; 
GO
SET STATISTICS TIME OFF 
GO








SET STATISTICS IO ON;
GO
SELECT  soh.AccountNumber,
        sod.LineTotal,
        sod.OrderQty,
        sod.UnitPrice,
        p.Name
FROM    Sales.SalesOrderHeader soh
        JOIN Sales.SalesOrderDetail sod
        ON soh.SalesOrderID = sod.SalesOrderID
        JOIN Production.Product p
        ON sod.ProductID = p.ProductID
WHERE   sod.SalesOrderID = 71856; 
GO
SET STATISTICS IO OFF;
GO





--Chapter 8

SELECT TOP 10
        p.ProductID,
        p.[Name],
        p.StandardCost,
        p.[Weight],
        ROW_NUMBER() OVER (ORDER BY p.Name DESC) AS RowNumber
FROM    Production.Product p
ORDER BY p.Name DESC;







SELECT TOP 10
        p.ProductID,
        p.[Name],
        p.StandardCost,
        p.[Weight],
        ROW_NUMBER() OVER (ORDER BY p.Name DESC) AS RowNumber
FROM    Production.Product p
ORDER BY p.StandardCost DESC;






IF (SELECT  OBJECT_ID('Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1; 
GO
CREATE TABLE dbo.Test1
    (
     C1 INT,
     C2 INT,
     C3 VARCHAR(50)
    ); 

WITH    Nums
          AS (SELECT TOP (10000)
                        ROW_NUMBER() OVER (ORDER BY (SELECT 1
                                                    )) AS n
              FROM      master.sys.All_Columns ac1
                        CROSS JOIN master.sys.All_Columns ac2
             )
    INSERT  INTO dbo.Test1
            (C1, C2, C3)
            SELECT  n,
                    n,
                    'C3'
            FROM    Nums;




UPDATE  dbo.Test1
SET     C1 = 1,
        C2 = 1
WHERE   C2 = 1;



CREATE CLUSTERED INDEX iTest 
ON dbo.Test1(C1);




CREATE INDEX iTest2 
ON dbo.Test1(C2);



UPDATE  dbo.Test1
SET     C1 = 1,
        C2 = 1
WHERE   C2 = 1;







SELECT  p.ProductID,
        p.Name,
        p.StandardCost,
        p.Weight
FROM    Production.Product p;





SELECT  p.ProductID,
        p.Name,
        p.StandardCost,
        p.Weight
FROM    Production.Product AS p
WHERE   p.ProductID = 738;






IF (SELECT  OBJECT_ID('Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1; 
GO
CREATE TABLE dbo.Test1 (C1 INT, C2 INT); 

WITH    Nums
          AS (SELECT    1 AS n
              UNION ALL
              SELECT    n + 1
              FROM      Nums
              WHERE     n < 20
             )
    INSERT  INTO dbo.Test1
            (C1, C2)
            SELECT  n,
                    2
            FROM    Nums;

CREATE INDEX iTest ON dbo.Test1(C1);



SELECT  i.Name,
        i.type_desc,
        ddips.page_count,
        ddips.record_count,
        ddips.index_level
FROM    sys.indexes i
        JOIN sys.dm_db_index_physical_stats(DB_ID(N'AdventureWorks2012'),
                                            OBJECT_ID(N'dbo.Test1'), NULL,
                                            NULL, 'DETAILED') AS ddips
        ON i.index_id = ddips.index_id
WHERE   i.object_id = OBJECT_ID(N'dbo.Test1');





DROP INDEX dbo.Test1.iTest;
ALTER TABLE dbo.Test1 ALTER COLUMN C1 CHAR(500);
CREATE INDEX iTest ON dbo.Test1(C1);





DROP TABLE dbo.Test1;





SELECT  COUNT(DISTINCT e.MaritalStatus) AS DistinctColValues,
        COUNT(e.MaritalStatus) AS NumberOfRows,
        (CAST(COUNT(DISTINCT e.MaritalStatus) AS DECIMAL)
         / CAST(COUNT(e.MaritalStatus) AS DECIMAL)) AS Selectivity,
		 (1.0/(COUNT(DISTINCT e.MaritalStatus))) AS Density
FROM    HumanResources.Employee AS e;







SELECT  e.*
FROM    HumanResources.Employee AS e
WHERE   e.MaritalStatus = 'M'
        AND e.BirthDate = '1984-12-05'
        AND e.Gender = 'M';




CREATE INDEX IX_Employee_Test ON HumanResources.Employee (Gender)
WITH (DROP_EXISTING = ON)
;


CREATE INDEX IX_Employee_Test ON
HumanResources.Employee (BirthDate,  Gender,  MaritalStatus)
WITH  (DROP_EXISTING = ON);





SELECT  e.*
FROM    HumanResources.Employee AS e WITH (INDEX (IX_Employee_Test))
WHERE   e.BirthDate = '1984-12-05'
        AND e.Gender = 'M'
        AND e.MaritalStatus = 'M';







SELECT  e.*
FROM    HumanResources.Employee AS e WITH (FORCESEEK)
WHERE   e.BirthDate = '1984-12-05'
        AND e.Gender = 'M'
        AND e.MaritalStatus = 'M';





DROP INDEX HumanResources.Employee.IX_Employee_Test;






CREATE INDEX IX_Test ON Person.Address (City, PostalCode);


SELECT  a.*
FROM    Person.Address AS a
WHERE   a.City = 'Dresden';


SELECT COUNT(*), a.City, a.PostalCode
FROM Person.Address AS a
GROUP BY a.City, a.PostalCode
HAVING COUNT(*) = 31;



SELECT  *
FROM    Person.Address AS a
WHERE   a.PostalCode = 'WA3 7BH';



SELECT  a.AddressID,
        a.City,
        a.PostalCode
FROM    Person.Address AS a
WHERE   a.City = 'Gloucestershire'
        AND a.PostalCode = 'GL7 1RY';






DROP INDEX Person.Address.IX_Test;



SELECT  dl.DatabaseLogID,
        dl.PostTime
FROM    dbo.DatabaseLog AS dl
WHERE   dl.DatabaseLogID = 115;





SELECT  d.DepartmentID,
        d.ModifiedDate
FROM    HumanResources.Department AS d
WHERE   d.DepartmentID = 10;




IF (SELECT  OBJECT_ID('Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1; 
GO
CREATE TABLE dbo.Test1 (C1 INT, C2 INT); 

WITH    Nums
          AS (SELECT TOP (20)
                        ROW_NUMBER() OVER (ORDER BY (SELECT 1
                                                    )) AS n
              FROM      Master.sys.All_Columns ac1
                        CROSS JOIN Master.sys.ALL_Columns ac2
             )
    INSERT  INTO dbo.Test1
            (C1, C2)
            SELECT  n,
                    n + 1
            FROM    Nums;
            
CREATE CLUSTERED INDEX iClustered 
ON dbo.Test1  (C2); 

CREATE NONCLUSTERED INDEX iNonClustered 
ON dbo.Test1  (C1);






SELECT  i.name,
        i.type_desc,
        s.page_count,
        s.record_count,
        s.index_level
FROM    sys.indexes i
        JOIN sys.dm_db_index_physical_stats(DB_ID(N'AdventureWorks2012'),
                                            OBJECT_ID(N'dbo.Test1'), NULL,
                                            NULL, 'DETAILED') AS s
        ON i.index_id = s.index_id
WHERE   i.object_id = OBJECT_ID(N'dbo.Test1');



DROP INDEX dbo.Test1.iClustered;
ALTER TABLE dbo.Test1 ALTER COLUMN C2 CHAR(500);
CREATE CLUSTERED INDEX iClustered ON dbo.Test1(C2);










IF (SELECT  OBJECT_ID('od')
   ) IS NOT NULL
    DROP TABLE dbo.od;
 GO
SELECT  pod.*
INTO    dbo.od
FROM    Purchasing.PurchaseOrderDetail AS pod;






EXEC sp_helpindex 'dbo.od';





SELECT  od.*
FROM    dbo.od
WHERE   od.ProductID BETWEEN 500 AND 510
ORDER BY od.ProductID;





CREATE CLUSTERED INDEX i1 ON od(ProductID);


DROP INDEX od.i1;
CREATE NONCLUSTERED INDEX i1 ON dbo.od(ProductID);



DROP TABLE dbo.od;





BEGIN TRAN
SET STATISTICS IO ON ;
UPDATE  Sales.SpecialOfferProduct
SET     ProductID = 345
WHERE   SpecialOfferID = 1
        AND  ProductID = 720 ;
SET STATISTICS IO OFF ;
ROLLBACK TRAN


CREATE NONCLUSTERED INDEX ixTest 
ON Sales.SpecialOfferProduct (ModifiedDate) ;


DROP INDEX Sales.SpecialOfferProduct.ixTest ;







IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1;
GO
CREATE TABLE dbo.Test1 (C1 INT,C2 INT);

WITH    Nums
          AS (SELECT TOP (10000)
                        ROW_NUMBER() OVER (ORDER BY (SELECT 1
                                                    )) AS n
              FROM      Master.sys.all_columns AS ac1
                        CROSS JOIN Master.sys.all_columns AS ac2
             )
    INSERT  INTO dbo.Test1
            (C1,C2)
            SELECT  n,
                    2
            FROM    Nums;





SELECT  t.C1,
        t.C2
FROM    dbo.Test1 AS t
WHERE   C1 = 1000;



CREATE NONCLUSTERED INDEX incl ON dbo.Test1(C1) ;

CREATE CLUSTERED INDEX icl ON dbo.Test1(C1) ;






SELECT  cc.CreditCardID,
        cc.CardNumber,
        cc.ExpMonth,
        cc.ExpYear
FROM    Sales.CreditCard AS cc
WHERE   cc.ExpMonth BETWEEN 6 AND 9
        AND cc.ExpYear = 2008
ORDER BY cc.ExpMonth;



CREATE NONCLUSTERED INDEX ixTest 
ON Sales.CreditCard (ExpMonth, ExpYear)
INCLUDE (CardNumber) ;


DROP INDEX Sales.CreditCard.ixTest ;



--Chapter 9

SELECT  a.PostalCode
FROM    Person.Address AS a
WHERE   a.StateProvinceID = 42;



CREATE NONCLUSTERED INDEX [IX_Address_StateProvinceID] 
ON [Person].[Address]  ([StateProvinceID] ASC)
INCLUDE  (PostalCode)
WITH (
DROP_EXISTING = ON);


CREATE NONCLUSTERED INDEX [IX_Address_StateProvinceID] 
ON [Person].[Address]  ([StateProvinceID] ASC)
--INCLUDE  (PostalCode)
WITH (
DROP_EXISTING = ON);



SELECT  soh.*
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.SalesPersonID = 276
        AND soh.OrderDate BETWEEN '4/1/2005' AND '7/1/2005';


CREATE NONCLUSTERED INDEX IX_Test 
ON Sales.SalesOrderHeader (OrderDate);


DROP INDEX Sales.SalesOrderHeader.IX_Test;



SELECT  soh.SalesPersonID,
        soh.OrderDate
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.SalesPersonID = 276
        AND soh.OrderDate BETWEEN '4/1/2005' AND '7/1/2005';


CREATE NONCLUSTERED INDEX IX_Test 
ON Sales.SalesOrderHeader (OrderDate ASC) ;



SELECT  soh.SalesPersonID,
        soh.OrderDate
FROM    Sales.SalesOrderHeader AS soh WITH 
			(INDEX (IX_Test,
                    IX_SalesOrderHeader_SalesPersonID))
WHERE   soh.OrderDate BETWEEN '4/1/2005' AND '7/1/2005';







SELECT  soh.PurchaseOrderNumber,
        soh.OrderDate,
        soh.ShipDate,
        soh.SalesPersonID
FROM    Sales.SalesOrderHeader AS soh
WHERE   PurchaseOrderNumber LIKE 'PO5%'
        AND soh.SalesPersonID IS NOT NULL;




CREATE NONCLUSTERED INDEX IX_Test 
ON Sales.SalesOrderHeader(PurchaseOrderNumber ,SalesPersonID)
INCLUDE  (OrderDate,ShipDate);

CREATE NONCLUSTERED INDEX IX_Test 
ON Sales.SalesOrderHeader(PurchaseOrderNumber,SalesPersonID) 
INCLUDE  (OrderDate,ShipDate) 
WHERE PurchaseOrderNumber IS NOT NULL AND SalesPersonID IS NOT NULL 
WITH  (DROP_EXISTING = ON);


DROP INDEX Sales.SalesOrderHeader.IX_Test;






SELECT  p.[Name] AS ProductName,
        SUM(pod.OrderQty) AS OrderOty,
        SUM(pod.ReceivedQty) AS ReceivedOty,
        SUM(pod.RejectedQty) AS RejectedOty
FROM    Purchasing.PurchaseOrderDetail AS pod
        JOIN Production.Product AS p
        ON p.ProductID = pod.ProductID
GROUP BY p.[Name];

SELECT  p.[Name] AS ProductName,
        SUM(pod.OrderQty) AS OrderOty,
        SUM(pod.ReceivedQty) AS ReceivedOty,
        SUM(pod.RejectedQty) AS RejectedOty
FROM    Purchasing.PurchaseOrderDetail AS pod
        JOIN Production.Product AS p
        ON p.ProductID = pod.ProductID
GROUP BY p.[Name]
HAVING  (SUM(pod.RejectedQty) / SUM(pod.ReceivedQty)) > .08;

SELECT  p.[Name] AS ProductName,
        SUM(pod.OrderQty) AS OrderQty,
        SUM(pod.ReceivedQty) AS ReceivedQty,
        SUM(pod.RejectedQty) AS RejectedQty
FROM    Purchasing.PurchaseOrderDetail AS pod
        JOIN Production.Product AS p
        ON p.ProductID = pod.ProductID
WHERE   p.[Name] LIKE 'Chain%'
GROUP BY p.[Name];


IF EXISTS ( SELECT  *
            FROM    sys.views
            WHERE   object_id = OBJECT_ID(N'[Purchasing].[IndexedView]') )
    DROP VIEW [Purchasing].[IndexedView];
GO
CREATE VIEW Purchasing.IndexedView
WITH SCHEMABINDING
AS
SELECT  pod.ProductID,
        SUM(pod.OrderQty) AS OrderQty,
        SUM(pod.ReceivedQty) AS ReceivedQty,
        SUM(pod.RejectedQty) AS RejectedQty,
        COUNT_BIG(*) AS [Count]
FROM    Purchasing.PurchaseOrderDetail AS pod
GROUP BY pod.ProductID;
GO
CREATE UNIQUE CLUSTERED INDEX iv 
ON Purchasing.IndexedView (ProductID); 
GO



SELECT  iv.ProductID,
        iv.ReceivedQty,
        iv.RejectedQty
FROM    Purchasing.IndexedView AS iv;



DROP VIEW Purchasing.IndexedView;




CREATE NONCLUSTERED INDEX IX_Test
ON Person.Address(City ASC, PostalCode ASC);



CREATE NONCLUSTERED INDEX IX_Comp_Test 
ON Person.Address (City,PostalCode) 
WITH (DATA_COMPRESSION = ROW);

CREATE NONCLUSTERED INDEX IX_Comp_Page_Test 
ON Person.Address  (City,PostalCode) 
WITH (DATA_COMPRESSION = PAGE);



SELECT  i.Name,
        i.type_desc,
        s.page_count,
        s.record_count,
        s.index_level,
        compressed_page_count
FROM    sys.indexes i
        JOIN sys.dm_db_index_physical_stats(DB_ID(N'AdventureWorks2012'),
                                            OBJECT_ID(N'Person.Address'),NULL,
                                            NULL,'DETAILED') AS s
        ON i.index_id = s.index_id
WHERE   i.OBJECT_ID = OBJECT_ID(N'Person.Address');


SELECT  a.City,
        a.PostalCode
FROM    Person.Address AS a
WHERE   a.City = 'Newton'
        AND a.PostalCode = 'V2M1N7';


SELECT  a.City,
        a.PostalCode
FROM    Person.Address AS a WITH (INDEX = IX_Test)
WHERE   a.City = 'Newton'
        AND a.PostalCode = 'V2M1N7';





SELECT  tha.ProductID,
        COUNT(tha.ProductID) AS CountProductID,
        SUM(tha.Quantity) AS SumQuantity,
        AVG(tha.ActualCost) AS AvgActualCost
FROM    Production.TransactionHistoryArchive AS tha
GROUP BY tha.ProductID;


DROP INDEX Person.Address.IX_Test; 
DROP INDEX Person.Address.IX_Comp_Test; 
DROP INDEX Person.Address.IX_Comp_Page_Test;






SELECT  tha.ProductID,
        COUNT(tha.ProductID) AS CountProductID,
        SUM(tha.Quantity) AS SumQuantity,
        AVG(tha.ActualCost) AS AvgActualCost
FROM    Production.TransactionHistoryArchive AS tha
GROUP BY tha.ProductID;



CREATE NONCLUSTERED COLUMNSTORE INDEX ix_csTest
ON Production.TransactionHistoryArchive
(ProductID,
Quantity,
ActualCost);


SELECT  *
INTO    dbo.TransactionHistoryArchive
FROM    Production.TransactionHistoryArchive;

CREATE CLUSTERED INDEX ClusteredColumnStoreTest
ON dbo.TransactionHistoryArchive
(TransactionID);




SELECT  tha.ProductID,
        COUNT(tha.ProductID) AS CountProductID,
        SUM(tha.Quantity) AS SumQuantity,
        AVG(tha.ActualCost) AS AvgActualCost
FROM    dbo.TransactionHistoryArchive AS tha
GROUP BY tha.ProductID;



CREATE CLUSTERED COLUMNSTORE INDEX ClusteredColumnStoreTest
ON dbo.TransactionHistoryArchive
WITH (DROP_EXISTING = ON);




SELECT  tha.ProductID,
        COUNT(tha.ProductID) AS CountProductID,
        SUM(tha.Quantity) AS SumQuantity,
        AVG(tha.ActualCost) AS AvgActualCost
FROM    dbo.TransactionHistoryArchive AS tha
GROUP BY tha.ProductID;

DROP TABLE dbo.TransactionHistoryArchive;
DROP INDEX Production.TransactionHistoryArchive.ix_csTest;



CREATE INDEX IX_Test 
ON Person.Address(City);


sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
GO
EXEC sp_configure 
    'max degree of parallelism';


--Chapter 10

SELECT  soh.DueDate,
        soh.CustomerID,
		soh.Status      
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.DueDate BETWEEN '1/1/2008' AND '2/1/2008';




CREATE PROCEDURE dbo.uspProductSize
AS
SELECT  p.ProductID, 
		p.Size
FROM	Production.Product AS p
WHERE	p.Size = '62';



CREATE NONCLUSTERED INDEX [_dta_index_SalesOrderHeader_5_1266103551__K4_6_11] ON [Sales].[SalesOrderHeader]
(
[DueDate] ASC
)
INCLUDE ( 	[Status],
[CustomerID]) WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]



--Chapter 11

SELECT  p.[Name],
        AVG(sod.LineTotal)
FROM    Sales.SalesOrderDetail AS sod
        JOIN Production.Product p
        ON sod.ProductID = p.ProductID
WHERE   sod.ProductID = 776
GROUP BY sod.CarrierTrackingNumber,
        p.[Name]
HAVING  MAX(sod.OrderQty) > 1
ORDER BY MIN(sod.LineTotal);





SELECT  *
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.ProductID = 776 ;




SELECT  *
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.ProductID = 793 ;




SELECT  *
FROM    Sales.SalesOrderDetail AS sod WITH (INDEX (IX_SalesOrderDetail_ProductID))
WHERE   sod.ProductID = 793 ;






SELECT  NationalIDNumber,
        JobTitle,
        HireDate
FROM    HumanResources.Employee AS e
WHERE   e.NationalIDNumber = '693168613';




CREATE UNIQUE NONCLUSTERED INDEX [AK_Employee_NationalIDNumber] ON
[HumanResources].[Employee]
(NationalIDNumber ASC,  
JobTitle ASC,  
HireDate ASC )
WITH DROP_EXISTING;



CREATE UNIQUE NONCLUSTERED INDEX [AK_Employee_NationalIDNumber]
ON [HumanResources].[Employee]
(NationalIDNumber ASC )  
INCLUDE  (JobTitle,HireDate) 
WITH DROP_EXISTING ;




SELECT  NationalIDNumber,
        BusinessEntityID
FROM    HumanResources.Employee AS e
WHERE   e.NationalIDNumber BETWEEN '693168613'
                           AND     '7000000000';




CREATE UNIQUE NONCLUSTERED INDEX [AK_Employee_NationalIDNumber]
ON [HumanResources].[Employee]
(
[NationalIDNumber] ASC 
)WITH DROP_EXISTING ;



DBCC SHOW_STATISTICS('HumanResources.Employee', 
AK_Employee_NationalIDNumber) ;






SELECT  poh.PurchaseOrderID,
        poh.VendorID,
        poh.OrderDate
FROM    Purchasing.PurchaseOrderHeader AS poh
WHERE   VendorID = 1636
        AND poh.OrderDate = '12/5/2007' ;



CREATE NONCLUSTERED INDEX Ix_TEST 
ON Purchasing.PurchaseOrderHeader(OrderDate) ;



--Chapter 12

DBCC TRACEON(2371,-1);






IF (SELECT  OBJECT_ID('Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1; 
GO
CREATE TABLE dbo.Test1 (C1 INT, C2 INT IDENTITY);

SELECT TOP 1500
        IDENTITY( INT,1,1 ) AS n
INTO    #Nums
FROM    Master.dbo.SysColumns sC1,
        Master.dbo.SysColumns sC2;
        
INSERT  INTO dbo.Test1
        (C1)
        SELECT  n
        FROM    #Nums;
        
DROP TABLE #Nums;

CREATE NONCLUSTERED INDEX i1 ON dbo.Test1 (C1);




SELECT  *
FROM    dbo.Test1
WHERE   C1 = 2;



CREATE EVENT SESSION [Statistics] ON SERVER 
ADD EVENT sqlserver.auto_stats(
    ACTION(sqlserver.sql_text)),
ADD EVENT sqlserver.missing_column_statistics(SET collect_column_list=(1)
    ACTION(sqlserver.sql_text)
    WHERE ([sqlserver].[database_name]=N'AdventureWorks2012'))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=ON,STARTUP_STATE=OFF)
GO





SELECT TOP 1500
        IDENTITY( INT,1,1 ) AS n
INTO    #Nums
FROM    Master.dbo.SysColumns scl,
        Master.dbo.SysColumns sC2 ;
INSERT  INTO dbo.Test1
        (C1)
        SELECT  2
        FROM    #Nums ;
DROP TABLE #Nums ;







IF (SELECT  OBJECT_ID('Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1; 
GO
CREATE TABLE dbo.Test1 (C1 INT, C2 INT IDENTITY);

SELECT TOP 1500
        IDENTITY( INT,1,1 ) AS n
INTO    #Nums
FROM    Master.dbo.SysColumns sC1,
        Master.dbo.SysColumns sC2;
        
INSERT  INTO dbo.Test1
        (C1)
        SELECT  n
        FROM    #Nums;
        
DROP TABLE #Nums;

CREATE NONCLUSTERED INDEX i1 ON dbo.Test1 (C1);
GO
ALTER DATABASE AdventureWorks2012 SET AUTO_UPDATE_STATISTICS OFF;
GO
SELECT TOP 1500
        IDENTITY( INT,1,1 ) AS n
INTO    #Nums
FROM    Master.dbo.SysColumns scl,
        Master.dbo.SysColumns sC2 ;
INSERT  INTO dbo.Test1
        (C1)
        SELECT  2
        FROM    #Nums ;
DROP TABLE #Nums ;
GO

SELECT  *
FROM    dbo.Test1
WHERE   C1 = 2;


ALTER DATABASE AdventureWorks2012 SET AUTO_UPDATE_STATISTICS ON;
GO







IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1; 
GO

CREATE TABLE dbo.Test1
    (Test1_C1 INT IDENTITY,
     Test1_C2 INT
    );
    
INSERT  INTO dbo.Test1
        (Test1_C2)
VALUES  (1);

SELECT TOP 10000
        IDENTITY( INT,1,1 ) AS n
INTO    #Nums
FROM    Master.dbo.SysColumns scl,
        Master.dbo.SysColumns sC2 ;
        
INSERT  INTO dbo.Test1
        (Test1_C2)
        SELECT  2
        FROM    #Nums 
GO 

CREATE CLUSTERED INDEX i1 ON dbo.Test1(Test1_C1)

--Create second table with 10001 rows, -- but opposite data distribution IF(SELECT 0BJECT_ID('dbo.Test2')) IS NOT NULL
IF (SELECT  OBJECT_ID('dbo.Test2')
   ) IS NOT NULL 
    DROP TABLE dbo.Test2; 
GO

CREATE TABLE dbo.Test2
    (Test2_C1 INT IDENTITY,
     Test2_C2 INT
    );
    
INSERT  INTO dbo.Test2
        (Test2_C2)
VALUES  (2);

INSERT  INTO dbo.Test2
        (Test2_C2)
        SELECT  1
        FROM    #Nums;
DROP TABLE #Nums; 
GO 

CREATE CLUSTERED INDEX il ON dbo.Test2(Test2_C1);




SELECT  DATABASEPROPERTYEX('AdventureWorks2012',
'IsAutoCreateStatistics');






SELECT  t1.Test1_C2,
        t2.Test2_C2
FROM    dbo.Test1 AS t1
        JOIN dbo.Test2 AS t2
        ON t1.Test1_C2 = t2.Test2_C2
WHERE   t1.Test1_C2 = 2;





SELECT  dest.text AS query,
        deqs.execution_count,
        deqp.query_plan
FROM    sys.dm_exec_query_stats AS deqs
        CROSS APPLY sys.dm_exec_text_query_plan(deqs.plan_handle,
                                                deqs.statement_start_offset,
                                                deqs.statement_end_offset) AS detqp
        CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS deqp
        CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest
WHERE   detqp.query_plan LIKE '%ColumnsWithNoStatistics%';












SELECT  s.name,
		s.auto_created,
		s.user_created
FROM    sys.stats AS s
WHERE   object_id = OBJECT_ID('Test1');






SELECT  t1.Test1_C2,
        t2.Test2_C2
FROM    dbo.Test1 AS t1
JOIN    dbo.Test2 AS t2
        ON t1.Test1_C2 = t2.Test2_C2
WHERE   t1.Test1_C2 = 1;





SELECT  c.name,
        sc.object_id,
        sc.stats_column_id,
        sc.stats_id
FROM    sys.stats_columns AS sc
        JOIN sys.columns AS c
        ON c.object_id = sc.object_id
           AND c.column_id = sc.column_id
WHERE   sc.object_id = OBJECT_ID('Test1');




DROP STATISTICS [Test1].StatisticsName;



ALTER DATABASE AdventureWorks2012 SET AUTO_CREATE_STATISTICS OFF;

--comment
SELECT  Test1.Test1_C2,
        Test2.Test2_C2
FROM    dbo.Test1
        JOIN dbo.Test2
        ON Test1.Test1_C2 = Test2.Test2_C2
WHERE   Test1.Test1_C2 = 2;


--second query
SELECT  Test1.Test1_C2,
        Test2.Test2_C2
FROM    dbo.Test2
        JOIN dbo.Test1
        ON Test1.Test1_C2 = Test2.Test2_C2
WHERE   Test1.Test1_C2 = 2;


DBCC FREEPROCCACHE()



ALTER DATABASE AdventureWorks2012 SET AUTO_CREATE_STATISTICS ON;







IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1; 
GO

CREATE TABLE dbo.Test1 (C1 INT,C2 INT IDENTITY);

INSERT  INTO dbo.Test1
        (C1)
VALUES  (1);

SELECT TOP 10000
        IDENTITY( INT,1,1 ) AS n
INTO    #Nums
FROM    Master.dbo.SysColumns sc1,
        Master.dbo.SysColumns sc2;
        
INSERT  INTO dbo.Test1
        (C1)
        SELECT  2
        FROM    #Nums;
        
DROP TABLE #Nums;
      
CREATE NONCLUSTERED INDEX FirstIndex ON dbo.Test1 (C1);



DBCC SHOW_STATISTICS(Test1, FirstIndex);






--Retrieve 1 row; 
SELECT  *
FROM    dbo.Test1
WHERE   C1 = 1;

--Retrieve 10000 rows; 
SELECT  *
FROM    dbo.Test1
WHERE   C1 = 2;








SELECT  1.0 / COUNT(DISTINCT C1)
FROM    dbo.Test1;








SELECT  p.Name,
        p.Class
FROM    Production.Product AS p
WHERE   p.Color = 'Red' AND
        p.DaysToManufacture > 15;




SELECT  s.name,
        s.auto_created,
        s.user_created,
        s.filter_definition,
        sc.column_id,
        c.name AS ColumnName
FROM    sys.stats AS s
        JOIN sys.stats_columns AS sc ON sc.stats_id = s.stats_id
AND sc.object_id = s.object_id
        JOIN sys.columns AS c ON c.column_id = sc.column_id
AND c.object_id = s.object_id
WHERE   s.object_id = OBJECT_ID('Production.Product');











CREATE NONCLUSTERED INDEX FirstIndex 
ON dbo.Test1(C1,C2) WITH DROP_EXISTING = ON;



SELECT  1.0 / COUNT(*)
FROM    (SELECT DISTINCT
                C1,
                C2
         FROM   dbo.Test1
        ) DistinctRows ;




CREATE INDEX IX_Test 
ON Sales.SalesOrderHeader (PurchaseOrderNumber)
WITH DROP_EXISTING = ON;





DBCC SHOW_STATISTICS('Sales.SalesOrderHeader',IX_Test);



CREATE INDEX IX_Test 
ON Sales.SalesOrderHeader (PurchaseOrderNumber) 
WHERE PurchaseOrderNumber IS NOT NULL 
WITH DROP_EXISTING = ON;




DROP INDEX Sales.SalesOrderHeader.IX_Test;


DBCC FREEPROCCACHE()








SELECT  p.Name,
        p.Class
FROM    Production.Product AS p
WHERE   p.Color = 'Red' AND
        p.DaysToManufacture > 15
		OPTION(QUERYTRACEON 9481);









SELECT  so.Description,
        p.Name AS ProductName,
        p.ListPrice,
        p.Size,
        pv.AverageLeadTime,
        pv.MaxOrderQty,
        v.Name AS VendorName
FROM    Sales.SpecialOffer AS so
JOIN    Sales.SpecialOfferProduct AS sop
ON      sop.SpecialOfferID = so.SpecialOfferID
JOIN    Production.Product AS p
ON      p.ProductID = sop.ProductID
JOIN    Purchasing.ProductVendor AS pv
ON      pv.ProductID = p.ProductID
JOIN    Purchasing.Vendor AS v
ON      v.BusinessEntityID = pv.BusinessEntityID
WHERE so.DiscountPct > .15;





SELECT  cc.CardNumber,
        cc.ExpMonth,
        cc.ExpYear
FROM    Sales.CreditCard AS cc
WHERE   cc.CardType = 'Vista';




IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1; 

CREATE TABLE dbo.Test1 (C1 INT); 

CREATE INDEX ixl ON dbo.Test1(C1); 

INSERT  INTO dbo.Test1
        (C1)
VALUES  (0);



SELECT  C1
FROM    dbo.Test1
WHERE   C1 = 0;



ALTER DATABASE AdventureWorks2012 SET AUTO_CREATE_STATISTICS OFF;


ALTER DATABASE AdventureWorks2012 SET AUTO_UPDATE_STATISTICS OFF;



ALTER DATABASE AdventureWorks2012 SET AUTO_UPDATE_STATISTICS_ASYNC ON;




USE AdventureWorks2012;
EXEC sp_autostats 
    'HumanResources.Department',
    'OFF';




EXEC sp_autostats 
    'HumanResources.Department',
    'OFF',
    AK_Department_Name;



EXEC sp_autostats 'Production.Product';



EXEC sp_autostats 
    'HumanResources.Department',
    'ON';
EXEC sp_autostats 
    'HumanResources.Department',
    'ON',
    AK_Department_Name;




SELECT  is_auto_create_stats_on
FROM    sys.databases
WHERE   [name] = 'AdventureWorks2012';




USE AdventureWorks2012;
EXEC sys.sp_autostats 
    'HumanResources.Department';




SELECT  DATABASEPROPERTYEX('AdventureWorks2012','IsAutoUpdateStatistics');





USE AdventureWorks2012;
EXEC sp_autostats 
    'Sales.SalesOrderDetail';







ALTER DATABASE AdventureWorks2012 SET AUTO_CREATE_STATISTICS OFF;
ALTER DATABASE AdventureWorks2012 SET AUTO_UPDATE_STATISTICS OFF;



IF EXISTS ( SELECT  *
            FROM    sys.objects
            WHERE   object_id = OBJECT_ID(N'dbo.Test1') )
    DROP TABLE  [dbo].[Test1];
		GO
	
CREATE TABLE dbo.Test1 (C1 INT,C2 INT,C3 CHAR(50));
INSERT  INTO dbo.Test1
        (C1,C2,C3)
VALUES  (51,1,'C3') ,
        (52,1,'C3');
        
CREATE NONCLUSTERED INDEX iFirstIndex ON dbo.Test1 (C1, C2);

SELECT TOP 10000
        IDENTITY( INT,1,1 ) AS n
INTO    #Nums
FROM    Master.dbo.SysColumns scl,
        Master.dbo.SysColumns sC2;
        
INSERT  INTO dbo.Test1
        (C1,C2,C3)
        SELECT  n % 50,
                n,
                'C3'
        FROM    #Nums;
DROP TABLE #Nums;




SELECT  *
FROM    dbo.Test1
WHERE   C2 = 1;


CREATE STATISTICS Stats1 ON Test1(C2);



DBCC FREEPROCCACHE();



SELECT  *
FROM    dbo.Test1
WHERE   C2 = 1;


DBCC SHOW_STATISTICS(Test1, iFirstIndex);




SELECT  *
FROM    dbo.Test1
WHERE   C1 = 51;



UPDATE STATISTICS Test1 iFirstIndex WITH FULLSCAN;


ALTER DATABASE AdventureWorks2012 SET AUTO_CREATE_STATISTICS ON; 
ALTER DATABASE AdventureWorks2012 SET AUTO_UPDATE_STATISTICS ON;



EXEC sys.sp_MSforeachtable 
    'UPDATE STATISTICS ? ALL WITH FULLSCAN;'

--Chapter 13


IF (SELECT  OBJECT_ID('Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1;
GO
CREATE TABLE dbo.Test1 (
C1 INT,
C2 CHAR(999),
C3 VARCHAR(10))
INSERT  INTO dbo.Test1
VALUES  (100,'C2',''),
        (200,'C2',''),
        (300,'C2',''),
        (400,'C2',''),
        (500,'C2',''),
        (600,'C2',''),
        (700,'C2',''),
        (800,'C2','');

CREATE CLUSTERED INDEX iClust 
ON dbo.Test1(C1);



SELECT  ddips.avg_fragmentation_in_percent,
        ddips.fragment_count,
        ddips.page_count,
        ddips.avg_page_space_used_in_percent,
        ddips.record_count,
        ddips.avg_record_size_in_bytes
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2012'),
                                       OBJECT_ID(N'dbo.Test1'), NULL,
NULL,'Sampled') AS ddips;



UPDATE  dbo.Test1
SET     C3 = 'Add data'
WHERE   C1 = 200;





DBCC IND(AdventureWorks2012,'dbo.Test1',-1)





INSERT  INTO dbo.Test1
VALUES  (410, 'C4', ''),
        (900, 'C4', '');




DBCC TRACEON(3604);
DBCC PAGE('AdventureWorks2012',1,24259,3);







INSERT  INTO Test1
VALUES  (110, 'C2', '');





INSERT  INTO dbo.Test1
VALUES  (410, 'C4', ''),
        (900, 'C4', '');





IF (SELECT  OBJECT_ID('Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1;
GO
CREATE TABLE dbo.Test1
    (
     C1 INT,
     C2 INT,
     C3 INT,
     c4 CHAR(2000)
    );
    
CREATE CLUSTERED INDEX i1 ON dbo.Test1 (C1);

WITH    Nums
          AS (SELECT TOP (10000)
                        ROW_NUMBER() OVER (ORDER BY (SELECT 1
                                                    )) AS n
              FROM      master.sys.All_Columns ac1
                        CROSS JOIN master.sys.All_Columns ac2
             )
    INSERT  INTO dbo.Test1
            (C1, C2, C3, c4)
            SELECT  n,
                    n,
                    n,
                    'a'
            FROM    Nums;
            
WITH    Nums
          AS (SELECT    1 AS n
              UNION ALL
              SELECT    n + 1
              FROM      Nums
              WHERE     n < 100
             )
    INSERT  INTO dbo.Test1
            (C1, C2, C3, c4)
            SELECT  41 - n,
                    n,
                    n,
                    'a'
            FROM    Nums;



--Reads 6 rows
SELECT  *
FROM    dbo.Test1
WHERE   C1 BETWEEN 21 AND 23 ; 

--Reads all rows
SELECT  *	
FROM    dbo.Test1
WHERE   C1 BETWEEN 1 AND 10000 ;


ALTER INDEX i1 ON dbo.Test1 REBUILD;


DBCC FREEPROCCACHE()


SELECT  *
FROM    dbo.Test1
WHERE   C1 = 1;




SELECT  ddips.avg_fragmentation_in_percent,
        ddips.fragment_count,
        ddips.page_count,
        ddips.avg_page_space_used_in_percent,
        ddips.record_count,
        ddips.avg_record_size_in_bytes
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2012'),
                                       OBJECT_ID(N'dbo.Test1'),NULL,
									   NULL,'Sampled') AS ddips;





SELECT  ddips.*
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2012'),
                                       OBJECT_ID(N'dbo.Test1'),NULL,
									   NULL,'Detailed') AS ddips;






IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1;
GO

CREATE TABLE dbo.Test1 (
C1 INT,
C2 INT,
C3 INT,
C4 CHAR(2000));
    
DECLARE @n INT = 1;

WHILE @n <= 28
    BEGIN
        INSERT  INTO dbo.Test1
        VALUES  (@n,@n,@n,'a');
        SET @n = @n + 1;
    END
    
CREATE CLUSTERED INDEX FirstIndex ON dbo.Test1(C1);



CREATE UNIQUE CLUSTERED INDEX FirstIndex 
ON dbo.Test1(C1) 
WITH (DROP_EXISTING = ON);





CREATE CLUSTERED INDEX FirstIndex 
ON dbo.Test1(C1) 
WITH (DROP_EXISTING = ON);








IF (SELECT  OBJECT_ID('Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ;
GO
CREATE TABLE dbo.Test1
    (C1 INT,
     C2 INT,
     C3 INT,
     c4 CHAR(2000)
    ) ;
    
CREATE CLUSTERED INDEX i1 ON dbo.Test1 (C1) ;

WITH    Nums
          AS (SELECT    1 AS n
              UNION ALL
              SELECT    n + 1
              FROM      Nums
              WHERE     n < 21
             )
    INSERT  INTO dbo.Test1
            (C1, C2, C3, c4)
            SELECT  n,
                    n,
                    n,
                    'a'
            FROM    Nums ;
            
WITH    Nums
          AS (SELECT    1 AS n
              UNION ALL
              SELECT    n + 1
              FROM      Nums
              WHERE     n < 21
             )
    INSERT  INTO dbo.Test1
            (C1, C2, C3, c4)
            SELECT  41 - n,
                    n,
                    n,
                    'a'
            FROM    Nums;




SELECT  ddips.avg_fragmentation_in_percent,
        ddips.fragment_count,
        ddips.page_count,
        ddips.avg_page_space_used_in_percent,
        ddips.record_count,
        ddips.avg_record_size_in_bytes
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2012'),
                                       OBJECT_ID(N'dbo.Test1'),NULL,
									   NULL,'Sampled') AS ddips;



ALTER INDEX i1 ON dbo.Test1 REBUILD;



ALTER INDEX ALL ON dbo.Test1 REBUILD;

ALTER INDEX i1 ON dbo.Test1 REORGANIZE;



ALTER INDEX i1 ON dbo.Test1 
REBUILD PARTITION = ALL
WITH (ONLINE = ON);






IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1; 
GO
CREATE TABLE dbo.Test1 (C1 INT,C2 CHAR(999));

WITH    Nums
          AS (SELECT    1 AS n
              UNION ALL
              SELECT    n + 1
              FROM      Nums
              WHERE     n < 24
             )
    INSERT  INTO dbo.Test1
            (C1,C2)
            SELECT  n * 100,
                    'a'
            FROM    Nums;



CREATE CLUSTERED INDEX FillIndex ON Test1(C1);

SELECT  ddips.avg_fragmentation_in_percent,
        ddips.fragment_count,
        ddips.page_count,
        ddips.avg_page_space_used_in_percent,
        ddips.record_count,
        ddips.avg_record_size_in_bytes
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2012'),
                                       OBJECT_ID(N'dbo.Test1'),NULL,
									   NULL,'Sampled') AS ddips;


ALTER INDEX FillIndex ON dbo.Test1 REBUILD 
WITH  (FILLFACTOR= 75);


INSERT  INTO dbo.Test1
VALUES  (110, 'a'),  --25th row 	
        (120, 'a') ;  --26th row


INSERT  INTO dbo.Test1
VALUES  (130, 'a') ;  --27th row




CREATE PROCEDURE IndexDefrag
AS
DECLARE @DBName NVARCHAR(255),
    @TableName NVARCHAR(255),
    @SchemaName NVARCHAR(255),
    @IndexName NVARCHAR(255),
    @PctFrag DECIMAL,
    @Defrag NVARCHAR(MAX)
    
IF EXISTS ( SELECT  *
            FROM    sys.objects
            WHERE   object_id = OBJECT_ID(N'#Frag') )
    DROP TABLE #Frag;
CREATE TABLE #Frag (
DBName NVARCHAR(255),
TableName NVARCHAR(255),
SchemaName NVARCHAR(255),
IndexName NVARCHAR(255),
AvgFragment DECIMAL)
EXEC sys.sp_MSforeachdb 'INSERT INTO #Frag ( DBName, TableName, SchemaName, IndexName, AvgFragment )  SELECT    ''?''  AS DBName ,t.Name AS TableName ,sc.Name AS SchemaName ,i.name AS IndexName ,s.avg_fragmentation_in_percent FROM        ?.sys.dm_db_index_physical_stats(DB_ID(''?''),  NULL,  NULL,
NULL,   ''Sampled'') AS s JOIN ?.sys.indexes i ON s.Object_Id = i.Object_id
AND s.Index_id = i.Index_id JOIN ?.sys.tables t ON i.Object_id = t.Object_Id JOIN ?.sys.schemas sc ON t.schema_id = sc.SCHEMA_ID

WHERE s.avg_fragmentation_in_percent > 20
AND t.TYPE = ''U''
AND s.page_count > 8
ORDER BY TableName,IndexName';

DECLARE cList CURSOR
FOR
SELECT  *
FROM    #Frag

OPEN cList;
FETCH NEXT FROM cList
INTO @DBName,@TableName,@SchemaName,@IndexName,@PctFrag;

WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @PctFrag BETWEEN 20.0 AND 40.0
            BEGIN
                SET @Defrag = N'ALTER INDEX ' + @IndexName + ' ON ' + @DBName
                    + '.' + @SchemaName + '.' + @TableName + ' REORGANIZE';
                EXEC sp_executesql @Defrag;
                PRINT 'Reorganize index: ' + @DBName + '.' + @SchemaName + '.'
                    + @TableName + '.' + @IndexName;
            END
        ELSE
            IF @PctFrag > 40.0
                BEGIN
                    SET @Defrag = N'ALTER INDEX ' + @IndexName + ' ON '
                        + @DBName + '.' + @SchemaName + '.' + @TableName
                        + ' REBUILD';
                    EXEC sp_executesql @Defrag;
                    PRINT 'Rebuild index: ' + @DBName + '.' + @SchemaName
                        + '.' + @TableName + '.' + @IndexName;
                END
        FETCH NEXT FROM cList
INTO @DBName,@TableName,@SchemaName,@IndexName,@PctFrag;
    END
CLOSE cList;
DEALLOCATE cList;
DROP TABLE #Frag;
GO



EXEC IndexDefrag;





--Chapter 14


CREATE TABLE dbo.Test1 (c1 INT);
INSERT  INTO dbo.Test1
VALUES  (1);
CEILEKT * FROM dbo.t1; --Error:  I meant,  SELECT * FROM t1





IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1; 
GO
CREATE TABLE dbo.Test1 (c1 INT);
INSERT  INTO dbo.Test1
VALUES  (1);
SELECT  'Before Error',
        c1
FROM    dbo.Test1 AS t; 
SELECT  'error',
        c1
FROM    dbo.no_Test1;
  --Error:  Table doesn't exist 
SELECT  'after error' c1
FROM    dbo.Test1 AS t;




SELECT  soh.AccountNumber,
        soh.OrderDate,
        soh.PurchaseOrderNumber,
        soh.SalesOrderNumber
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.SalesOrderID BETWEEN 62500 AND 62550;


SELECT *
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.SalesOrderID BETWEEN 62500 AND 62550;




SELECT  soh.SalesOrderNumber,
        sod.OrderQty,
        sod.LineTotal,
        sod.UnitPrice,
        sod.UnitPriceDiscount,
        p.[Name] AS ProductName,
        p.ProductNumber,
        ps.[Name] AS ProductSubCategoryName,
        pc.[Name] AS ProductCategoryName
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
JOIN    Production.Product AS p
        ON sod.ProductID = p.ProductID
JOIN    Production.ProductModel AS pm
        ON p.ProductModelID = pm.ProductModelID
JOIN    Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
JOIN    Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE   soh.CustomerID = 29658;





SELECT  deqoi.counter,
        deqoi.occurrence,
        deqoi.value
FROM    sys.dm_exec_query_optimizer_info AS deqoi;






USE master;
EXEC sp_configure 'show advanced option','1';
RECONFIGURE;
EXEC sp_configure 'affinity mask',15;
 --Bit map: 00001111
RECONFIGURE;


USE master;
EXEC sp_configure 'show advanced option','1';
RECONFIGURE;
EXEC sp_configure 'max degree of parallelism',2;
RECONFIGURE;




SELECT  *
FROM    dbo.t1
WHERE   C1 = 1
OPTION  (MAXDOP 2);





USE master;
EXEC sp_configure 'show advanced option','1';
RECONFIGURE;
EXEC sp_configure 'cost threshold for parallelism',35;
RECONFIGURE;



--Chapter 15

SELECT  *
FROM    sys.dm_exec_cached_plans;




SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID = 29690
        AND sod.ProductID = 711;


SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderlD = sod.SalesOrderlD
WHERE   soh.CustomerlD = 29690
        AND sod.productid = 711;




IF (SELECT  OBJECT_ID('dbo.BasicSalesInfo')
   ) IS NOT NULL
    DROP PROC dbo.BasicSalesInfo;
GO
CREATE PROC dbo.BasicSalesInfo
    @ProductID INT,
    @CustomerID INT
AS
SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID = @CustomerID
        AND sod.ProductID = @ProductID;

DBCC FREEPROCCACHE



SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID = 29690
        AND sod.ProductID = 711;



SELECT    c.usecounts
	,c.cacheobjtype 
	,c.objtype 
FROM       sys.dm_exec_cached_plans c
	CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t 
WHERE      t.text =  'SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID = 29690
        AND sod.ProductID = 711;';


SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID = 29500
        AND sod.ProductID = 711;



SELECT  c.usecounts,
        c.cacheobjtype,
        c.objtype,
        t.text,
        c.size_in_bytes
FROM    sys.dm_exec_cached_plans c
CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t
WHERE   t.text LIKE 'SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID%';





sp_configure 
    'optimize for ad hoc workloads',
    1;
GO
RECONFIGURE;





sp_configure 
    'optimize for ad hoc workloads',
    0;
GO
RECONFIGURE;


DBCC FREEPROCCACHE


SELECT  a.*
FROM    Person.Address AS a
WHERE   a.AddressID = 42;


SELECT    a.*
FROM       Person.Address AS a
WHERE      a.[AddressID] = 52; --previous value was 42

SELECT  c.usecounts,
        c.cacheobjtype,
        c.objtype,
        t.text
FROM    sys.dm_exec_cached_plans c
CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t


SELECT  a.*
FROM    Person.Address AS a
WHERE   a.AddressID BETWEEN 40 AND 60;


SELECT  a.*
FROM    Person.Address AS a
WHERE   a.AddressID >= 40
        AND a.AddressID <= 60;



ALTER DATABASE AdventureWorks2012 SET PARAMETERIZATION SIMPLE;

ALTER DATABASE AdventureWorks2012 SET PARAMETERIZATION FORCED;

DBCC FREEPROCCACHE


SELECT ea.EmailAddress,
    e.BirthDate,
    a.City
FROM   Person.Person AS p
JOIN   HumanResources.Employee AS e
    ON p.BusinessEntityID = e.BusinessEntityID
JOIN   Person.BusinessEntityAddress AS bea
    ON e.BusinessEntityID = bea.BusinessEntityID
JOIN   Person.Address AS a
    ON bea.AddressID = a.AddressID
JOIN   Person.StateProvince AS sp
    ON a.StateProvinceID = sp.StateProvinceID
JOIN   Person.EmailAddress AS ea
    ON p.BusinessEntityID = ea.BusinessEntityID
WHERE  ea.EmailAddress LIKE 'david%'
    AND sp.StateProvinceCode = 'WA';



(@0 varchar(8000))
SELECT  ea.EmailAddress,
        e.BirthDate,
        a.City
FROM    Person.Person AS p
JOIN    HumanResources.Employee AS e
        ON p.BusinessEntityID = e.BusinessEntityID
JOIN    Person.BusinessEntityAddress AS bea
        ON e.BusinessEntityID = bea.BusinessEntityID
JOIN    Person.Address AS a
        ON bea.AddressID = a.AddressID
JOIN    Person.StateProvince AS sp
        ON a.StateProvinceID = sp.StateProvinceID
JOIN    Person.EmailAddress AS ea
        ON p.BusinessEntityID = ea.BusinessEntityID
WHERE   ea.EmailAddress LIKE 'david%'
        AND sp.StateProvinceCode = @0



ALTER DATABASE AdventureWorks2012 SET PARAMETERIZATION SIMPLE;




IF (SELECT  OBJECT_ID('BasicSalesInfo')
   ) IS NOT NULL 
    DROP PROC dbo.BasicSalesInfo ;
GO
CREATE PROC dbo.BasicSalesInfo
    @ProductID INT,
    @CustomerID INT
AS 
SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID = @CustomerID
        AND sod.ProductID = @ProductID;



DBCC FREEPROCCACHE


EXEC dbo.BasicSalesInfo 
    @CustomerID = 29690,
    @ProductID = 711;



EXEC dbo.BasicSalesInfo 
    @CustomerID = 29690,
    @ProductID = 777;





IF (SELECT  OBJECT_ID('dbo.MyNewProc')
   ) IS NOT NULL
    DROP PROCEDURE dbo.MyNewProc 
GO
CREATE PROCEDURE dbo.MyNewProc
AS
SELECT  MyID
FROM    dbo.NotHere; --Table no_tl doesn't exist




IF (SELECT  OBJECT_ID('dbo.RestrictedAccess')
   ) IS NOT NULL
    DROP TABLE dbo.RestrictedAccess;
GO
CREATE TABLE dbo.RestrictedAccess (ID INT,Status VARCHAR(7));
INSERT  INTO t1
VALUES  (1,'New');
GO 
IF (SELECT  OBJECT_ID('dbo.spuMarkDeleted')
   ) IS NOT NULL
    DROP PROCEDURE dbo.spuMarkDeleted;
GO
CREATE PROCEDURE dbo.spuMarkDeleted @ID INT
AS
UPDATE  dbo.RestrictedAccess
SET     Status = 'Deleted'
WHERE   ID = @ID;
GO

--Prevent user u1 from deleting rows 
DENY DELETE ON dbo.RestrictedAccess TO UserOne;

--Allow user u1 to mark a row as 'deleted' 
GRANT EXECUTE ON dbo.spuMarkDeleted TO UserOne;





CREATE PROCEDURE dbo.MarkDeleted @ID INT
AS
DECLARE @SQL NVARCHAR(MAX);

SET @SQL = 'UPDATE  dbo.RestrictedAccess
SET     Status = ''Deleted''
WHERE   ID = ' + @ID;

EXEC sys.sp_executesql @SQL;
GO

GRANT EXECUTE ON dbo.MarkDeleted TO UserOne;





DECLARE @query NVARCHAR(MAX),
    @paramlist NVARCHAR(MAX);

SET @query = N'SELECT    soh.SalesOrderNumber ,soh.OrderDate ,sod.OrderQty ,sod.LineTotal FROM       Sales.SalesOrderHeader AS soh
JOIN Sales.SalesOrderDetail AS sod ON soh.SalesOrderID = sod.SalesOrderID WHERE      soh.CustomerID = @CustomerID
AND sod.ProductID = @ProductID';

SET @paramlist = N'@CustomerID INT, @ProductID INT';

EXEC sp_executesql @query,@paramlist,@CustomerID = 29690,@ProductID = 711;


SELECT  c.usecounts,
        c.cacheobjtype,
        c.objtype,
        t.text
FROM    sys.dm_exec_cached_plans c
CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t 
WHERE text LIKE '(@CustomerID%';



DECLARE @query NVARCHAR(MAX),
    @paramlist NVARCHAR(MAX);

SET @query = N'SELECT    soh.SalesOrderNumber ,soh.OrderDate ,sod.OrderQty ,sod.LineTotal FROM       Sales.SalesOrderHeader AS soh
JOIN Sales.SalesOrderDetail AS sod ON soh.SalesOrderID = sod.SalesOrderID WHERE      soh.CustomerID = @CustomerID
AND sod.ProductID = @ProductID';

SET @paramlist = N'@CustomerID INT, @ProductID INT';

EXEC sp_executesql @query,@paramlist,@CustomerID = 29690,@ProductID = 777;




DECLARE @query NVARCHAR(MAX),
    @paramlist NVARCHAR(MAX);

SET @query = N'SELECT    soh.SalesOrderNumber ,soh.OrderDate ,sod.OrderQty ,sod.LineTotal FROM       Sales.SalesOrderHeader AS soh JOIN Sales.SalesOrderDetail AS sod ON soh.SalesOrderID = sod.SalesOrderID where      soh.CustomerID = @CustomerID AND sod.ProductID = @ProductID' ;

SET @paramlist = N'@CustomerID INT, @ProductID INT';

EXEC sp_executesql @query,@paramlist,@CustomerID = 29690,@ProductID = 777;


SELECT  decp.usecounts,
        decp.cacheobjtype,
        decp.objtype,
        dest.text,
        deqs.creation_time,
        deqs.execution_count,
	deqs.query_hash,
	deqs.query_plan_hash	      
FROM    sys.dm_exec_cached_plans AS decp
CROSS APPLY sys.dm_exec_sql_text(decp.plan_handle) AS dest
JOIN    sys.dm_exec_query_stats AS deqs
        ON decp.plan_handle = deqs.plan_handle
WHERE   dest.text LIKE '(@CustomerID INT, @ProductID INT)%' ;

DBCC FREEPROCCACHE

SELECT  *
FROM    Production.Product AS p
JOIN    Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
JOIN    Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE   pc.[Name] = 'Bikes'
        AND ps.[Name] = 'Touring Bikes';

SELECT  *
FROM    Production.Product AS p
JOIN    Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
JOIN    Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
where   pc.[Name] = 'Bikes'
        and ps.[Name] = 'Road Bikes';





SELECT  deqs.execution_count,
        deqs.query_hash,
        deqs.query_plan_hash,
        dest.text
FROM    sys.dm_exec_query_stats AS deqs
CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) dest
WHERE   dest.text LIKE 'SELECT  *
FROM    Production.Product AS p%';



SELECT  p.ProductID
FROM    Production.Product AS p
JOIN    Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
JOIN    Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE   pc.[Name] = 'Bikes'
        AND ps.[Name] = 'Touring Bikes';




SELECT  deqs.execution_count,
        deqs.query_hash,
        deqs.query_plan_hash,
        dest.text
FROM    sys.dm_exec_query_stats AS deqs
CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) dest
WHERE   dest.text LIKE 'SELECT  *
FROM    Production.Product AS p%'
OR dest.text LIKE 'SELECT  p.ProductID%';



DBCC FREEPROCCACHE()


SELECT  p.[Name],
        tha.TransactionDate,
        tha.TransactionType,
        tha.Quantity,
        tha.ActualCost
FROM    Production.TransactionHistoryArchive tha
JOIN    Production.Product p
        ON tha.ProductID = p.ProductID
WHERE   p.ProductID = 461 ;


SELECT  p.[Name],
        tha.TransactionDate,
        tha.TransactionType,
        tha.Quantity,
        tha.ActualCost
FROM    Production.TransactionHistoryArchive tha
JOIN    Production.Product p
        ON tha.ProductID = p.ProductID
WHERE   p.ProductID = 712 ;



SELECT  deqs.execution_count,
        deqs.query_hash,
        deqs.query_plan_hash,
        dest.text
FROM    sys.dm_exec_query_stats AS deqs
CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) dest
WHERE   dest.text LIKE '%SELECT  p.[Name],%'



DECLARE @n VARCHAR(3) = '776',
    @sql VARCHAR(MAX) ;

SET @sql = 'SELECT * FROM Sales.SalesOrderDetail sod  '
    + 'JOIN Sales.SalesOrderHeader soh  '
    + 'ON sod.SalesOrderID=soh.SalesOrderID ' + 'WHERE    sod.ProductID='''
    + @n + '''' ;

--Execute the dynamic query using EXECUTE statement 
EXECUTE  (@sql) ;



SELECT  deqs.execution_count,
        deqs.query_hash,
        deqs.query_plan_hash,
        dest.text,
		deqp.query_plan
FROM    sys.dm_exec_query_stats AS deqs
CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) dest
CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS deqp;



DECLARE @n NVARCHAR(3) = '776',
    @sql NVARCHAR(MAX),
    @paramdef NVARCHAR(6) ;
    
SET @sql = 'SELECT * FROM Sales.SalesOrderDetail sod  '
    + 'JOIN Sales.SalesOrderHeader soh  '
    + 'ON sod.SalesOrderID=soh.SalesOrderID ' + 'WHERE    sod.ProductID=@1' ;
SET @paramdef = N'@1 INT' ;

--Execute the dynamic query using sp_executesql system stored procedure 
EXECUTE sp_executesql 
    @sql,
    @paramdef,
    @1 = @n ;




GO

--Chapter 16
CREATE PROCEDURE dbo.ProductDetails 
(@ProductID INT)
AS
DECLARE @CurrentDate DATETIME = GETDATE();

SELECT  p.Name,
        p.Color,
        p.DaysToManufacture,
        pm.CatalogDescription
FROM    Production.Product AS p
JOIN    Production.ProductModel AS pm
        ON pm.ProductModelID = p.ProductModelID
WHERE   p.ProductID = @ProductID
        AND pm.ModifiedDate < @CurrentDate;
GO




IF (SELECT  OBJECT_ID('dbo.AddressByCity')
   ) IS NOT NULL
    DROP PROC dbo.AddressByCity 
GO
CREATE PROC dbo.AddressByCity @City NVARCHAR(30)
AS
SELECT  a.AddressID,
        a.AddressLine1,
        AddressLine2,
        a.City,
        sp.[Name] AS StateProvinceName,
        a.PostalCode
FROM    Person.Address AS a
JOIN    Person.StateProvince AS sp
        ON a.StateProvinceID = sp.StateProvinceID
WHERE   a.City = @City;
GO


EXEC dbo.AddressByCity @City = N'London';





DECLARE @City NVARCHAR(30) = N'London';

SELECT  a.AddressID,
        a.AddressLine1,
        AddressLine2,
        a.City,
        sp.[Name] AS StateProvinceName,
        a.PostalCode
FROM    Person.Address AS a
JOIN    Person.StateProvince AS sp
        ON a.StateProvinceID = sp.StateProvinceID
WHERE   a.City = @City;



EXEC dbo.AddressByCity @City = N'Mentor';




SELECT  dest.text,
        deqs.execution_count,
        deqs.creation_time
FROM    sys.dm_exec_query_stats AS deqs
CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest
WHERE   dest.text LIKE 'CREATE PROC dbo.AddressByCity%';



DBCC FREEPROCCACHE;

EXEC dbo.AddressByCity @City = N'Mentor';

EXEC dbo.AddressByCity @City = N'London'; -- nvarchar(30)




SELECT  deps.execution_count,
        deps.total_elapsed_time,
        deps.total_logical_reads,
        deps.total_logical_writes,
		deqp.query_plan
FROM    sys.dm_exec_procedure_stats AS deps
CROSS APPLY sys.dm_exec_query_plan(deps.plan_handle) AS deqp
WHERE   deps.object_id = OBJECT_ID('AdventureWorks2012.dbo.AddressByCity');



DBCC SHOW_STATISTICS('Person.Address','_WA_Sys_00000004_164452B1');



CREATE PROC dbo.AddressByCity @City NVARCHAR(30)
AS
-- This allows me to bypass parameter sniffing
DECLARE @LocalCity NVARCHAR(30) = @City;

SELECT  a.AddressID,
        a.AddressLine1,
        AddressLine2,
        a.City,
        sp.[Name] AS StateProvinceName,
        a.PostalCode
FROM    Person.Address AS a
JOIN    Person.StateProvince AS sp
        ON a.StateProvinceID = sp.StateProvinceID
WHERE   a.City = @LocalCity;
GO



ALTER PROC dbo.AddressByCity @City NVARCHAR(30)
AS
SELECT  a.AddressID,
        a.AddressLine1,
        AddressLine2,
        a.City,
        sp.[Name] AS StateProvinceName,
        a.PostalCode
FROM    Person.Address AS a
JOIN    Person.StateProvince AS sp
        ON a.StateProvinceID = sp.StateProvinceID
WHERE   a.City = @City
OPTION (OPTIMIZE FOR (@City = 'Mentor'));

exec dbo.addressbycity @city = 'London';



--Chapter 17
IF (SELECT  OBJECT_ID('dbo.WorkOrder')
   ) IS NOT NULL
    DROP PROCEDURE dbo.WorkOrder; 
GO
CREATE PROCEDURE dbo.WorkOrder
AS
SELECT  wo.WorkOrderID,
        wo.ProductID,
        wo.StockedQty
FROM    Production.WorkOrder AS wo
WHERE   wo.StockedQty BETWEEN 500 AND 700;


EXEC dbo.WorkOrder;



CREATE INDEX IX_Test ON Production.WorkOrder(StockedQty,ProductID);


DROP INDEX Production.WorkOrder.IX_Test;



IF (SELECT  OBJECT_ID('dbo.WorkOrderAll')
   ) IS NOT NULL
    DROP PROCEDURE dbo.WorkOrderAll; 
GO
CREATE PROCEDURE dbo.WorkOrderAll
AS
SELECT  *
FROM    Production.WorkOrder AS wo;

EXEC dbo.WorkOrderAll;
GO
CREATE INDEX IX_Test ON Production.WorkOrder(StockedQty,ProductID);
GO
EXEC dbo.WorkOrderAll; --After creation of index IX_Test


IF (SELECT  OBJECT_ID('dbo.p1')
   ) IS NOT NULL
    DROP PROC dbo.p1; 
GO
CREATE PROC dbo.p1
AS
CREATE TABLE #t1 (c1 INT);
INSERT  INTO #t1
        (c1)
VALUES  (42); -- data change causes recompile 
GO 

EXEC dbo.p1;




SELECT  DATABASEPROPERTYEX('AdventureWorks2012', 'IsAutoUpdateStatistics');




--step 1 set up
IF EXISTS ( SELECT  *
            FROM    sys.objects AS o
            WHERE   o.object_id = OBJECT_ID(N'dbo.NewOrderDetail')
                    AND o.type IN (N'U') )
    DROP TABLE dbo.NewOrderDetail; 
GO
SELECT  *
INTO    dbo.NewOrderDetail
FROM    Sales.SalesOrderDetail; 
GO
CREATE INDEX IX_NewOrders_ProductID ON dbo.NewOrderDetail (ProductID);
GO
IF EXISTS ( SELECT  *
            FROM    sys.objects
            WHERE   object_id = OBJECT_ID(N'dbo.NewOrders')
                    AND type IN (N'P',N'PC') )
    DROP PROCEDURE dbo.NewOrders; 
GO
CREATE PROCEDURE dbo.NewOrders
AS
SELECT  nod.OrderQty,
        nod.CarrierTrackingNumber
FROM    dbo.NewOrderDetail nod
WHERE   nod.ProductID = 897; 
GO
SET STATISTICS XML ON;
EXEC dbo.NewOrders;
SET STATISTICS XML OFF; 
GO


--step 2 modify the data and execute the query
UPDATE  dbo.NewOrderDetail
SET     ProductID = 897
WHERE   ProductID BETWEEN 800 AND 900;
GO
SET STATISTICS XML ON;
EXEC dbo.NewOrders;
SET STATISTICS XML OFF; 
GO





IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL
    DROP PROC dbo.TestProc; 
GO
CREATE PROC dbo.TestProc
AS
CREATE TABLE dbo.ProcTest1 (C1 INT);  --Ensure table doesn't exist 
SELECT  *
FROM    dbo.ProcTest1;   --Causes recompilation 
DROP TABLE dbo.ProcTest1; 
GO

EXEC dbo.TestProc;   --First execution 
EXEC dbo.TestProc;   --Second execution





IF (SELECT  OBJECT_ID('dbo.TestProc2')
   ) IS NOT NULL
    DROP PROC dbo.TestProc2; 
GO
CREATE PROC dbo.TestProc2
AS
CREATE TABLE #ProcTest2 (C1 INT);  --Ensure table doesn't exist 
SELECT  *
FROM    #ProcTest2;   --Causes recompilation 
DROP TABLE #ProcTest2; 
GO

EXEC dbo.TestProc2;   --First execution 
EXEC dbo.TestProc2;   --Second execution










IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL
    DROP PROC dbo.TestProc; 
GO
CREATE PROC dbo.TestProc
AS
SELECT  'a' + NULL + 'b'; --1st 
SET CONCAT_NULL_YIELDS_NULL OFF; 
SELECT  'a' + NULL + 'b'; --2nd 
SET ANSI_NULLS OFF; 
SELECT  'a' + NULL + 'b';
 --3rd 
GO
EXEC dbo.TestProc; --First execution 
EXEC dbo.TestProc; --Second execution




sp_recompile  'dbo.DatabaseLog';




DBCC FREEPROCCACHE;
 --Clear the procedure cache
GO
DECLARE @query NVARCHAR(MAX);
DECLARE @param NVARCHAR(MAX);
SET @query = N'SELECT    soh.SalesOrderNumber ,soh.OrderDate ,sod.OrderQty ,sod.LineTotal FROM       Sales.SalesOrderHeader AS soh
JOIN Sales.SalesOrderDetail AS sod ON soh.SalesOrderID = sod.SalesOrderID WHERE      soh.CustomerID >= @CustomerId;'
SET @param = N'@CustomerId INT';
EXEC sp_executesql @query,@param,@CustomerId = 1;
EXEC sp_executesql @query,@param,@CustomerId = 30118;







IF (SELECT  OBJECT_ID('dbo.CustomerList')
   ) IS NOT NULL
    DROP PROC dbo.CustomerList;
GO
CREATE PROCEDURE dbo.CustomerList @CustomerId INT
    WITH RECOMPILE
AS
SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= @CustomerId; 
GO



EXEC CustomerList 
    @CustomerId = 1;
EXEC CustomerList 
    @CustomerId = 30118;




IF (SELECT  OBJECT_ID('dbo.CustomerList')
   ) IS NOT NULL
    DROP PROC dbo.CustomerList;
GO
CREATE PROCEDURE dbo.CustomerList @CustomerId INT
AS
SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= @CustomerId
OPTION  (RECOMPILE); 
GO







IF (SELECT  OBJECT_ID('dbo.TempTable')
   ) IS NOT NULL
    DROP PROC dbo.TempTable 
GO
CREATE PROC dbo.TempTable
AS
CREATE TABLE #MyTempTable (ID INT,Dsc NVARCHAR(50))
INSERT  INTO #MyTempTable
        (ID,
         Dsc)
        SELECT  pm.ProductModelID,
                pm.[Name]
        FROM    Production.ProductModel AS pm;   --Needs 1st recompilation
SELECT  *
FROM    #MyTempTable AS mtt;
CREATE CLUSTERED INDEX iTest ON #MyTempTable (ID);
SELECT  *
FROM    #MyTempTable AS mtt; --Needs 2nd recompilation
CREATE TABLE #t2 (c1 INT);
SELECT  *
FROM    #t2;
 --Needs 3rd recompilation 
GO

EXEC dbo.TempTable; --First execution









--Create a small table with one row and an index 
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO
CREATE TABLE dbo.Test1 (C1 INT, C2 CHAR(50)) ;
INSERT  INTO dbo.Test1
VALUES  (1, '2') ;
CREATE NONCLUSTERED INDEX IndexOne ON dbo.Test1 (C1) ;

--Create a stored procedure referencing the previous table 
IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL 
    DROP PROC dbo.TestProc ; 
GO 
CREATE PROC dbo.TestProc
AS 
SELECT  *
FROM    dbo.Test1 AS t
WHERE   t.C1 = 1
OPTION  (KEEPFIXED PLAN) ;
GO

--First execution of stored procedure with 1 row in the table 
EXEC dbo.TestProc ;
 --First execution

--Add many rows to the table to cause statistics change 
WITH    Nums
          AS (SELECT    1 AS n
              UNION ALL
              SELECT    n + 1
              FROM      Nums
              WHERE     n < 1000
             )
    INSERT  INTO dbo.Test1
            (C1,
             C2
            )
            SELECT  1,
                    n
            FROM    Nums
    OPTION  (MAXRECURSION 1000) ; 
GO
--Reexecute the stored procedure with a change in statistics 
EXEC dbo.TestProc ; --With change in data distribution





IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL
    DROP PROC dbo.TestProc; 
GO
CREATE PROC dbo.TestProc
AS
DECLARE @TempTable TABLE (C1 INT); 
INSERT  INTO @TempTable
        (C1)
VALUES  (42);
 --Recompilation not needed
GO

EXEC dbo.TestProc; --First execution





DECLARE @t1 TABLE (c1 INT); 
INSERT  INTO @t1
VALUES  (1);
BEGIN TRAN
INSERT  INTO @t1
VALUES  (2);
ROLLBACK
SELECT  *
FROM    @t1; --Returns 2 rows




IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL
    DROP PROC dbo.TestProc 
GO
CREATE PROC dbo.TestProc
AS
SELECT  'a' + NULL + 'b'; --1st SET CONCAT_NULL_YIELDS_NULL OFF

SELECT  'a' + NULL + 'b'; --2nd
SET ANSI_NULLS OFF
SELECT  'a' + NULL + 'b';
 --3rd
GO

SET CONCAT_NULL_YIELDS_NULL OFF; 
SET ANSI_NULLS OFF;

EXEC dbo.TestProc;

SET CONCAT_NULL_YIELDS_NULL ON;
 --Reset to default
SET ANSI_NULLS ON;	--Reset to default







IF (SELECT  OBJECT_ID('dbo.CustomerList')
   ) IS NOT NULL
    DROP PROC dbo.CustomerList 
GO
CREATE PROCEDURE dbo.CustomerList @CustomerID INT
AS
SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= @CustomerID
OPTION  (OPTIMIZE FOR (@CustomerID = 1)); 
GO





EXEC dbo.CustomerList 
    @CustomerID = 7920 
    WITH RECOMPILE; 
EXEC dbo.CustomerList 
    @CustomerID = 30118 
    WITH RECOMPILE;





IF (SELECT  OBJECT_ID('dbo.CustomerList')
   ) IS NOT NULL
    DROP PROC dbo.CustomerList; 
GO 
IF (SELECT  OBJECT_ID('dbo. CustomerList')
   ) IS NOT NULL
    DROP PROC dbo. CustomerList; 
GO
CREATE PROCEDURE dbo.CustomerList @CustomerID INT
AS
SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= @CustomerId; 
GO


sp_create_plan_guide @name = N'MyGuide',
    @stmt = N'SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= @CustomerId;',@type = N'OBJECT',
    @module_or_batch = N'dbo.CustomerList',@params = NULL,
    @hints = N'OPTION (OPTIMIZE FOR (@CustomerId = 1))';



EXEC dbo.CustomerList 
    @CustomerID = 7920 
    WITH RECOMPILE; 
EXEC dbo.CustomerList 
    @CustomerID = 30118 
    WITH RECOMPILE;





SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= 1;





EXECUTE sp_create_plan_guide @name = N'MyBadSOLGuide',
    @stmt = N'SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
join Sales.SalesOrderDetail AS sod
ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= @Customerld',@type = N'SQL',@module_or_batch = NULL,
    @params = N'@CustomerID int',
    @hints = N'OPTION  (TABLE HINT(soh,  FORCESEEK))';



EXECUTE sp_control_plan_guide 
    @operation = 'Drop',
    @name = N'MyBadSQLGuide';



EXECUTE sp_create_plan_guide 
    @name = N'MyGoodSQLGuide',
    @stmt = N'SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= 1;',
    @type = N'SQL',
    @module_or_batch = NULL,
    @params = NULL,
    @hints = N'OPTION  (TABLE HINT(soh,  FORCESEEK))';


DBCC FREEPROCCACHE();


EXECUTE sp_control_plan_guide 
    @operation = 'Drop',
    @name = N'MyGoodSQLGuide';





DECLARE @plan_handle VARBINARY(64),
    @start_offset INT ;

SELECT  @plan_handle = deqs.plan_handle,
        @start_offset = deqs.statement_start_offset
FROM    sys.dm_exec_query_stats AS deqs
CROSS APPLY sys.dm_exec_sql_text(sql_handle)
CROSS APPLY sys.dm_exec_text_query_plan(deqs.plan_handle,
                                        deqs.statement_start_offset,
                                        deqs.statement_end_offset) AS qp
WHERE   text LIKE N'SELECT  soh.SalesOrderNumber%'

EXECUTE sp_create_plan_guide_from_handle 
    @name = N'ForcedPlanGuide',
    @plan_handle = @plan_handle,
    @statement_start_offset = @start_offset ; 
GO






--Chapter 18


SELECT  Name,
        TerritoryID
FROM    Sales.SalesTerritory AS st
WHERE   st.Name = 'Australia';



SELECT  *
FROM    Sales.SalesTerritory AS st
WHERE   st.Name = 'Australia';




SELECT  sod.*
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.SalesOrderID = 51825 OR
        sod.SalesOrderID = 51826 OR
        sod.SalesOrderID = 51827 OR
        sod.SalesOrderID = 51828 OR
		sod.SalesOrderID = 51829 OR
		sod.SalesOrderID = 51830 OR
		sod.SalesOrderID = 51831 OR
		sod.SalesOrderID = 51832 OR
		sod.SalesOrderID = 51833 OR
		sod.SalesOrderID = 51834 OR
		sod.SalesOrderID = 51835 OR
		sod.SalesOrderID = 51836;

SELECT * FROM Sales.SalesOrderDetail AS sod
WHERE sod.SalesOrderID IN (51825,51826,51827,51828,51829,51830,51831,51832,51833,51834,51835,51836);


SELECT  sod.*
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.SalesOrderID BETWEEN 51825 AND 51836;



SELECT  c.CurrencyCode
FROM    Sales.Currency AS c
WHERE   c.[Name] LIKE 'Ice%';



SELECT  c.CurrencyCode
FROM    Sales.Currency AS c
WHERE   c.[Name] >= N'Ice'
        AND c.[Name] < N'IcF';





SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh
WHERE   poh.PurchaseOrderID >= 2975;
SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh
WHERE   poh.PurchaseOrderID !< 2975;



SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh
WHERE   poh.PurchaseOrderID * 2 = 3400;


SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh
WHERE   poh.PurchaseOrderID = 3400 / 2;







SELECT  d.Name
FROM    HumanResources.Department AS d
WHERE   SUBSTRING(d.[Name], 1, 1) = 'F';




SELECT  d.Name
FROM    HumanResources.Department AS d
WHERE   d.[Name] LIKE 'F%';






IF EXISTS ( SELECT  *
            FROM    sys.indexes
            WHERE   object_id = OBJECT_ID(N'[Sales].[SalesOrderHeader]')
                    AND name = N'IndexTest' ) 
    DROP INDEX IndexTest ON [Sales].[SalesOrderHeader] ;
GO
CREATE INDEX IndexTest ON Sales.SalesOrderHeader(OrderDate) ;






SELECT  soh.SalesOrderID,
        soh.OrderDate
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   DATEPART(yy, soh.OrderDate) = 2008
        AND DATEPART(mm, soh.OrderDate) = 4;



SELECT  soh.SalesOrderID,
        soh.OrderDate
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.OrderDate >= '2008-04-01'
        AND soh.OrderDate < '2008-05-01';




DROP INDEX Sales.SalesOrderHeader.IndexTest;






SELECT s.[Name] AS StoreName,
    p.[LastName] + ', ' + p.[FirstName]
FROM   [Sales].[Store] s
JOIN   [Sales].SalesPerson AS sp
    ON s.SalesPersonID = sp.BusinessEntityID
JOIN   HumanResources.Employee AS e
    ON sp.BusinessEntityID = e.BusinessEntityID
JOIN   Person.Person AS p
    ON e.BusinessEntityID = p.BusinessEntityID;




SELECT  s.[Name] AS StoreName,
        p.[LastName] + ',   ' + p.[FirstName]
FROM    [Sales].[Store] s
JOIN    [Sales].SalesPerson AS sp
        ON s.SalesPersonID = sp.BusinessEntityID
JOIN    HumanResources.Employee AS e
        ON sp.BusinessEntityID = e.BusinessEntityID
JOIN    Person.Person AS p
        ON e.BusinessEntityID = p.BusinessEntityID
OPTION  (LOOP JOIN);




SELECT  s.[Name] AS StoreName,
        p.[LastName] + ',   ' + p.[FirstName]
FROM    [Sales].[Store] s
INNER LOOP JOIN [Sales].SalesPerson AS sp
        ON s.SalesPersonID = sp.BusinessEntityID
JOIN    HumanResources.Employee AS e
        ON sp.BusinessEntityID = e.BusinessEntityID
JOIN    Person.Person AS p
        ON e.BusinessEntityID = p.BusinessEntityID;




SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh
WHERE   poh.PurchaseOrderID = 3400 / 2;

SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh
WHERE   poh.PurchaseOrderID * 2 = 3400;

SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh WITH (INDEX (PK_PurchaseOrderHeader_PurchaseOrderID))
WHERE   poh.PurchaseOrderID * 2 = 3400 ;








SELECT  p.FirstName
FROM    Person.Person AS p
WHERE   p.FirstName < 'B'
        OR p.FirstName >= 'C';

SELECT  p.MiddleName
FROM    Person.Person AS p
WHERE   p.MiddleName < 'B'
        OR p.MiddleName >= 'C';




CREATE INDEX TestIndex1
ON Person.Person (MiddleName); 

CREATE INDEX TestIndex2 
ON Person.Person (FirstName);



SELECT  p.FirstName
FROM    Person.Person AS p
WHERE   p.FirstName < 'B'
        OR p.FirstName >= 'C' ;

SELECT  p.MiddleName
FROM    Person.Person AS p
WHERE   p.MiddleName < 'B'
        OR p.MiddleName >= 'C'
        OR p.MiddleName IS NULL;




DROP INDEX TestIndex1 ON Person.Person ;

DROP INDEX TestIndex2 ON Person.Person ;



IF EXISTS ( SELECT  *
            FROM    sys.foreign_keys
            WHERE   object_id = OBJECT_ID(N'[Person].[FK_Address_StateProvince_StateProvinceID]')
                    AND parent_object_id = OBJECT_ID(N'[Person].[Address]') )
    ALTER TABLE  [Person].[Address] DROP CONSTRAINT [FK_Address_StateProvince_StateProvinceID];






SELECT  a.AddressID,
        sp.StateProvinceID
FROM    Person.Address AS a
JOIN    Person.StateProvince AS sp
        ON a.StateProvinceID = sp.StateProvinceID
WHERE   a.AddressID = 27234;


SELECT  a.AddressID,
        a.StateProvinceID
FROM    Person.Address AS a
JOIN    Person.StateProvince AS sp
        ON a.StateProvinceID = sp.StateProvinceID
WHERE   a.AddressID = 27234;




ALTER TABLE [Person].[Address]
WITH CHECK ADD CONSTRAINT [FK_Address_StateProvince_StateProvinceID] 
FOREIGN KEY ([StateProvinceID]) 
REFERENCES [Person].[StateProvince] ([StateProvinceID]);






--Chapter 19

IF EXISTS ( SELECT  *
            FROM    sys.objects
            WHERE   object_id = OBJECT_ID(N'dbo.Test1') )
    DROP TABLE  dbo.Test1; 

CREATE TABLE dbo.Test1 (
Id INT IDENTITY(1,1),
MyKey VARCHAR(50),
MyValue VARCHAR(50)); 
CREATE UNIQUE CLUSTERED INDEX Test1PrimaryKey ON dbo.Test1  ([Id] ASC); 
CREATE UNIQUE NONCLUSTERED INDEX TestIndex ON dbo.Test1 (MyKey); 
GO

SELECT TOP 10000
        IDENTITY( INT,1,1 ) AS n
INTO    #Tally
FROM    Master.dbo.SysColumns scl,
        Master.dbo.SysColumns sc2;

INSERT  INTO dbo.Test1
        (MyKey,
         MyValue)
        SELECT TOP 10000
                'UniqueKey' + CAST(n AS VARCHAR),
                'Description'
        FROM    #Tally;

DROP TABLE #Tally;

SELECT  t.MyValue
FROM    dbo.Test1 AS t
WHERE   t.MyKey = 'UniqueKey333';

SELECT  t.MyValue
FROM    dbo.Test1 AS t
WHERE   t.MyKey = N'UniqueKey333';








DECLARE @n INT;
SELECT  @n = COUNT(*)
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.OrderQty = 1;
IF @n > 0
    PRINT 'Record Exists';




IF EXISTS ( SELECT  sod.*
            FROM    Sales.SalesOrderDetail AS sod
            WHERE   sod.OrderQty = 1 ) 
    PRINT 'Record Exists';




SELECT  *
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.ProductID = 934
UNION
SELECT  *
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.ProductID = 932;

SELECT  *
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.ProductID = 934
UNION ALL
SELECT  *
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.ProductID = 932;










SELECT  MIN(sod.UnitPrice)
FROM    Sales.SalesOrderDetail AS sod;



CREATE INDEX TestIndex ON Sales.SalesOrderDetail (UnitPrice ASC);





DECLARE @Startid INT = 1, @StopID INT = 42;
SELECT  pod.LineTotal,
poh.OrderDate
FROM    Purchasing.PurchaseOrderDetail AS pod
JOIN    Purchasing.PurchaseOrderHeader AS poh
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
WHERE   poh.PurchaseOrderID BETWEEN @StartId AND @StopID;

DECLARE @Id INT = 1;
SELECT  pod.LineTotal,
        poh.OrderDate
FROM    Purchasing.PurchaseOrderDetail AS pod
JOIN    Purchasing.PurchaseOrderHeader AS poh
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
WHERE   poh.PurchaseOrderID >= @Id;

SELECT  pod.LineTotal,
        poh.OrderDate
FROM    Purchasing.PurchaseOrderDetail AS pod
JOIN    Purchasing.PurchaseOrderHeader AS poh
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
WHERE   poh.PurchaseOrderID >= 1;



CREATE PROCEDURE spProductDetails (@id INT)
AS 
SELECT  pod.LineTotal,
		poh.OrderDate
FROM    Purchasing.PurchaseOrderDetail AS pod
JOIN    Purchasing.PurchaseOrderHeader AS poh
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
WHERE   poh.PurchaseOrderID >= @id;
 
GO 
EXEC spProductDetails 
    @id = 1;







IF EXISTS ( SELECT  *
            FROM    sys.objects
            WHERE   object_id = OBJECT_ID(N'[dbo].[sp_Dont]')
                    AND type IN (N'P',N'PC') )
    DROP PROCEDURE  [dbo].[sp_Dont]
GO
CREATE PROC [sp_Dont]
AS
PRINT 'Done!' 
GO
--Add plan of sp_Dont to procedure cache
EXEC AdventureWorks2012.dbo.[sp_Dont]; 
GO
--Use the above cached plan of sp_Dont
EXEC AdventureWorks2012.dbo.[sp_Dont]; 
GO




CREATE PROC sp_addmessage @param1 NVARCHAR(25) 
AS
PRINT  '@param1 =  '  + @param1 ;
GO

EXEC AdventureWorks2012.dbo.[sp_addmessage] 'AdventureWorks';

DROP PROC dbo.[sp_addmessage]








--Create a test table 
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1; 
GO
CREATE TABLE dbo.Test1 (C1 TINYINT); 
GO

--Insert 10000 rows 
DBCC SQLPERF(LOGSPACE);
DECLARE @Count INT = 1;
WHILE @Count <= 10000
    BEGIN
        INSERT  INTO dbo.Test1
                (C1)
        VALUES  (@Count % 256);
        SET @Count = @Count + 1;
    END
DBCC SQLPERF(LOGSPACE);


DECLARE @Count INT = 1;
DBCC SQLPERF(LOGSPACE);
BEGIN TRANSACTION
WHILE @Count <= 10000 
    BEGIN
        INSERT  INTO dbo.Test1
                (C1)
        VALUES  (@Count % 256) ;
        SET @Count = @Count + 1 ;
        END
COMMIT
DBCC SQLPERF(LOGSPACE);










--Chapter 20

--Create a test table
IF (SELECT  OBJECT_ID('dbo.ProductTest')
   ) IS NOT NULL
    DROP TABLE dbo.ProductTest;
GO 
CREATE TABLE dbo.ProductTest (
ProductID INT CONSTRAINT ValueEqualsOne CHECK (ProductID = 1));
GO
--All ProductIDs are added into t1 as a logical unit of work
INSERT  INTO dbo.ProductTest
        SELECT  p.ProductID
        FROM    Production.Product AS p;
GO
SELECT  *
FROM    dbo.ProductTest; --Returns 0 rows




BEGIN TRAN
 --Start:  Logical unit of work
--First:
INSERT  INTO dbo.ProductTest
        SELECT  p.ProductID
        FROM    Production.Product AS p;
--Second:
INSERT  INTO dbo.ProductTest
VALUES  (1);
COMMIT --End:   Logical unit of work
GO


SELECT  *
FROM    dbo.ProductTest; --Returns a row with t1.c1 = 1




SET XACT_ABORT ON;
GO
BEGIN TRAN
 --Start:  Logical unit of work
--First:
INSERT  INTO dbo.ProductTest
        SELECT  p.ProductID
        FROM    Production.Product AS p
--Second:
INSERT  INTO dbo.ProductTest
VALUES  (1)
COMMIT
 --End:   Logical unit of work GO
SET XACT_ABORT OFF;
GO




BEGIN TRY
    BEGIN TRAN 
	--Start: Logical unit of work 
	--First:
    INSERT  INTO dbo.ProductTest
            SELECT  p.ProductID
            FROM    Production.Product AS p

    Second:
    INSERT  INTO dbo.ProductTest
            (ProductID)
    VALUES  (1)
    COMMIT --End: Logical unit of work 
END TRY 
BEGIN CATCH
    ROLLBACK
    PRINT 'An error occurred'
    RETURN 
END CATCH




--Create a test table
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1;
GO
CREATE TABLE dbo.Test1 (C1 INT);
INSERT  INTO dbo.Test1
VALUES  (1);
GO

BEGIN TRAN
DELETE  dbo.Test1
WHERE   C1 = 1; 

SELECT  dtl.request_session_id,
        dtl.resource_database_id,
        dtl.resource_associated_entity_id,
        dtl.resource_type,
        dtl.resource_description,
        dtl.request_mode,
        dtl.request_status
FROM    sys.dm_tran_locks AS dtl
WHERE   dtl.request_session_id = @@SPID;
ROLLBACK




SELECT  OBJECT_NAME(1668200993),
        DB_NAME(5);



CREATE CLUSTERED INDEX TestIndex ON dbo.Test1(C1);

BEGIN TRAN
DELETE  dbo.Test1
WHERE   C1 = 1 ;

SELECT  dtl.request_session_id,
        dtl.resource_database_id,
        dtl.resource_associated_entity_id,
        dtl.resource_type,
        dtl.resource_description,
        dtl.request_mode,
        dtl.request_status
FROM    sys.dm_tran_locks AS dtl
WHERE   dtl.request_session_id = @@SPID ;
ROLLBACK




BEGIN TRANSACTION LockTran1
UPDATE  Sales.Currency
SET     Name = 'Euro'
WHERE   CurrencyCode = 'EUR';
COMMIT



--Execute from a second connection

BEGIN TRANSACTION LockTran2
--Retain an  (S) lock on the resource
SELECT  *
FROM    Sales.Currency AS c WITH (REPEATABLEREAD)
WHERE   c.CurrencyCode = 'EUR' ;
--Allow sp_lock to be executed before second step of
-- UPDATE statement is executed by transaction LockTran1
WAITFOR DELAY  '00:00:01' ; 
COMMIT



SELECT  dtl.request_session_id,
        dtl.resource_database_id,
        dtl.resource_associated_entity_id,
        dtl.resource_type,
        dtl.resource_description,
        dtl.request_mode,
        dtl.request_status
FROM    sys.dm_tran_locks AS dtl
ORDER BY dtl.request_session_id;




BEGIN TRAN
--1.Read data to be modified using (S)lock instead of (U)lock.
--	Retain the (S)lock using REPEATABLEREAD locking hint, since
--	the original (U)lock is retained until the conversion to
--	(X)lock.
SELECT  *
FROM    Sales.Currency AS c WITH (REPEATABLEREAD)
WHERE   c.CurrencyCode = 'EUR' ;
--Allow another equivalent update action to start concurrently
WAITFOR DELAY '00:00:10' ;

--2. Modify the data by acquiring (X)lock
UPDATE  Sales.Currency WITH (XLOCK)
SET     Name = 'EURO'
WHERE   CurrencyCode = 'EUR' ;
COMMIT





BEGIN TRAN
DELETE  Sales.Currency
WHERE   CurrencyCode = 'ALL';

SELECT  tl.request_session_id,
        tl.resource_database_id,
        tl.resource_associated_entity_id,
        tl.resource_type,
        tl.resource_description,
        tl.request_mode,
        tl.request_status
FROM    sys.dm_tran_locks tl;

ROLLBACK TRAN





SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED




SELECT  *
FROM    Production.Product AS p WITH (NOLOCK);



SET TRANSACTION ISOLATION LEVEL READ COMMITTED



ALTER DATABASE AdventureWorks2012
SET READ_COMMITTED_SNAPSHOT ON;




BEGIN TRANSACTION;
SELECT  p.Color
FROM    Production.Product AS p
WHERE   p.ProductID = 711;
--commit



BEGIN TRANSACTION ;
UPDATE  Production.Product
SET     Color = 'Coyote'
WHERE   ProductID = 711;
SELECT  p.Color
FROM    Production.Product AS p
WHERE   p.ProductID = 711;




IF (SELECT  OBJECT_ID('dbo.MyProduct')
   ) IS NOT NULL 
DROP TABLE dbo.MyProduct ;
GO 
CREATE TABLE dbo.MyProduct
    (ProductID INT,
     Price MONEY
    ) ;
INSERT  INTO dbo.MyProduct
VALUES  (1, 15.0) ;



DECLARE @Price INT;
BEGIN TRAN NormailizePrice
SELECT  @Price = mp.Price
FROM    dbo.MyProduct AS mp
WHERE   mp.ProductID = 1;
/*Allow transaction 2 to execute*/
WAITFOR DELAY '00:00:10';
IF @Price > 10
    UPDATE  dbo.MyProduct
    SET     Price = Price - 10
    WHERE   ProductID = 1;
COMMIT

--Transaction 2 from Connection 2
BEGIN TRAN ApplyDiscount
UPDATE  dbo.MyProduct
SET     Price = Price * 0.6 --Discount = 40%
WHERE   Price > 10;
COMMIT





SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
GO
--Transaction 1 from Connection 1
DECLARE @Price INT;
BEGIN TRAN NormalizePrice
SELECT  @Price = Price
FROM    dbo.MyProduct AS mp
WHERE   mp.ProductID = 1;
/*Allow transaction 2 to execute*/
WAITFOR DELAY  '00:00:10';
IF @Price > 10
    UPDATE  dbo.MyProduct
    SET     Price = Price - 10
    WHERE   ProductID = 1;
COMMIT
GO
SET TRANSACTION ISOLATION LEVEL READ COMMITTED
 --Back to default
GO




DECLARE @Price INT;
BEGIN TRAN NormaalizePrice
SELECT  @Price = Price
FROM    dbo.MyProduct AS mp WITH (REPEATABLEREAD)
WHERE   mp.ProductID = 1;
/*Allow transaction 2 to execute*/
WAITFOR DELAY  '00:00:10'
IF @Price > 10
    UPDATE  dbo.MyProduct
    SET     Price = Price - 10
    WHERE   ProductID = 1;
COMMIT




DECLARE @Price INT;
BEGIN TRAN NormailizePrice
SELECT  @Price = Price
FROM    dbo.MyProduct AS mp WITH (UPDLOCK)
WHERE   mp.ProductID = 1;
/*Allow transaction 2 to execute*/
WAITFOR DELAY  '00:00:10'
IF @Price > 10
    UPDATE  dbo.MyProduct
    SET     Price = Price - 10
    WHERE   ProductID = 1;
COMMIT


IF (SELECT  OBJECT_ID('dbo.MyEmployees')
   ) IS NOT NULL 
    DROP TABLE dbo.MyEmployees ; 
GO 
CREATE TABLE dbo.MyEmployees
    (EmployeeID INT,
     GroupID INT,
     Salary MONEY
    ) ;
CREATE CLUSTERED INDEX i1 ON dbo.MyEmployees  (GroupID) ;

--Employee 1 in group 10
INSERT  INTO dbo.MyEmployees
VALUES  (1,10,1000),
--Employee 2 in group 10
        (2,10,1000),
--Employees 3 & 4 in different groups
        (3,20,1000),
        (4,9,1000);






DECLARE @Fund MONEY = 100,
    @Bonus MONEY,
    @NumberOfEmployees INT;

BEGIN TRAN PayBonus
SELECT  @NumberOfEmployees = COUNT(*)
FROM    dbo.MyEmployees
WHERE   GroupID = 10;

/*Allow transaction 2 to execute*/
WAITFOR DELAY  '00:00:10';

IF @NumberOfEmployees > 0
    BEGIN
        SET @Bonus = @Fund / @NumberOfEmployees;
        UPDATE  dbo.MyEmployees
        SET     Salary = Salary + @Bonus
        WHERE   GroupID = 10;
        PRINT 'Fund balance =
' + CAST((@Fund - (@@ROWCOUNT * @Bonus)) AS VARCHAR(6)) + '   $';
    END
COMMIT





SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
GO
DECLARE @Fund MONEY = 100,
    @Bonus MONEY,
    @NumberOfEmployees INT;

BEGIN TRAN PayBonus
SELECT  @NumberOfEmployees = COUNT(*)
FROM    dbo.MyEmployees
WHERE   GroupID = 10;

/*Allow transaction 2 to execute*/
WAITFOR DELAY  '00:00:10';
IF @NumberOfEmployees > 0
    BEGIN
        SET @Bonus = @Fund / @NumberOfEmployees;
        UPDATE  dbo.MyEmployees
        SET     Salary = Salary + @Bonus
        WHERE   GroupID = 10;

        PRINT 'Fund balance =
' + CAST((@Fund - (@@ROWCOUNT * @Bonus)) AS VARCHAR(6)) + '   $';
    END
COMMIT 
GO
--Back to default 
SET TRANSACTION ISOLATION LEVEL READ COMMITTED ;
GO






DECLARE @Fund MONEY = 100,
    @Bonus MONEY,
    @NumberOfEmployees INT ;

BEGIN TRAN PayBonus
SELECT  @NumberOfEmployees = COUNT(*)
FROM    dbo.MyEmployees WITH (HOLDLOCK)
WHERE   GroupID = 10 ;

/*Allow transaction 2 to execute*/
WAITFOR DELAY  '00:00:10' ;

IF @NumberOfEmployees > 0
    BEGIN
        SET @Bonus = @Fund / @NumberOfEmployees
        UPDATE  dbo.MyEmployees
        SET     Salary = Salary + @Bonus
        WHERE   GroupID = 10 ;

        PRINT 'Fund balance =
' + CAST((@Fund - (@@ROWCOUNT * @Bonus)) AS VARCHAR(6)) + '   $' ;
    END
COMMIT



BEGIN TRAN NewEmployee
INSERT  INTO dbo.MyEmployees
VALUES  (6, 15, 1000);
COMMIT


DELETE  dbo.MyEmployees
WHERE   GroupID > 10;




BEGIN TRAN NewEmployee
INSERT  INTO dbo.MyEmployees
VALUES  (7, 999, 1000);
COMMIT




IF (SELECT  OBJECT_ID('dbo.MyEmployees')
   ) IS NOT NULL
    DROP TABLE dbo.MyEmployees;
GO
CREATE TABLE dbo.MyEmployees
    (EmployeeID INT,
     GroupID INT,
     Salary MONEY
    );
CREATE CLUSTERED INDEX i1 ON dbo.MyEmployees  (EmployeeID);

--Employee 1 in group 10
INSERT  INTO dbo.MyEmployees
VALUES  (1,10,1000),
--Employee 2 in group 10
        (2,10,1000),
--Employees 3 & 4 in different groups
        (3,20,1000),
        (4,9,1000);







IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1;
GO

CREATE TABLE dbo.Test1 (C1 INT, C2 DATETIME);

INSERT  INTO dbo.Test1
VALUES  (1, GETDATE());



BEGIN TRAN LockBehavior
UPDATE  dbo.Test1 WITH (REPEATABLEREAD)  --Hold all acquired locks
SET     C2 = GETDATE()
WHERE   C1 = 1 ;
--Observe lock behavior from another connection
WAITFOR DELAY  '00:00:10' ;
COMMIT


CREATE NONCLUSTERED INDEX iTest ON dbo.Test1(C1);





ALTER INDEX iTest ON dbo.Test1 
    SET (ALLOW_ROW_LOCKS = OFF ,ALLOW_PAGE_LOCKS= OFF);

BEGIN TRAN LockBehavior
UPDATE  dbo.Test1 WITH (REPEATABLEREAD)  --Hold all acquired locks
SET     C2 = GETDATE()
WHERE   C1 = 1;

--Observe lock behavior using sys.dm_tran_locks 
--from another connection
WAITFOR DELAY  '00:00:10';
COMMIT

ALTER INDEX iTest ON dbo.Test1 
    SET (ALLOW_ROW_LOCKS = ON ,ALLOW_PAGE_LOCKS= ON);



CREATE CLUSTERED INDEX iTest ON dbo.Test1(C1) WITH DROP_EXISTING;


SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
GO
BEGIN TRAN SerializableTran
DECLARE @NumberOfEmployees INT;
SELECT  @NumberOfEmployees = COUNT(*)
FROM    dbo.MyEmployees WITH (HOLDLOCK)
WHERE   GroupID = 10;
WAITFOR DELAY '00:00:10';
COMMIT
GO
SET TRANSACTION ISOLATION LEVEL READ COMMITTED ;
GO




SELECT  dtl.request_session_id AS WaitingSessionID,
        der.blocking_session_id AS BlockingSessionID,
        dowt.resource_description,
        der.wait_type,
        dowt.wait_duration_ms,
        DB_NAME(dtl.resource_database_id) AS DatabaseName,
        dtl.resource_associated_entity_id AS WaitingAssociatedEntity,
        dtl.resource_type AS WaitingResourceType,
        dtl.request_type AS WaitingRequestType,
        dest.[text] AS WaitingTSql,
        dtlbl.request_type BlockingRequestType,
        destbl.[text] AS BlockingTsql
FROM    sys.dm_tran_locks AS dtl
JOIN    sys.dm_os_waiting_tasks AS dowt
        ON dtl.lock_owner_address = dowt.resource_address
JOIN    sys.dm_exec_requests AS der
        ON der.session_id = dtl.request_session_id
CROSS APPLY sys.dm_exec_sql_text(der.sql_handle) AS dest
LEFT JOIN sys.dm_exec_requests derbl
        ON derbl.session_id = dowt.blocking_session_id
OUTER APPLY sys.dm_exec_sql_text(derbl.sql_handle) AS destbl
LEFT JOIN sys.dm_tran_locks AS dtlbl
        ON derbl.session_id = dtlbl.request_session_id;





IF (SELECT  OBJECT_ID('dbo.BlockTest')
   ) IS NOT NULL
    DROP TABLE dbo.BlockTest;
GO 

CREATE TABLE dbo.BlockTest
    (C1 INT,
     C2 INT,
     C3 DATETIME
    );

INSERT  INTO dbo.BlockTest
VALUES  (11, 12, GETDATE()),
        (21, 22, GETDATE());



ALTER DATABASE AdventureWorks2012
SET READ_COMMITTED_SNAPSHOT OFF;

SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
GO

BEGIN TRAN User1
UPDATE  dbo.BlockTest
SET     C3 = GETDATE();
--COMMIT



BEGIN TRAN User2
SELECT  C2
FROM    dbo.BlockTest
WHERE   C1 = 11;
COMMIT



EXEC sp_configure 'show advanced option', '1';
RECONFIGURE;
EXEC sp_configure 
    'blocked process threshold',
    5;
RECONFIGURE;





CREATE CLUSTERED INDEX i1 ON dbo.BlockTest(C1);




SET TRANSACTION ISOLATION LEVEL READ COMMITTED snapshot;
GO
BEGIN TRAN User2
SELECT  C2
FROM    dbo.BlockTest
WHERE   C1 = 11;
COMMIT 
GO
--Back to default
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
GO


ALTER DATABASE AdventureWorks2012
SET READ_COMMITTED_SNAPSHOT OFF;



--Chapter 21

DECLARE @retry AS TINYINT = 1,
    @retrymax AS TINYINT = 2,
    @retrycount AS TINYINT = 0;
WHILE @retry = 1
    AND @retrycount <= @retrymax
    BEGIN
        SET @retry = 0;
            
        BEGIN TRY
            UPDATE  HumanResources.Employee
            SET     LoginID = '54321'
            WHERE   BusinessEntityID = 100;
        END TRY
        BEGIN CATCH
            IF (ERROR_NUMBER() = 1205)
                BEGIN
                    SET @retrycount = @retrycount + 1;
                    SET @retry = 1;
                END
        END CATCH
    END




--Step 1
BEGIN TRAN
UPDATE  Purchasing.PurchaseOrderHeader
SET     Freight = Freight * 0.9 -- 10% discount on shipping
WHERE   PurchaseOrderID = 1255;

--Step 2
UPDATE  Purchasing.PurchaseOrderDetail
SET     OrderQty = 2
WHERE   ProductID = 448
        AND PurchaseOrderID = 1255 ;


ROLLBACK


--In a seperate connection
BEGIN TRAN
UPDATE  Purchasing.PurchaseOrderDetail
SET     OrderQty = 4
WHERE   ProductID = 448
        AND PurchaseOrderID = 1255 ;







DBCC TRACEON (1222, -1);
DBCC TRACEON (1204, -1);






--Chapter 22



--Associate a SELECT statement to a cursor and define the
--cursor's characteristics
DECLARE MyCursor CURSOR /*<cursor characteristics>*/
FOR
SELECT  adt.AddressTypeID,
        adt.Name,
        adt.ModifiedDate
FROM    Person.AddressType adt;


--Open the cursor to access the result set returned by the
--SELECT statement
OPEN MyCursor;

--Retrieve one row at a time from the result set returned by
--the SELECT statement
DECLARE @AddressTypeId INT,
    @Name VARCHAR(50),
    @ModifiedDate DATETIME;

FETCH NEXT FROM MyCursor INTO @AddressTypeId,@Name,@ModifiedDate;

WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT 'NAME =   ' + @Name;

--Optionally, modify the row through the cursor
        UPDATE  Person.AddressType
        SET     Name = Name + 'z'
        WHERE CURRENT OF MyCursor;
        
        FETCH NEXT FROM MyCursor 
            INTO @AddressTypeId,@Name,@ModifiedDate;
    END 

--Close the cursor and release all resources assigned to the
--cursor
CLOSE MyCursor;
DEALLOCATE MyCursor;





UPDATE  Person.AddressType
SET     [Name] = LEFT([Name], LEN([Name]) - 1);







DECLARE MyCursor CURSOR READ_ONLY
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1 ;





DECLARE MyCursor CURSOR OPTIMISTIC
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1;





DECLARE MyCursor CURSOR SCROLL_LOCKS
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1;






DECLARE MyCursor CURSOR FAST_FORWARD
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1;







DECLARE MyCursor CURSOR STATIC
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1;



DECLARE MyCursor CURSOR KEYSET
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1;





USE AdventureWorks2012;
GO
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1;
GO

CREATE TABLE dbo.Test1 (C1 INT,C2 CHAR(996));

CREATE CLUSTERED INDEX Test1Index ON dbo.Test1 (C1);

INSERT  INTO dbo.Test1
VALUES  (1,'1') ,
        (2,'2'); 
GO





SELECT TOP 100000
        IDENTITY( INT,1,1 ) AS n
INTO    #Tally
FROM    Master.dbo.SysColumns scl,
        Master.dbo.SysColumns sc2;

INSERT  INTO dbo.Test1
        (C1, C2)
SELECT  n,
        n
FROM    #Tally AS t; 
GO



SELECT  dtl.request_session_id,
        dtl.resource_database_id,
        dtl.resource_associated_entity_id,
        dtl.resource_type,
        dtl.resource_description,
        dtl.request_mode,
        dtl.request_status
FROM    sys.dm_tran_locks AS dtl;





IF (SELECT  OBJECT_ID('dbo.TotalLoss_CursorBased')
   ) IS NOT NULL
    DROP PROC dbo.TotalLoss_CursorBased;
GO

CREATE PROC dbo.TotalLoss_CursorBased
AS --Declare a T-SQL cursor with default settings,  i.e.,  fast
--forward-only to retrieve products that have been discarded
DECLARE ScrappedProducts CURSOR
FOR
SELECT  p.ProductID,
        wo.ScrappedQty,
        p.ListPrice
FROM    Production.WorkOrder AS wo
JOIN    Production.ScrapReason AS sr
        ON wo.ScrapReasonID = sr.ScrapReasonID
JOIN    Production.Product AS p
        ON wo.ProductID = p.ProductID;

--Open the cursor to process one product at a time
OPEN ScrappedProducts;

DECLARE @MoneyLostPerProduct MONEY = 0,
    @TotalLoss MONEY = 0;

--Calculate money lost per product by processing one product
--at a time
DECLARE @ProductId INT,
    @UnitsScrapped SMALLINT,
    @ListPrice MONEY;

FETCH NEXT FROM ScrappedProducts INTO @ProductId,@UnitsScrapped,@ListPrice;

WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @MoneyLostPerProduct = @UnitsScrapped * @ListPrice; --Calculate total loss
        SET @TotalLoss = @TotalLoss + @MoneyLostPerProduct;
        
        FETCH NEXT FROM ScrappedProducts INTO @ProductId,@UnitsScrapped,
            @ListPrice;
    END

--Determine status
IF (@TotalLoss > 5000)
    SELECT  'We are bankrupt!' AS Status;
ELSE
    SELECT  'We are safe!' AS Status;
--Close the cursor and release all resources assigned to the cursor
CLOSE ScrappedProducts;
DEALLOCATE ScrappedProducts;
GO





EXEC dbo.TotalLoss_CursorBased;




SELECT  SUM(page_count)
FROM    sys.dm_db_index_physical_stats(DB_ID(N'AdventureWorks2012'),
            OBJECT_ID('Production.Products'),
            DEFAULT, DEFAULT, DEFAULT);




IF (SELECT  OBJECT_ID('dbo.TotalLoss')
   ) IS NOT NULL
    DROP PROC dbo.TotalLoss;
GO
CREATE PROC dbo.TotalLoss
AS
SELECT  CASE  --Determine status based on following computation
             WHEN SUM(MoneyLostPerProduct) > 5000 THEN 'We are bankrupt!'
             ELSE 'We are safe!'
        END AS Status
FROM    (--Calculate total money lost for all discarded products
         SELECT SUM(wo.ScrappedQty * p.ListPrice) AS MoneyLostPerProduct
         FROM   Production.WorkOrder AS wo
         JOIN   Production.ScrapReason AS sr
                ON wo.ScrapReasonID = sr.ScrapReasonID
         JOIN   Production.Product AS p
                ON wo.ProductID = p.ProductID
         GROUP BY p.ProductID
        ) DiscardedProducts;
GO


EXEC dbo.TotalLoss;



--CHAPTER 23
USE master
GO

CREATE DATABASE InMemoryTest ON PRIMARY 
	(NAME = N'InMemoryTest_Data',
	FILENAME = N'C:\Data\InMemoryTest_Data.mdf', 
	SIZE = 5GB)
LOG ON 
	(NAME = N'InMemoryTest_Log',
	FILENAME = N'C:\Data\InMemoryTest_Log.ldf');





ALTER DATABASE InMemoryTest 
	ADD FILEGROUP InMemoryTest_InMemoryData
	CONTAINS MEMORY_OPTIMIZED_DATA;
ALTER DATABASE InMemoryTest 
	ADD FILE (NAME='InMemoryTest_InMemoryData', 
	filename='C:\Data\InMemoryTest_InMemoryData.ndf') 
	TO FILEGROUP InMemoryTest_InMemoryData;

DROP PROCEDURE dbo.AddressDetails;
DROP TABLE dbo.Address;


CREATE TABLE dbo.Address
    (
     AddressID INT IDENTITY(1, 1)
                   NOT NULL
                   PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT = 50000),
     AddressLine1 NVARCHAR(60) NOT NULL,
     AddressLine2 NVARCHAR(60) NULL,
     City NVARCHAR(30) NOT NULL,
     StateProvinceID INT NOT NULL,
     PostalCode NVARCHAR(15) NOT NULL,
	--[SpatialLocation geography NULL,
	--rowguid uniqueidentifier ROWGUIDCOL  NOT NULL CONSTRAINT DF_Address_rowguid  DEFAULT (newid()),
     ModifiedDate DATETIME
        NOT NULL
        CONSTRAINT DF_Address_ModifiedDate DEFAULT (GETDATE())
    )
    WITH (
         MEMORY_OPTIMIZED=
         ON,
         DURABILITY =
         SCHEMA_AND_DATA);





SELECT  a.AddressID
FROM    dbo.Address AS a
WHERE	a.AddressID = 42;




CREATE TABLE dbo.AddressStaging(
	AddressLine1 nvarchar(60) NOT NULL,
	AddressLine2 nvarchar(60) NULL,
	City nvarchar(30) NOT NULL,
	StateProvinceID int NOT NULL,
	PostalCode nvarchar(15) NOT NULL
);


INSERT  dbo.AddressStaging
        (AddressLine1,
         AddressLine2,
         City,
         StateProvinceID,
         PostalCode
        )
SELECT  a.AddressLine1,
        a.AddressLine2,
        a.City,
        a.StateProvinceID,
        a.PostalCode
FROM    AdventureWorks2012.Person.Address AS a;


INSERT  dbo.Address
        (AddressLine1,
         AddressLine2,
         City,
         StateProvinceID,
         PostalCode
        )
SELECT  a.AddressLine1,
        a.AddressLine2,
        a.City,
        a.StateProvinceID,
        a.PostalCode
FROM    dbo.AddressStaging AS a;

DROP TABLE dbo.AddressStaging;




DROP TABLE dbo.StateProvince;


CREATE TABLE dbo.StateProvince(
	 StateProvinceID int IDENTITY(1,1) NOT NULL PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT=10000),
	 StateProvinceCode nchar(3) NOT NULL,
	 CountryRegionCode nvarchar(3) COLLATE Latin1_General_100_BIN2 NOT NULL,
	 Name VARCHAR(50) NOT NULL,
	 TerritoryID int NOT NULL,
	 ModifiedDate datetime NOT NULL CONSTRAINT DF_StateProvince_ModifiedDate  DEFAULT (getdate())
) WITH (MEMORY_OPTIMIZED=ON);



CREATE TABLE dbo.CountryRegion(
	CountryRegionCode nvarchar(3) NOT NULL,
	Name VARCHAR(50) NOT NULL,
	ModifiedDate datetime NOT NULL CONSTRAINT DF_CountryRegion_ModifiedDate  DEFAULT (getdate()),
 CONSTRAINT PK_CountryRegion_CountryRegionCode PRIMARY KEY CLUSTERED 
(
	CountryRegionCode ASC
));




SELECT  sp.StateProvinceCode,
        sp.CountryRegionCode,
        sp.Name,
        sp.TerritoryID
INTO    dbo.StateProvinceStaging
FROM    AdventureWorks2012.Person.StateProvince AS sp;

INSERT  dbo.StateProvince
        (StateProvinceCode,
         CountryRegionCode,
         Name,
         TerritoryID
        )
SELECT  stateprovincecode,
        countryregioncode,
        name,
        territoryid
FROM    dbo.stateprovincestaging;


DROP TABLE dbo.StateProvinceStaging;


INSERT  dbo.countryregion
        (countryregioncode,
         name
        )
        SELECT  cr.CountryRegionCode,
                cr.Name
        FROM    AdventureWorks2012.Person.CountryRegion AS cr;


USE InMemoryTest;

SELECT  a.AddressLine1,
        a.City,
        a.PostalCode,
        sp.Name AS StateProvinceName,
        cr.Name AS CountryName
FROM    dbo.Address AS a
        JOIN dbo.StateProvince AS sp
        ON sp.StateProvinceID = a.StateProvinceID
        JOIN dbo.CountryRegion cr
        ON cr.CountryRegionCode = sp.CountryRegionCode
WHERE a.AddressID = 42;


USE AdventureWorks2012;

SELECT  a.AddressLine1,
        a.City,
        a.PostalCode,
        sp.Name AS StateProvinceName,
        cr.Name AS CountryName
FROM    Person.Address AS a
        JOIN Person.StateProvince AS sp
        ON sp.StateProvinceID = a.StateProvinceID
        JOIN Person.CountryRegion AS cr
        ON cr.CountryRegionCode = sp.CountryRegionCode
WHERE   a.AddressID = 42;





USE InMemoryTest


SELECT  i.name AS 'index name',
        hs.total_bucket_count,
        hs.empty_bucket_count,
        hs.avg_chain_length,
        hs.max_chain_length
FROM    sys.dm_db_xtp_hash_index_stats AS hs
        JOIN sys.indexes AS i
        ON hs.object_id = i.object_id AND
           hs.index_id = i.index_id
WHERE   OBJECT_NAME(hs.object_id) = 'Address';

   






SELECT * FROM sys.dm_db_xtp_hash_index_stats AS ddxhis








SELECT  a.AddressLine1,
        a.City,
        a.PostalCode,
        sp.Name AS StateProvinceName,
        cr.Name AS CountryName
FROM    dbo.Address AS a
        JOIN dbo.StateProvince AS sp
        ON sp.StateProvinceID = a.StateProvinceID
        JOIN dbo.CountryRegion AS cr
        ON cr.CountryRegionCode = sp.CountryRegionCode
WHERE   a.City = 'Walla Walla';






DROP TABLE dbo.Address

CREATE TABLE dbo.Address(
	AddressID int IDENTITY(1,1) NOT NULL PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT=50000),
	AddressLine1 nvarchar(60) NOT NULL,
	AddressLine2 nvarchar(60) NULL,
	City nvarchar(30) COLLATE Latin1_General_100_BIN2 NOT NULL,
	StateProvinceID int NOT NULL,
	PostalCode nvarchar(15) NOT NULL,
	ModifiedDate datetime NOT NULL CONSTRAINT DF_Address_ModifiedDate  DEFAULT (getdate()),
	INDEX nci NONCLUSTERED (City)
) WITH (MEMORY_OPTIMIZED=ON);



CREATE TABLE dbo.AddressStaging(
	AddressLine1 nvarchar(60) NOT NULL,
	AddressLine2 nvarchar(60) NULL,
	City nvarchar(30) NOT NULL,
	StateProvinceID int NOT NULL,
	PostalCode nvarchar(15) NOT NULL
);


INSERT  dbo.AddressStaging
        (AddressLine1,
         AddressLine2,
         City,
         StateProvinceID,
         PostalCode
        )
SELECT  a.AddressLine1,
        a.AddressLine2,
        a.City,
        a.StateProvinceID,
        a.PostalCode
FROM    AdventureWorks2012.Person.Address AS a;


INSERT  dbo.Address
        (AddressLine1,
         AddressLine2,
         City,
         StateProvinceID,
         PostalCode
        )
SELECT  a.AddressLine1,
        a.AddressLine2,
        a.City,
        a.StateProvinceID,
        a.PostalCode
FROM    dbo.AddressStaging AS a;

DROP TABLE dbo.AddressStaging;





SELECT  a.AddressLine1,
        a.City,
        a.PostalCode,
        sp.Name AS StateProvinceName,
        cr.Name AS CountryName
FROM    dbo.Address AS a
        JOIN dbo.StateProvince AS sp
        ON sp.StateProvinceID = a.StateProvinceID
        JOIN dbo.CountryRegion AS cr
        ON cr.CountryRegionCode = sp.CountryRegionCode
WHERE   a.City = 'Walla Walla';







DBCC SHOW_STATISTICS (Address,nci);


UPDATE STATISTICS dbo.Address WITH FULLSCAN, NORECOMPUTE;


SELECT  s.name AS TableName,
		si.name AS IndexName,
        ddxis.scans_started,
        ddxis.rows_returned,
        ddxis.rows_expiring,
        ddxis.rows_expired
FROM    sys.dm_db_xtp_index_stats AS ddxis
        JOIN sys.sysobjects AS s
        ON ddxis.object_id = s.id
		JOIN sys.sysindexes AS si
		ON ddxis.object_id = si.id
		AND ddxis.index_id = si.indid






SELECT * FROM sys.dm_db_xtp_index_stats AS ddxis




SELECT * FROM sys.dm_db_xtp_nonclustered_index_stats AS ddxnis




CREATE PROC dbo.AddressDetails @City NVARCHAR(30)
    WITH NATIVE_COMPILATION,
         SCHEMABINDING,
         EXECUTE AS OWNER
AS
    BEGIN ATOMIC
WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')
        SELECT  a.AddressLine1,
                a.City,
                a.PostalCode,
                sp.Name AS StateProvinceName,
                cr.Name AS CountryName
        FROM    dbo.Address AS a
                JOIN dbo.StateProvince AS sp
                ON sp.StateProvinceID = a.StateProvinceID
                JOIN dbo.CountryRegion AS cr
                ON cr.CountryRegionCode = sp.CountryRegionCode
        WHERE   a.City = @City;
    END




DROP TABLE dbo.CountryRegion;

CREATE TABLE dbo.CountryRegion(
	CountryRegionCode nvarchar(3) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT = 1000) ,
	Name VARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
	ModifiedDate datetime NOT NULL CONSTRAINT DF_CountryRegion_ModifiedDate  DEFAULT (getdate()),
 )WITH (MEMORY_OPTIMIZED=ON);


SELECT  CountryRegionCode,
        Name
INTO    dbo.CountryRegionStaging
FROM    AdventureWorks2012.Person.CountryRegion AS cr;

INSERT dbo.CountryRegion
        (CountryRegionCode,
         Name
        )
SELECT CountryRegionCode,Name FROM CountryRegionStaging;






CREATE PROC dbo.AddressDetails @City NVARCHAR(30)
    WITH NATIVE_COMPILATION,
         SCHEMABINDING,
         EXECUTE AS OWNER
AS
    BEGIN ATOMIC
WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')
        SELECT  a.AddressLine1,
                a.City,
                a.PostalCode,
                sp.Name AS StateProvinceName,
                cr.Name AS CountryName
        FROM    dbo.Address AS a
                JOIN dbo.StateProvince AS sp
                ON sp.StateProvinceID = a.StateProvinceID
                JOIN dbo.CountryRegion AS cr
                ON cr.CountryRegionCode = sp.CountryRegionCode
        WHERE   a.City = @City;
    END


EXEC dbo.AddressDetails @City = N'Walla Walla';


SELECT * FROM dbo.Address AS a


USE InMemoryTest;
GO

CREATE TABLE dbo.AddressStaging
    (
     AddressID INT NOT NULL
                   IDENTITY(1, 1)
                   PRIMARY KEY,
     AddressLine1 NVARCHAR(60) NOT NULL,
     AddressLine2 NVARCHAR(60) NULL,
     City NVARCHAR(30) NOT NULL,
     StateProvinceID INT NOT NULL,
     PostalCode NVARCHAR(15) NOT NULL
    );






CREATE PROCEDURE dbo.FailWizard (@City NVARCHAR(30))
AS
    SELECT  a.AddressLine1,
            a.City,
            a.PostalCode,
            sp.Name AS StateProvinceName,
            cr.Name AS CountryName
    FROM    dbo.Address AS a
            JOIN dbo.StateProvince AS sp
            ON sp.StateProvinceID = a.StateProvinceID
            JOIN dbo.CountryRegion AS cr WITH ( NOLOCK)
            ON cr.CountryRegionCode = sp.CountryRegionCode
    WHERE   a.City = @City;
GO

CREATE PROCEDURE dbo.PassWizard (@City NVARCHAR(30))
AS
    SELECT  a.AddressLine1,
            a.City,
            a.PostalCode,
            sp.Name AS StateProvinceName,
            cr.Name AS CountryName
    FROM    dbo.Address AS a
            JOIN dbo.StateProvince AS sp
            ON sp.StateProvinceID = a.StateProvinceID
            JOIN dbo.CountryRegion AS cr
            ON cr.CountryRegionCode = sp.CountryRegionCode
    WHERE   a.City = @City;
GO






--Chapter 24


USE AdventureWorks2012;
GO

CREATE PROCEDURE dbo.ShoppingCart
    @ShoppingCartId VARCHAR(50)
AS 
--provides the output from the shopping cart including the line total
SELECT  sci.Quantity,
        p.ListPrice,
        p.ListPrice * sci.Quantity AS LineTotal,
        p.[Name]
FROM    Sales.ShoppingCartItem AS sci
JOIN    Production.Product AS p
        ON sci.ProductID = p.ProductID
WHERE   sci.ShoppingCartID = @ShoppingCartId ;
GO

CREATE PROCEDURE dbo.ProductBySalesOrder @SalesOrderID INT
AS
/*provides a list of products from a particular sales order, 
and provides line ordering by modified date but ordered 
by product name*/

SELECT  ROW_NUMBER() OVER (ORDER BY sod.ModifiedDate) AS LineNumber,
        p.[Name],
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
JOIN    Production.Product AS p
        ON sod.ProductID = p.ProductID
WHERE   soh.SalesOrderID = @SalesOrderID
ORDER BY p.[Name] ASC ;
GO

CREATE PROCEDURE dbo.PersonByFirstName
    @FirstName NVARCHAR(50)
AS 
--gets anyone by first name from the Person table
SELECT  p.BusinessEntityID,
        p.Title,
        p.LastName,
        p.FirstName,
        p.PersonType
FROM    Person.Person AS p
WHERE   p.FirstName = @FirstName ;
GO


CREATE PROCEDURE dbo.ProductTransactionsSinceDate
    @LatestDate DATETIME,
    @ProductName NVARCHAR(50)
AS
--Gets the latest transaction against 
--all products that have a transaction
SELECT  p.Name,
        th.ReferenceOrderID,
        th.ReferenceOrderLineID,
        th.TransactionType,
        th.Quantity
FROM    Production.Product AS p
JOIN    Production.TransactionHistory AS th
        ON p.ProductID = th.ProductID AND
           th.TransactionID = (SELECT TOP (1)
                                   th2.TransactionID
                          FROM     Production.TransactionHistory th2
                          WHERE    th2.ProductID = p.ProductID
                          ORDER BY th2.TransactionID DESC
                         )
WHERE   th.TransactionDate > @LatestDate AND
        p.Name LIKE @ProductName ;
GO


CREATE PROCEDURE dbo.PurchaseOrderBySalesPersonName
    @LastName NVARCHAR(50),
    @VendorID INT = NULL
AS
SELECT  poh.PurchaseOrderID,
        poh.OrderDate,
        pod.LineTotal,
        p.[Name] AS ProductName,
        e.JobTitle,
        per.LastName + ', ' + per.FirstName AS SalesPerson,
		poh.VendorID
FROM    Purchasing.PurchaseOrderHeader AS poh
        JOIN Purchasing.PurchaseOrderDetail AS pod
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
        JOIN Production.Product AS p
        ON pod.ProductID = p.ProductID
        JOIN HumanResources.Employee AS e
        ON poh.EmployeeID = e.BusinessEntityID
        JOIN Person.Person AS per
        ON e.BusinessEntityID = per.BusinessEntityID
WHERE   per.LastName LIKE @LastName AND
        poh.VendorID = COALESCE(@VendorID, poh.VendorID)
ORDER BY per.LastName,
        per.FirstName; 
GO





EXEC dbo.PurchaseOrderBySalesPersonName
    @LastName = 'Hill%';
GO
EXEC dbo.ShoppingCart
    @ShoppingCartID = '20621';
GO
EXEC dbo.ProductBySalesOrder
    @SalesOrderID = 43867;
GO
EXEC dbo.PersonByFirstName
   @FirstName = 'Gretchen';
GO
EXEC dbo.ProductTransactionsSinceDate
    @LatestDate = '9/1/2004',
    @ProductName = 'Hex Nut%';
GO
EXEC dbo.PurchaseOrderBySalesPersonName
    @LastName = 'Hill%',
	@VendorID = 1496;
GO






IF (SELECT  OBJECT_ID('dbo.ExEvents')
   ) IS NOT NULL
    DROP TABLE dbo.ExEvents;
GO
WITH    xEvents
        AS (SELECT    object_name AS xEventName,
                    CAST (event_data AS XML) AS xEventData
            FROM      sys.fn_xe_file_target_read_file('C:\Data\MSSQL11.RANDORI\MSSQL\Log\QueryMetrics*.xel',
                                                    NULL, NULL, NULL)
            )
SELECT  xEventName,
        xEventData.value('(/event/data[@name=''duration'']/value)[1]',
                            'bigint') Duration,
        xEventData.value('(/event/data[@name=''physical_reads'']/value)[1]',
                            'bigint') PhysicalReads,
        xEventData.value('(/event/data[@name=''logical_reads'']/value)[1]',
                            'bigint') LogicalReads,
        xEventData.value('(/event/data[@name=''cpu_time'']/value)[1]',
                            'bigint') CpuTime,
        CASE xEventName
            WHEN 'sql_batch_completed'
            THEN xEventData.value('(/event/data[@name=''batch_text'']/value)[1]',
                                'varchar(max)')
            WHEN 'rpc_completed'
            THEN xEventData.value('(/event/data[@name=''statement'']/value)[1]',
                                'varchar(max)')
        END AS SQLText,
        xEventData.value('(/event/data[@name=''query_plan_hash'']/value)[1]',
                            'binary(8)') QueryPlanHash
INTO    dbo.ExEvents
FROM    xEvents;



SELECT  *
FROM    dbo.ExEvents AS ee
ORDER BY ee.Duration DESC;


SELECT  ee.SQLText,
        SUM(Duration) AS SumDuration,
        AVG(Duration) AS AvgDuration,
        COUNT(Duration) AS CountDuration
FROM    dbo.ExEvents AS ee
GROUP BY ee.SQLText;






DBCC FREEPROCCACHE();
DBCC DROPCLEANBUFFERS;
GO
SET STATISTICS TIME ON;
GO
SET STATISTICS IO ON;
GO
EXEC dbo.PurchaseOrderBySalesPersonName @LastName = 'Hill%';
GO
SET STATISTICS TIME OFF;
GO
SET STATISTICS IO OFF;
GO





DBCC SHOW_STATISTICS('HumanResources.Employee',
'PK_Employee_BusinessEntityID');





SELECT  s.avg_fragmentation_in_percent,
        s.fragment_count,
        s.page_count,
        s.avg_page_space_used_in_percent,
        s.record_count,
        s.avg_record_size_in_bytes,
        s.index_id
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2012'),
                                       OBJECT_ID(N'HumanResources.Employee'),
                                       NULL, NULL, 'Sampled') AS s
WHERE   s.record_count > 0
ORDER BY s.index_id;






SELECT  s.avg_fragmentation_in_percent,
        s.fragment_count,
        s.page_count,
        s.avg_page_space_used_in_percent,
        s.record_count,
        s.avg_record_size_in_bytes,
        s.index_id
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2012'),
                                       OBJECT_ID(N'Purchasing.PurchaseOrderHeader'),
                                       NULL, NULL, 'Sampled') AS s
WHERE   s.record_count > 0
ORDER BY s.index_id;
SELECT  s.avg_fragmentation_in_percent,
        s.fragment_count,
        s.page_count,
        s.avg_page_space_used_in_percent,
        s.record_count,
        s.avg_record_size_in_bytes,
        s.index_id
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2012'),
                                       OBJECT_ID(N'Purchasing.PurchaseOrderDetail'),
                                       NULL, NULL, 'Sampled') AS s
WHERE   s.record_count > 0
ORDER BY s.index_id;
SELECT  s.avg_fragmentation_in_percent,
        s.fragment_count,
        s.page_count,
        s.avg_page_space_used_in_percent,
        s.record_count,
        s.avg_record_size_in_bytes,
        s.index_id
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2012'),
                                       OBJECT_ID(N'Production.Product'),
                                       NULL, NULL, 'Sampled') AS s
WHERE   s.record_count > 0
ORDER BY s.index_id;
SELECT  s.avg_fragmentation_in_percent,
        s.fragment_count,
        s.page_count,
        s.avg_page_space_used_in_percent,
        s.record_count,
        s.avg_record_size_in_bytes,
        s.index_id
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2012'),
                                       OBJECT_ID(N'Person.Person'),
                                       NULL, NULL, 'Sampled') AS s
WHERE   s.record_count > 0
ORDER BY s.index_id;




DECLARE @DBName NVARCHAR(255),
    @TableName NVARCHAR(255),
    @SchemaName NVARCHAR(255),
    @IndexName NVARCHAR(255),
    @PctFrag DECIMAL,
    @Defrag NVARCHAR(MAX)
    
IF EXISTS ( SELECT  *
            FROM    sys.objects
            WHERE   object_id = OBJECT_ID(N'#Frag') )
    DROP TABLE #Frag;
CREATE TABLE #Frag
    (
     DBName NVARCHAR(255),
     TableName NVARCHAR(255),
     SchemaName NVARCHAR(255),
     IndexName NVARCHAR(255),
     AvgFragment DECIMAL
    )
EXEC sys.sp_MSforeachdb 'INSERT INTO #Frag ( DBName, TableName, SchemaName, IndexName, AvgFragment )  SELECT    ''?''  AS DBName ,t.Name AS TableName ,sc.Name AS SchemaName ,i.name AS IndexName ,s.avg_fragmentation_in_percent FROM        ?.sys.dm_db_index_physical_stats(DB_ID(''?''),  NULL,  NULL,
NULL,   ''Sampled'') AS s JOIN ?.sys.indexes i ON s.Object_Id = i.Object_id
AND s.Index_id = i.Index_id JOIN ?.sys.tables t ON i.Object_id = t.Object_Id JOIN ?.sys.schemas sc ON t.schema_id = sc.SCHEMA_ID

WHERE s.avg_fragmentation_in_percent > 20
AND t.TYPE = ''U''
AND s.page_count > 8
ORDER BY TableName,IndexName';

DECLARE cList CURSOR
FOR
    SELECT  *
    FROM    #Frag

OPEN cList;
FETCH NEXT FROM cList
INTO @DBName, @TableName, @SchemaName, @IndexName, @PctFrag;

WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @PctFrag BETWEEN 20.0 AND 40.0
            BEGIN
                SET @Defrag = N'ALTER INDEX ' + @IndexName + ' ON ' + @DBName +
                    '.' + @SchemaName + '.' + @TableName + ' REORGANIZE';
                EXEC sp_executesql @Defrag;
                PRINT 'Reorganize index: ' + @DBName + '.' + @SchemaName + '.' +
                    @TableName + '.' + @IndexName;
            END
        ELSE
            IF @PctFrag > 40.0
                BEGIN
                    SET @Defrag = N'ALTER INDEX ' + @IndexName + ' ON ' +
                        @DBName + '.' + @SchemaName + '.' + @TableName +
                        ' REBUILD';
                    EXEC sp_executesql @Defrag;
                    PRINT 'Rebuild index: ' + @DBName + '.' + @SchemaName +
                        '.' + @TableName + '.' + @IndexName;
                END
        FETCH NEXT FROM cList
INTO @DBName, @TableName, @SchemaName, @IndexName, @PctFrag;
    END
CLOSE cList;
DEALLOCATE cList;
DROP TABLE #Frag;
GO







ALTER PROCEDURE dbo.PurchaseOrderBySalesPersonName
    @LastName NVARCHAR(50),
    @VendorID INT = NULL
AS
IF @VendorID IS NULL
BEGIN
SELECT  poh.PurchaseOrderID,
        poh.OrderDate,
        pod.LineTotal,
        p.[Name] AS ProductName,
        e.JobTitle,
        per.LastName + ', ' + per.FirstName AS SalesPerson,
		poh.VendorID
FROM    Purchasing.PurchaseOrderHeader AS poh
        JOIN Purchasing.PurchaseOrderDetail AS pod
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
        JOIN Production.Product AS p
        ON pod.ProductID = p.ProductID
        JOIN HumanResources.Employee AS e
        ON poh.EmployeeID = e.BusinessEntityID
        JOIN Person.Person AS per
        ON e.BusinessEntityID = per.BusinessEntityID
WHERE   per.LastName LIKE @LastName 
ORDER BY per.LastName,
        per.FirstName; 
END
ELSE
BEGIN
SELECT  poh.PurchaseOrderID,
        poh.OrderDate,
        pod.LineTotal,
        p.[Name] AS ProductName,
        e.JobTitle,
        per.LastName + ', ' + per.FirstName AS SalesPerson,
		poh.VendorID
FROM    Purchasing.PurchaseOrderHeader AS poh
        JOIN Purchasing.PurchaseOrderDetail AS pod
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
        JOIN Production.Product AS p
        ON pod.ProductID = p.ProductID
        JOIN HumanResources.Employee AS e
        ON poh.EmployeeID = e.BusinessEntityID
        JOIN Person.Person AS per
        ON e.BusinessEntityID = per.BusinessEntityID
WHERE   per.LastName LIKE @LastName AND
        poh.VendorID = @VendorID
ORDER BY per.LastName,
        per.FirstName; 
END
GO


EXEC dbo.PurchaseOrderBySalesPersonName
    @LastName = 'Hill%',
	@VendorID = 1496;




CREATE NONCLUSTERED INDEX IX_PurchaseOrderHeader_VendorID ON Purchasing.PurchaseOrderHeader
(
VendorID ASC
)
INCLUDE(OrderDate,EmployeeID,PurchaseOrderID)
WITH DROP_EXISTING;
GO

CREATE NONCLUSTERED INDEX [IX_PurchaseOrderHeader_EmployeeID] ON [Purchasing].[PurchaseOrderHeader]
(
	[EmployeeID] ASC
)
INCLUDE (VendorID, OrderDate)
WITH DROP_EXISTING;



DBCC FREEPROCCACHE();
DBCC DROPCLEANBUFFERS;
GO
SET STATISTICS TIME ON;
GO
SET STATISTICS IO ON;
GO
EXEC dbo.PurchaseOrderBySalesPersonName @LastName = 'Hill%',
	@VendorID = 1496;
GO
SET STATISTICS TIME OFF;
GO
SET STATISTICS IO OFF;
GO
DBCC FREEPROCCACHE();
DBCC DROPCLEANBUFFERS;
GO
SET STATISTICS TIME ON;
GO
SET STATISTICS IO ON;
GO
EXEC dbo.PurchaseOrderBySalesPersonName @LastName = 'Hill%';
GO
SET STATISTICS TIME OFF;
GO
SET STATISTICS IO OFF;
GO


CREATE NONCLUSTERED INDEX IX_PurchaseOrderHeader_VendorID ON Purchasing.PurchaseOrderHeader
(
VendorID ASC
)
INCLUDE(orderDate)
WITH DROP_EXISTING;
GO



ALTER PROCEDURE dbo.PurchaseOrderBySalesPersonName
    @LastName NVARCHAR(50),
    @VendorID INT = NULL
AS
    IF @VendorID IS NULL
        BEGIN
            EXEC dbo.PurchaseOrderByLastName @LastName
        END
    ELSE
        BEGIN
            EXEC dbo.PurchaseOrderByLastNameVendor @LastName, @VendorID
        END
GO


CREATE PROCEDURE dbo.PurchaseOrderByLastName @LastName NVARCHAR(50)
AS
    SELECT  poh.PurchaseOrderID,
            poh.OrderDate,
            pod.LineTotal,
            p.[Name] AS ProductName,
            e.JobTitle,
            per.LastName + ', ' + per.FirstName AS SalesPerson,
            poh.VendorID
    FROM    Purchasing.PurchaseOrderHeader AS poh
            JOIN Purchasing.PurchaseOrderDetail AS pod
            ON poh.PurchaseOrderID = pod.PurchaseOrderID
            JOIN Production.Product AS p
            ON pod.ProductID = p.ProductID
            JOIN HumanResources.Employee AS e
            ON poh.EmployeeID = e.BusinessEntityID
            JOIN Person.Person AS per
            ON e.BusinessEntityID = per.BusinessEntityID
    WHERE   per.LastName LIKE @LastName
    ORDER BY per.LastName,
            per.FirstName; 
GO

CREATE PROCEDURE dbo.PurchaseOrderByLastNameVendor
    @LastName NVARCHAR(50),
    @VendorID INT
AS
    SELECT  poh.PurchaseOrderID,
            poh.OrderDate,
            pod.LineTotal,
            p.[Name] AS ProductName,
            e.JobTitle,
            per.LastName + ', ' + per.FirstName AS SalesPerson,
            poh.VendorID
    FROM    Purchasing.PurchaseOrderHeader AS poh
            JOIN Purchasing.PurchaseOrderDetail AS pod
            ON poh.PurchaseOrderID = pod.PurchaseOrderID
            JOIN Production.Product AS p
            ON pod.ProductID = p.ProductID
            JOIN HumanResources.Employee AS e
            ON poh.EmployeeID = e.BusinessEntityID
            JOIN Person.Person AS per
            ON e.BusinessEntityID = per.BusinessEntityID
    WHERE   per.LastName LIKE @LastName AND
            poh.VendorID = @VendorID
    ORDER BY per.LastName,
            per.FirstName; 
GO




EXEC dbo.PurchaseOrderBySalesPersonName
    @LastName = 'Hill%';
GO
EXEC dbo.ShoppingCart
    @ShoppingCartID = '20621';
GO
EXEC dbo.ProductBySalesOrder
    @SalesOrderID = 43867;
GO
EXEC dbo.PersonByFirstName
   @FirstName = 'Gretchen';
GO
EXEC dbo.ProductTransactionsSinceDate
    @LatestDate = '9/1/2004',
    @ProductName = 'Hex Nut%';
GO
EXEC dbo.PurchaseOrderBySalesPersonName
    @LastName = 'Hill%',
	@VendorID = 1496;
GO





--errors
INSERT  INTO Purchasing.PurchaseOrderDetail
        (PurchaseOrderID,
         DueDate,
         OrderQty,
         ProductID,
         UnitPrice,
         ReceivedQty,
         RejectedQty,
         ModifiedDate
        )
VALUES  (1066,
         '1/1/2009',
         1,
         42,
         98.6,
         5,
         4,
         '1/1/2009'
        ) ;
GO

SELECT  p.[Name],
        psc.[Name]
FROM    Production.Product AS p,
        Production.ProductSubCategory AS psc ; 
GO







--Chapter 26
CREATE    NONCLUSTERED INDEX [AK_Product_Name]
ON [Production].[Product]  ([Name] ASC) WITH  (
DROP_EXISTING = ON)
ON    [PRIMARY];
GO



SELECT DISTINCT
        (p.[Name])
FROM    Production.Product AS p;



CREATE UNIQUE NONCLUSTERED INDEX [AK_Product_Name] 
ON [Production].[Product]([Name] ASC) 
WITH (
DROP_EXISTING = ON)
ON    [PRIMARY];
GO








--Create two test tables
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL
    DROP TABLE dbo.Test1;
GO
CREATE TABLE dbo.Test1
    (
     C1 INT,
     C2 INT CHECK (C2 BETWEEN 10 AND 20)
    );
INSERT  INTO dbo.Test1
VALUES  (11, 12);
GO
IF (SELECT  OBJECT_ID('dbo.Test2')
   ) IS NOT NULL
    DROP TABLE dbo.Test2;
GO
CREATE TABLE dbo.Test2 (C1 INT, C2 INT);
INSERT  INTO dbo.Test2
VALUES  (101, 102);





SELECT  T1.C1,
        T1.C2,
        T2.C2
FROM    dbo.Test1 AS T1
JOIN    dbo.Test2 AS T2
        ON T1.C1 = T2.C2 AND
           T1.C2 = 20;
GO
SELECT  T1.C1,
        T1.C2,
        T2.C2
FROM    dbo.Test1 AS T1
JOIN    dbo.Test2 AS T2
        ON T1.C1 = T2.C2 AND
           T1.C2 = 30;





SELECT  p.*
FROM    Production.Product AS p
WHERE   p.[Name] LIKE '%Caps';






SELECT  soh.SalesOrderNumber
FROM    Sales.SalesOrderHeader AS soh
WHERE   'SO5' = LEFT(SalesOrderNumber, 3);

SELECT  soh.SalesOrderNumber
FROM    Sales.SalesOrderHeader AS soh
WHERE   SalesOrderNumber LIKE 'SO5%';







