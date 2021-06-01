EXECUTE sp_execute_external_script
@language = N'Python',
@script = N'

print("Hello, World!")';


go
DECLARE @Input_Query NVARCHAR(MAX) = N'SELECT Location, ShortName from [PetVet].[dbo].[DEF_Location]';
EXEC sp_execute_external_script @language = N'Python',
@script = N'
print(Input_Data.head(5))', 
@input_data_1 = @Input_Query,
@input_data_1_name = N'Input_Data',
@output_data_1_name =N'Output_Data';
--WITH RESULT SETS ((ProductId INT, ProductName VARCHAR(100), Price MONEY);

import pyodbc 
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DB05.petvetcarecenters.com\BETA,1435;'
                      'Database=[PetVet].[dbo].[DEF_Location];'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

cursor.execute('''

               CREATE TABLE People
               (
               Name nvarchar(50),
               Age int,
               City nvarchar(50)
               )

               ''')

conn.commit()


---------
DECLARE @Input_Query NVARCHAR(MAX) = N'SELECT Location, ShortName from [PetVet].[dbo].[DEF_Location]';
EXEC sp_execute_external_script @language = N'Python',
@script = N'
print(Input_Data.head(5))', 
@input_data_1 = @Input_Query,
@input_data_1_name = N'Input_Data',
@output_data_1_name =N'Output_Data';
-----------


EXEC sp_execute_external_script @language = N'Python',
@script = N'
print(Input_Data.head(5))', 
@input_data_1 = N'SELECT Location, ShortName from [PetVet].[dbo].[DEF_Location]',
@input_data_1_name = N'Input_Data',
@output_data_1_name =N'Output_Data';
---------------

----
--- DF OUTPUT TESTING
DECLARE @Input_Query NVARCHAR(MAX) = N'SELECT Location, ShortName from [PetVet].[dbo].[DEF_Location]';
EXEC sp_execute_external_script @language = N'Python',
@script = N'
import pyodbc 
conn = pyodbc.connect("Driver={SQL Server};"
                      "Server=DB05.petvetcarecenters.com\BETA,1435;"
                      "Database=[PetVet].[dbo].[DEF_Location];"
                      "Trusted_Connection=yes;")

cursor = conn.cursor()

cursor.execute("""SELECT * INTO #tempy FROM (SELECT Location, ShortName from [PetVet].[dbo].[DEF_Location]) A""")

conn.commit()
', 
@input_data_1 = @Input_Query,
@input_data_1_name = N'Input_Data',
@output_data_1_name =N'Output_Data';
----------
---------
SELECT * INTO #tempy FROM (SELECT Location, ShortName from [PetVet].[dbo].[DEF_Location]) A

DROP TABLE #tempy

-----
--------
------
DECLARE @Input_Query NVARCHAR(MAX) = N'select top 5 * from [DB04].[Dayforce].[dbo].[JournalEntryReportWOutDebitandCredit]';
EXEC sp_execute_external_script @language = N'Python',
@script = N'
print(Input_Data.head(5))
', 
@input_data_1 = @Input_Query,
@input_data_1_name = N'Input_Data',
@output_data_1_name =N'Output_Data';
--------
--------------
-------------

EXEC sp_execute_external_script @language = N'Python',
@script = N'
import pyodbc 
conn = pyodbc.connect("Driver={SQL Server};"
                      "Server=DB05.petvetcarecenters.com\BETA,1435;"
                      "Database=PetVet;"
                      "Trusted_Connection=yes;"
                     )

cursor = conn.cursor()
cursor.execute("""SELECT top 5 * from [PetVet].[dbo].[stage_DVMReport_Revenue]""")
data = cursor.fetchall()

records = []
for record in data:
    records.append(list(record))

columns = [i[0] for i in cursor.description]
';, 
@input_data_1 = N'select top 5 * from stage_DVMReport_Revenue',
@input_data_1_name = N'Input_Data',
@output_data_1_name =N'Output_Data';


---------

DECLARE @Input_Query NVARCHAR(MAX) = N'select * from [PetVet].[dbo].[stage_DVMReport_Revenue]';
EXEC sp_execute_external_script @language = N'Python',
@script = N'
import pandas as pd

print(pd.DataFrame(Input_Data))
', 
@input_data_1 = @Input_Query,
@input_data_1_name = N'Input_Data',
@output_data_1_name =N'Output_Data'
with result sets();

---------
----------

DECLARE @Input_Query NVARCHAR(MAX) = N'select * from [PetVet].[dbo].[stage_DVMReport_Revenue]';
EXEC sp_execute_external_script @language = N'Python',
@script = N'
import pandas as pd

import pyodbc 
conn = pyodbc.connect("Driver={SQL Server};"
                      "Server=db04.petvetcarecenters.com;"
                      "Database=Dayforce;"
                      "Trusted_Connection=yes;"
                     )
cursor = conn.cursor()
cursor.execute("""SELECT distinct [ID]
      ,[Pay_Group_Name]
      ,[Register_History_Record_Type_Code_Name]
      ,[Legal_Entity]
      ,[Employee_Name]
      ,[Employee_Number]
      ,[Pay_Type]
      ,[Expense_Type]
      ,[Expense_Code]
      ,cast([Amount] as float)
      ,[Pay_Date]
      ,[Period_Start]
      ,[Period_End]
      ,[Pay_Period]
      ,cast([Hours] as float) 
      ,[PSID]
      ,[Location]
      ,[Position]
      ,[Department]
      ,[Extraction_DT] from [Dayforce].[dbo].[JournalEntryReportWOutDebitandCredit]""")
data = cursor.fetchall()
records = []
for record in data:
    records.append(list(record))
columns = [i[0] for i in cursor.description]
wages = pd.DataFrame(records,columns = columns)
wages.drop_duplicates(inplace=True)

Output_Data = wages
', 
@input_data_1 = @Input_Query,
@input_data_1_name = N'Input_Data',
@output_data_1_name =N'Output_Data';



---------------
--------------
---------
ALTER TABLE [PetVet].[dbo].[stage_DVMReport_Revenue]
   ALTER COLUMN hardcode_wage_perc float
ALTER TABLE [PetVet].[dbo].[stage_DVMReport_Revenue]
   ALTER COLUMN [Target %] float
ALTER TABLE [PetVet].[dbo].[stage_DVMReport_Revenue]   
   ALTER COLUMN BaseSalary float
ALTER TABLE [PetVet].[dbo].[stage_DVMReport_Revenue]   
   ALTER COLUMN BaseRate float
ALTER TABLE [PetVet].[dbo].[stage_DVMReport_Revenue]   
   ALTER COLUMN TargetRev float




