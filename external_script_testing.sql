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


go
DECLARE @Input_Query NVARCHAR(MAX) = N'SELECT Location, ShortName from [PetVet].[dbo].[DEF_Location]';
EXEC sp_execute_external_script @language = N'Python',
@script = N'
print(Input_Data.head(5))', 
@input_data_1 = @Input_Query,
@input_data_1_name = N'Input_Data',
@output_data_1_name =N'Output_Data';





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

SELECT * INTO #tempy FROM (SELECT Location, ShortName from [PetVet].[dbo].[DEF_Location]) A

DROP TABLE #tempy
