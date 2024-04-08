CREATE PROC PR_MGR_LNK_PREPARE_ANCHOR_NORTHWIND (@LS_NAME VARCHAR(90) , @SRC_DB_NAME VARCHAR(90) )
AS

--declare @LS_NAME VARCHAR(90) = 'fdw_ms_sql_web' , @SRC_DB_NAME VARCHAR(90) = 'northwind'

DECLARE @LS_QUERY VARCHAR(MAX)
SET @LS_QUERY = 
  'SELECT QUOTENAME(SCHEMA_NAME(SCHEMA_ID)) AS SCH_NAME,QUOTENAME(T.NAME) AS TBL_NAME , CT.min_valid_version as CT_MIN_VALID_VER ,CT.min_valid_version AS CT_FETCH_VER ,CT.min_valid_version   AS CT_EXEC_VER,QUOTENAME(c.name  ) AS IDENT_COL,ident_current(SCHEMA_NAME(T.SCHEMA_ID)+''''.''''+T.NAME) AS IDENT_VAL,
 
	 STUFF( ( SELECT '''', ['''' + COLUMN_NAME + '''']''''
    FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = T.NAME AND TABLE_SCHEMA = SCHEMA_NAME(SCHEMA_ID) ORDER BY ORDINAL_POSITION FOR XML PATH('''''''') ), 1, 2, '''''''' )   
     AS SEL_COL_LIST


	,   STUFF( ( SELECT '''' X.['''' + COLUMN_NAME + '''']=''''  +''''Y.['''' + COLUMN_NAME + ''''] AND'''' 
    FROM ['+@SRC_DB_NAME+'[INFORMATION_SCHEMA].KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + ''''.'''' + QUOTENAME(CONSTRAINT_NAME)), ''''IsPrimaryKey'''') = 1 
  AND TABLE_NAME = T.NAME AND TABLE_SCHEMA = SCHEMA_NAME(SCHEMA_ID) ORDER BY ORDINAL_POSITION FOR XML PATH('''''''') ), 1, 2, '''''''' )  
  +'''' 1=1'''' as CTE_JOIN ,
  
  NULL AS vLog , 1 as active

from '+@SRC_DB_NAME+'.sys.tables T left outer   join sys.change_tracking_tables CT on T.object_id = CT.object_id left outer  JOIN sys.columns c ON t.object_id=c.object_id and c.is_identity=1'


SET @LS_QUERY =  'INSERT INTO LS_CT_TRACKING_NORTHWIND SELECT * FROM OPENQUERY('+@LS_NAME+','''+@LS_QUERY+''')'

exec  (@LS_QUERY )
