
create  PROC LNK_MGR_NORTHWIND (@ls_name VARCHAR(90),@FULL_LOAD BIT = 0) -- 0 = only change tracking , 1 is full load
AS

DECLARE @IID VARCHAR(9) ,@SCH_NAME VARCHAR(256),@TBL_NAME VARCHAR(256),@CT_MIN_VALID_VER VARCHAR(9),@CT_FETCH_VER VARCHAR(9),@CT_EXEC_VER VARCHAR(9),@IDENT_COL VARCHAR(256),@IDENT_VAL VARCHAR(256),@SEL_COL_LIST  VARCHAR(MAX),@CT_JOIN VARCHAR(MAX),@vLog varchar(max),@active bit


-- SET COLUMN ACTIE = 0 when dest table doesnt exist
UPDATE AUTO_CT_TRACKING_NORTHWIND  SET ACTIVE = 0 FROM AUTO_CT_TRACKING_NORTHWIND X left outer JOIN NORTHWIND.SYS.TABLES Y ON X.SCH_NAME =  QUOTENAME(SCHEMA_NAME(Y.SCHEMA_ID)) AND X.TBL_NAME = QUOTENAME(Y.NAME) where Y.NAME IS NULL
 
 
DECLARE @EXEC VARCHAR(MAX) = ''

if @FULL_LOAD = 0
	BEGIN
			DECLARE CUR_MGR CURSOR FOR SELECT * FROM AUTO_CT_TRACKING_NORTHWIND where ACTIVE = 1 AND CT_MIN_VALID_VER IS NOT NULL -- GET ONLY CHANGE TRACKING TABLES
	END
  ELSE 
	BEGIN
			DECLARE CUR_MGR CURSOR FOR SELECT * FROM AUTO_CT_TRACKING_NORTHWIND where ACTIVE = 1     --- GET FULL LOAD
	END


	
OPEN CUR_MGR
		FETCH NEXT FROM CUR_MGR INTO @IID  ,@SCH_NAME ,@TBL_NAME  ,@CT_MIN_VALID_VER,@CT_FETCH_VER,@CT_EXEC_VER,@IDENT_COL,@IDENT_VAL,@SEL_COL_LIST ,@CT_JOIN,@vLog ,@active

WHILE @@FETCH_STATUS = 0
BEGIN 
set @EXEC = ''  -- to execute one table at a time
 
-- UPDATE PRE VER AND POST VER
		EXEC (' DECLARE @I INT ; SELECT   @I = CCV FROM OPENQUERY('+@ls_name+',''SELECT CHANGE_TRACKING_CURRENT_VERSION () as ccv'');
			UPDATE AUTO_CT_TRACKING_NORTHWIND SET CT_FETCH_VER  = @I WHERE IID = '+@IID+' 
			')
---------------------------



		IF @FULL_LOAD =0 AND @CT_MIN_VALID_VER IS NOT NULL 
		

			BEGIN
		 
					SET @EXEC= '
					DELETE FROM '+@SCH_NAME+'.'+ @TBL_NAME + ' FROM '+@SCH_NAME+'.'+ @TBL_NAME + ' X 
					JOIN ( SELECT * FROM OPENQUERY('+@ls_name+',''select X.' +replace(@SEL_COL_LIST ,', ',',X.')+' FROM '+@SCH_NAME+'.'+ @TBL_NAME+' X  JOIN CHANGETABLE(CHANGES '+@SCH_NAME+'.'+ @TBL_NAME +','+ @CT_EXEC_VER +') AS Y ON X' +@CT_JOIN +''')) Y ON X'+@CT_JOIN+';
					
			
					
					UPDATE AUTO_CT_TRACKING_NORTHWIND SET vLog = vLog+'',(''+cast(@@rowcount as varchar(9)) + '',''''U'''''' where IID ='+ @IID
					 
					SET @EXEC= @EXEC+'
					INSERT INTO '+@SCH_NAME+'.'+ @TBL_NAME +'('+@SEL_COL_LIST+')'+
					' SELECT * FROM OPENQUERY('+@ls_name+',''select X.' +replace(@SEL_COL_LIST ,', ',',X.')+' FROM '+@SCH_NAME+'.'+ @TBL_NAME+' X  JOIN CHANGETABLE(CHANGES '+@SCH_NAME+'.'+ @TBL_NAME +','+ @CT_EXEC_VER +') AS Y ON X' +@CT_JOIN +''')
					UPDATE AUTO_CT_TRACKING_NORTHWIND SET vLog = vLog + cast(@@rowcount as varchar(9)) + '',''''U'''',''+ cast(getdate() as varchar(9)) +'')'' where IID ='+ @IID
			
			END		
						
			
			 			
		IF @FULL_LOAD =1 	
			BEGIN
					SET @EXEC= '


					INSERT INTO '+@SCH_NAME+'.'+ @TBL_NAME +'('+@SEL_COL_LIST+')'+
					'SELECT * FROM OPENQUERY('+@ls_name+',''select X.' +replace(@SEL_COL_LIST ,', ',',X.')+' FROM '+@SCH_NAME+'.'+ @TBL_NAME+' X '');'

					
			END
 
 

			-- if the table has identity column 
		IF @IDENT_COL IS NOT NULL
					BEGIN

							SET @EXEC = 'SET IDENTITY_INSERT '+ @SCH_NAME+'.'+ @TBL_NAME  +' ON
										'+@EXEC 

										+'SET IDENTITY_INSERT '+ @SCH_NAME+'.'+ @TBL_NAME  +' OFF

							
							SELECT setval(pg_get_serial_sequence('+  '''"'+DB_NAME() +'_'+replace(replace(@SCH_NAME ,'[',''),']','"')+'.'+ replace(replace(lower(@TBL_NAME ),'[','"'),']','"')+ ''','''+replace(replace(LOWER(@IDENT_COL) ,'[',''),']','')+'''' + '),'+@IDENT_VAL+')' -- [] doesnt work with spaces for postgres function , desired output -- SELECT setval(pg_get_serial_sequence('"northwind_dbo"."employees"','employeeid'),9)
			
					

					END

						print  (@EXEC  )
						
						exec (@EXEC  )
						
 
		 
			 UPDATE AUTO_CT_TRACKING_NORTHWIND SET CT_EXEC_VER  = CT_FETCH_VER WHERE IID = @IID  -- IF BOTH COLUMNS ARE EQUAL THEN THE FETCHED VERSION IS COMPLETED AND UPDATED AT EXEC VER
					
									   
									  
		FETCH NEXT FROM CUR_MGR INTO @IID  ,@SCH_NAME ,@TBL_NAME  ,@CT_MIN_VALID_VER,@CT_FETCH_VER,@CT_EXEC_VER,@IDENT_COL,@IDENT_VAL,@SEL_COL_LIST ,@CT_JOIN,@vLog ,@active

END
CLOSE CUR_MGR
DEALLOCATE CUR_MGR

GO
