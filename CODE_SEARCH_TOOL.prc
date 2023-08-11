CREATE OR REPLACE PROCEDURE code_search_tool  (
p_search_keyword VARCHAR2,
p_details_flag VARCHAR2 DEFAULT 'N',
p_dependants_flag NUMBER DEFAULT 0,
p_search_objects VARCHAR2 --DEFAULT 'ALL'
) AS
v_keyword VARCHAR2(100) := TRIM(UPPER(p_search_keyword));
v_search_objects VARCHAR2(4000) := UPPER(p_search_objects);
--v_search_objects VARCHAR2(4000) := UPPER('AGENT_TRANSFER_PRCS,ATY_EVENT_DATA_CXT_LOG_INS,AUTO_RELEASE_KITS,UPDATE_PREPAID_KITS');

v_run_flag NUMBER := 1;
v_process_finish NUMBER := 1;
v_search_level NUMBER := 1; 
v_var_search_level NUMBER := 1; 
v_detailed_flag VARCHAR2(1) := p_details_flag; 
v_dependant_flag NUMBER := p_dependants_flag;
v_owner VARCHAR2(100);
v_name VARCHAR2(100);
v_type VARCHAR2(100);
v_variable_line NUMBER ;
v_object_line NUMBER ;
v_exit_flag NUMBER := 5;

v_orig_keyword VARCHAR2(100) := UPPER(p_search_keyword); 

BEGIN

EXECUTE IMMEDIATE 'truncate table code_search_gtt';
EXECUTE IMMEDIATE 'truncate table code_results_gtt';
EXECUTE IMMEDIATE 'truncate table code_comma_gtt';
EXECUTE IMMEDIATE 'truncate table code_results_final_gtt';

--DBMS_OUTPUT.PUT_LINE(' STARTING THE LOOP FOR FIRST KEYWORD' );

WHILE v_run_flag = 1 and v_exit_flag > 0 LOOP
--DBMS_OUTPUT.PUT_LINE(' IN THE LOOP FOR KEYWORD-and v_exist_flag-'||v_keyword ||' '||v_exit_flag);


EXECUTE IMMEDIATE 'truncate table code_search_gtt';
         
EXECUTE IMMEDIATE 'truncate table code_results_gtt';
         
EXECUTE IMMEDIATE 'truncate table code_comma_gtt';         
         
         
INSERT INTO code_search_gtt 
WITH keyword_source AS 
(select * from all_source where upper(text) like '%'||v_keyword||'%' )
select * from (
select  s2.owner, s2.type, s2.name, s2.line, s2.text,
       --CASE WHEN (dml_line IS NOT NULL or select_line IS NOT NULL or merge_insert_line IS NOT NULL or merge_update_line IS NOT NULL or WHERE_LINE is not null)
       --  THEN s2.end_line ELSE NULL END as end_line,
       s2.end_line,  
       s2.obj_line, s2.comment_start_line_no, s2.comment_end_line_no, 
       s2.dml_line, s2.select_line, s2.where_line, s2.merge_insert_line, s2.merge_update_line, null as keyword_start_line, null as keyword_end_line, obj_dpndnt, 
       s2.lead_text, s2.lag_text , s2.variable_keyword,
       null as prev_iudm_line_no, null as prev_sel_line_no, null as next_end_line, null as next_mrg_ins_line_no, null as next_mrg_updt_line_no,
       null as next_cond_line_no, null as x, null as y--, s2.into_line_no , s2.set_line_no
       /*,
       lag(dml_line) ignore nulls over(partition by s2.name order by s2.line) as prev_dml_line,
       lag(select_line) ignore nulls over(partition by s2.name order by s2.line) as prev_select_line,
       lead(end_line) ignore nulls over(partition by s2.name order by s2.line) as next_end_line ,
       lead(merge_insert_line) ignore nulls over(partition by s2.name order by s2.line) as next_mrg_ins_line ,
       lead(merge_update_line) ignore nulls over(partition by s2.name order by s2.line) as next_mrg_updt_line ,
       lead(where_line) ignore nulls over(partition by s2.name order by s2.line) as next_cond_line */  
 from (
select s1.*,
       CASE WHEN substr(s1.text,1,1) = ' ' THEN   -- this is to exclude the procedure names with INSERT Or UPDATE (should consider DELETE and MERGE too)
           CASE WHEN ( s1.text  LIKE '% INSERT %' OR s1.text  LIKE '% UPDATE %') AND                           
                     ( s1.text NOT LIKE '%BEFORE%' AND 
                       s1.text NOT LIKE '%AFTER%' AND
                       s1.lag_text NOT LIKE '%WHEN%' AND 
                       s1.lag_text NOT LIKE '%MATCHED%' AND 
                       s1.lag_text NOT LIKE '%THEN%' AND
                       s1.lead_text NOT LIKE '%BEFORE%' AND 
                       s1.lead_text NOT LIKE '%AFTER%' ) THEN s1.line 
                WHEN  s1.text  LIKE '% DELETE %' OR s1.text  LIKE '% MERGE %' THEN s1.line END
           ELSE
           CASE WHEN (s1.text  LIKE 'INSERT %' OR s1.text  LIKE 'UPDATE %') AND -- if no space before INSERT AND UPDATE
                     ( s1.text NOT LIKE '%BEFORE%' AND 
                       s1.text NOT LIKE '%AFTER%' AND
                       s1.lag_text NOT LIKE '%WHEN%' AND 
                       s1.lag_text NOT LIKE '%MATCHED%' AND 
                       s1.lag_text NOT LIKE '%THEN%' AND
                       s1.lead_text NOT LIKE '%BEFORE%' AND 
                       s1.lead_text NOT LIKE '%AFTER%' ) THEN s1.line 
                WHEN  s1.text  LIKE 'DELETE %' OR s1.text  LIKE 'MERGE %' THEN s1.line END
        END         
     as DML_LINE,
     CASE WHEN text LIKE '%SELECT %' AND 
               (text LIKE '%;%' OR
                text LIKE '% IS %' OR
                text LIKE '% FOR %' OR                
                text LIKE '% IN %' OR
                text LIKE '% BEGIN %' OR
                lag_text LIKE '%;%' OR 
                lag_text LIKE '% IS %' OR
                lag_text LIKE '% FOR %' OR
                lag_text LIKE '% IN %' OR
                lag_text LIKE '% BEGIN %'
                ) THEN line END as select_line,
    CASE WHEN text LIKE '%WHERE %' THEN line END as where_line, 
    CASE WHEN s1.text LIKE '%INSERT%' AND
              (s1.lag_text LIKE '%THEN%' OR 
               s1.lag_text LIKE '%MATCHED%' OR 
               s1.lag_text LIKE '%WHEN%'
              )  
         THEN line END as merge_insert_line,
    CASE WHEN s1.text LIKE '%UPDATE%' AND
              (s1.lag_text LIKE '%THEN%' OR 
               s1.lag_text LIKE '%MATCHED%' OR 
               s1.lag_text LIKE '%WHEN%'
              )  
         THEN line END as merge_update_line,
    CASE WHEN substr(s1.text,1,1) = ' ' THEN   -- this is to exclude the procedure names with INSERT Or UPDATE (should consider DELETE and MERGE too)
           CASE WHEN ( s1.text  LIKE '% INSERT %' ) AND                           
                     ( s1.text NOT LIKE '%BEFORE%' AND 
                       s1.text NOT LIKE '%AFTER%' AND
                       s1.lag_text NOT LIKE '%WHEN%' AND 
                       s1.lag_text NOT LIKE '%MATCHED%' AND 
                       s1.lag_text NOT LIKE '%THEN%' AND
                       s1.lead_text NOT LIKE '%BEFORE%' AND 
                       s1.lead_text NOT LIKE '%AFTER%' ) THEN
                CASE WHEN 
                SUBSTR(s1.text,-12) = 'INSERT INTO ' THEN
                substr(rtrim(s1.lead_text),1,instr(rtrim(s1.lead_text),' ',1))  -- replace the table with (  
                ELSE 
                SUBSTR(s1.text, INSTR(s1.text,'INSERT INTO')+12,
                       INSTR(s1.text,' ',INSTR(s1.text,'INSERT INTO')+12) 
                        - INSTR(s1.text,'INSERT INTO')+12 
                )
                END
                WHEN  s1.text  LIKE '% UPDATE %' AND                           
                     ( s1.text NOT LIKE '%BEFORE%' AND 
                       s1.text NOT LIKE '%AFTER%' AND
                       s1.lag_text NOT LIKE '%WHEN%' AND 
                       s1.lag_text NOT LIKE '%MATCHED%' AND 
                       s1.lag_text NOT LIKE '%THEN%' AND
                       s1.lead_text NOT LIKE '%BEFORE%' AND 
                       s1.lead_text NOT LIKE '%AFTER%' ) THEN
                CASE WHEN 
                SUBSTR(s1.text,-7) = 'UPDATE ' THEN
                substr(rtrim(s1.lead_text),1,instr(rtrim(s1.lead_text),' ',1)) 
                ELSE 
                SUBSTR(s1.text, INSTR(s1.text,'UPDATE')+7,
                       INSTR(s1.text,' ',INSTR(s1.text,'UPDATE')+7) 
                        - INSTR(s1.text,'UPDATE')+7  
                )
                END
                WHEN  s1.text  LIKE '% DELETE %'  THEN 
                CASE WHEN 
                SUBSTR(s1.text,-12) = 'DELETE FROM ' THEN
                substr(rtrim(s1.lead_text),1,instr(rtrim(s1.lead_text),' ',1)) 
                ELSE 
                SUBSTR(s1.text, INSTR(s1.text,'DELETE FROM')+12,
                       INSTR(s1.text,' ',INSTR(s1.text,'DELETE FROM')+12) 
                        - INSTR(s1.text,'DELETE FROM')+12 
                )
                END                
                END
           ELSE
           CASE WHEN ( s1.text  LIKE 'INSERT %' ) AND                           
                     ( s1.text NOT LIKE '%BEFORE%' AND 
                       s1.text NOT LIKE '%AFTER%' AND
                       s1.lag_text NOT LIKE '%WHEN%' AND 
                       s1.lag_text NOT LIKE '%MATCHED%' AND 
                       s1.lag_text NOT LIKE '%THEN%' AND
                       s1.lead_text NOT LIKE '%BEFORE%' AND 
                       s1.lead_text NOT LIKE '%AFTER%' ) THEN
                CASE WHEN 
                SUBSTR(s1.text,-12) = 'INSERT INTO ' THEN
                substr(ltrim(s1.lead_text),1,instr(ltrim(s1.lead_text),' ',1)) 
                ELSE 
                SUBSTR(s1.text, INSTR(s1.text,'INSERT INTO')+12,
                       INSTR(s1.text,' ',INSTR(s1.text,'INSERT INTO')+12) 
                        - INSTR(s1.text,'INSERT INTO')+12 
                )
                END
                WHEN  s1.text  LIKE 'UPDATE %' AND                           
                     ( s1.text NOT LIKE '%BEFORE%' AND 
                       s1.text NOT LIKE '%AFTER%' AND
                       s1.lag_text NOT LIKE '%WHEN%' AND 
                       s1.lag_text NOT LIKE '%MATCHED%' AND 
                       s1.lag_text NOT LIKE '%THEN%' AND
                       s1.lead_text NOT LIKE '%BEFORE%' AND 
                       s1.lead_text NOT LIKE '%AFTER%' ) THEN
                CASE WHEN 
                SUBSTR(s1.text,-7) = 'UPDATE ' THEN
                substr(rtrim(s1.lead_text),1,instr(rtrim(s1.lead_text),' ',1)) 
                ELSE 
                SUBSTR(s1.text, INSTR(s1.text,'UPDATE')+7,
                       INSTR(s1.text,' ',INSTR(s1.text,'UPDATE')+7) 
                        - INSTR(s1.text,'UPDATE')+7  
                )
                END
                WHEN  s1.text  LIKE '% DELETE %'  THEN 
                CASE WHEN 
                SUBSTR(s1.text,-12) = 'DELETE FROM ' THEN
                substr(rtrim(s1.lead_text),1,instr(rtrim(s1.lead_text),' ',1)) 
                ELSE 
                SUBSTR(s1.text, INSTR(s1.text,'DELETE FROM')+12,
                       INSTR(s1.text,' ',INSTR(s1.text,'DELETE FROM')+12) 
                        - INSTR(s1.text,'DELETE FROM')+12 
                )
                END                
                END
        END         
     as OBJ_DPNDNT ,
     CASE WHEN s1.text LIKE '%:=%' THEN 
     nvl(substr(rtrim(substr(s1.text, 1,instr(s1.text,':=')-1)),
        instr(rtrim(substr(s1.text, 1,instr(s1.text,':=')-1)),' ',-1,1) ), lag_text) 
      END as variable_keyword /*,
      CASE WHEN s1.text LIKE '% INTO %' THEN s1.line END as into_line_no,
      CASE WHEN s1.text LIKE '% SET %' THEN s1.line END as set_line_no      */
     /* nvl(comment_start_line,lag(s1.comment_start_line) ignore nulls over(partition by s1.name order by s1.line)) as comment_start_line_no,                        
      nvl(comment_end_line,lead(s1.comment_end_line) ignore nulls over(partition by s1.name order by s1.line)) as comment_end_line_no  
*/
  from (  
select  s0.owner, s0.name,s0.type,s0.line,regexp_replace(replace(upper(s0.text),chr(10),' '),'[[:space:]]{2,}',' ') as text ,
        instr(s0.text,'--') as sngle_cmmnt_pos,
        replace(case when s0.text is not null then upper(lead(s0.text) ignore nulls over(partition by s0.name order by s0.line)) end,chr(10),' ') as lead_text,
        replace(case when s0.text is not null then upper(lag(s0.text) ignore nulls over(partition by s0.name order by s0.line)) end,chr(10),' ') as lag_text,
        case when upper(s0.text) like '%;%'  then s0.line
            else NULL end as end_line,    
       case when upper(s0.text) like '%PROCEDURE%' or upper(s0.text) like '%FUNCTION%'  then line
            else NULL end as obj_line ,
       case when ltrim(s0.text) like '/*%' then s0.line else null end as comment_start_line_no, 
       case when s0.text like '%*/%' then s0.line else null end as comment_end_line_no 
from         
(select s.owner,s.name,s.type,s.line,
        case when trim(s.text) like '--%' and trim(s.text) not like '%*/%' then null 
           else s.text end as text    
 from all_source s
 where exists (select 1 from keyword_source k where s.name = k.name)
  and s.text IS NOT NULL    
  and rtrim(text, chr(10)) is not null
  and owner = 'TBMS_PREPAID'
  and type != 'PACKAGE'
  and name != 'PPD_BOOST_DEALER_CREATE_API'  
  and owner = 'TBMS_PREPAID'
  and (name in (select 
                     upper(regexp_substr(v_search_objects,'[^,]+', 1, level)) AS object_name
                from dual
                connect by regexp_substr(v_search_objects, '[^,]+', 1, level) is not null
               ) OR  v_search_objects = 'ALL' )
 ) s0
  ) s1
  ) s2 
  ) 
  where (dml_line is not null or 
  select_line is not null or 
  where_line is not null or 
  merge_insert_line is not null or 
  merge_update_line is not null or 
  end_line is not null or 
  obj_line is not null or 
  comment_start_line_no is not null or 
  comment_end_line_no is not null or 
  variable_keyword is not null /*or
   into_line_no is not null or 
   set_line_No is not null 
   */)
  ;

commit;

--DBMS_OUTPUT.PUT_LINE(' INSERTED INTO FIRST GTT FOR -'||v_keyword || ' - FOR loop - '||v_exit_flag);


MERGE INTO code_search_gtt cso
USING ( 
select cs.cs_name,cs.cs_line_no , cs.cs_owner,
       lag(iudm_line_no) ignore nulls over(partition by cs.cs_name order by cs.cs_line_no) as prev_dml_line,
       lag(sel_line_no) ignore nulls over(partition by cs.cs_name order by cs.cs_line_no) as prev_select_line,
       lead(end_line_no) ignore nulls over(partition by cs.cs_name order by cs.cs_line_no) as next_end_line ,
       lead(mrg_ins_line_no) ignore nulls over(partition by cs.cs_name order by cs.cs_line_no) as next_mrg_ins_line ,
       lead(mrg_updt_line_no) ignore nulls over(partition by cs.cs_name order by cs.cs_line_no) as next_mrg_updt_line ,
       lead(cond_line_no) ignore nulls over(partition by cs.cs_name order by cs.cs_line_no) as next_cond_line 
 from code_search_gtt cs 
 ) csi
 ON (csi.cs_name =cso.cs_name and csi.cs_line_no = cso.cs_line_no and csi.cs_owner = cso.cs_owner )
WHEN MATCHED THEN 
UPDATE SET cso.prev_iudm_line_no = csi.prev_dml_line ,
           cso.prev_sel_line_no = csi.prev_select_line ,
           cso.next_end_line = csi.next_end_line ,
           cso.next_mrg_ins_line_no = csi.next_mrg_ins_line ,
           cso.next_mrg_updt_line_no = csi.next_mrg_updt_line ,
           cso.next_cond_line_no = csi.next_cond_line
           ;
           
commit;           

--DBMS_OUTPUT.PUT_LINE(' MERGED INTO FIRST GTT FOR -'||v_keyword|| ' - FOR loop - '||v_exit_flag);


INSERT INTO CODE_RESULTS_GTT (
cr_owner, cr_type,cr_name, cr_line_no, cr_text,CR_END_LINE_NO ,CR_IUDM_LINE_NO ,CR_SEL_LINE_NO ,CR_COND_LINE_NO,CR_MRG_INS_LINE_NO,
  CR_MRG_UPDT_LINE_NO ,CR_INTO_LINE_NO,CR_SET_LINE_NO,CR_OBJ_DPNDNT,CR_OBJ_COLUMN,CR_VARIABLE_KEYWORD,CR_KEYWORD_START_LINE,
  CR_KEYWORD_END_LINE,CR_KEYWORD_FLAG,CR_OBJ_LINE_NO ,CR_VAR_PROCESS_FLAG ,CR_OBJ_PROCESS_FLAG ,CR_OBJ_NAME )
SELECT DISTINCT 
       owner,
       TYPE,
       name,
       line,
       text,
       end_line_No,
       min(iudm_line_no) over(partition by owner,type,name, keyword_end_line) as iudm_line_no,
       min(sel_line_no) over(partition by owner,type,name, keyword_end_line) as sel_line_no,
       NULL COND_LINE_NO,
       NULL MRG_INS_LINE_NO,
       NULL MRG_UPDT_LINE_NO,
       NULL INTO_LINE_NO,
       NULL SET_LINE_NO,
       min(obj_dpndnt) over(partition by owner,type,name, keyword_end_line) as obj_dpndnt,                       
       NULL,
       min(variable_keyword) over(partition by owner,type,name, keyword_end_line) as variable_keyword,      
       min(keyword_start_line) over(partition by owner,type,name, keyword_end_line) as keyword_start_line,
       keyword_end_line,
       0    AS keyword_flag,
       min(obj_line_no) over(partition by owner,type,name, keyword_end_line) as obj_line,
       NULL AS var_process_flag,
       NULL AS obj_process_flag,
       min(obj_name) over(partition by owner,type,name, keyword_end_line) as obj_name 
FROM (                 
SELECT DISTINCT 
       a.owner,
       a.TYPE,
       a.name,
       a.line,
       a.text,
       NVL(END_LINE_NO,NEXT_END_LINE) end_line_No,
       IUDM_LINE_NO,
       SEL_LINE_NO,
       NULL COND_LINE_NO,
       NULL MRG_INS_LINE_NO,
       NULL MRG_UPDT_LINE_NO,
       NULL INTO_LINE_NO,
       NULL SET_LINE_NO,
       obj_dpndnt,
       NULL,
       variable_keyword,
       keyword_start_line,
       keyword_end_line,
       0    AS keyword_flag,
       obj_line_no,
       NULL AS var_process_flag,
       NULL AS obj_process_flag,
       CASE WHEN INSTR (LTRIM (obj_name), '(') > 0 THEN
             REPLACE (
                obj_name,
                SUBSTR (LTRIM (obj_name), INSTR (LTRIM (obj_name), '(')),
                '')
            ELSE
             obj_name
       END
          AS obj_name
  FROM (
        SELECT cs_name,
               cs_text,
               cs_line_no,
               END_LINE_NO,
               obj_line_no,
               cs_owner,
               IUDM_LINE_NO,
               SEL_LINE_NO,
               COND_LINE_NO,
               next_end_line,
               MRG_INS_LINE_NO,
               MRG_UPDT_LINE_NO,
               INTO_LINE_NO,
               SET_LINE_NO,
               CASE WHEN INSTR (LTRIM (obj_dpndnt), '(') > 0 THEN
                     REPLACE (
                        obj_dpndnt,
                        SUBSTR (LTRIM (obj_dpndnt),
                                INSTR (LTRIM (obj_dpndnt), '(')),
                        '')
                    ELSE
                     obj_dpndnt
               END
                  AS obj_dpndnt,
               variable_keyword,
               NVL (iudm_line_no, sel_line_no)  AS keyword_start_line,
               NVL (end_line_no, next_end_line) AS keyword_end_line,
               CASE WHEN     next_comment_start_line_no IS NOT NULL
                         AND next_comment_start_line_no <= next_comment_end_line_no
                         AND obj_line_no < next_comment_start_line_no
                         AND obj_line_no IS NOT NULL
                    THEN
                     SUBSTR (LTRIM (cs_text), 1,INSTR (LTRIM (cs_text),' ',1, 2))    
                    WHEN     next_comment_start_line_no IS NULL
                         AND obj_line_no > NVL (next_comment_end_line_no, 0)
                    THEN SUBSTR (LTRIM (cs_text), 1, INSTR (LTRIM (cs_text),' ', 1, 2))                    
                    END
                  AS obj_name
          FROM (SELECT cs.*,
                       NVL ( cs.cmnt_start_line_no,  
                           LAG ( cs.cmnt_start_line_no)
                          IGNORE NULLS
                          OVER (PARTITION BY cs.cs_name
                                ORDER BY cs.cs_line_no))
                          AS prev_start_line_no,
                       NVL (
                          cs.cmnt_start_line_no,
                          LEAD (
                             cs.cmnt_start_line_no)
                          IGNORE NULLS
                          OVER (PARTITION BY cs.cs_name
                                ORDER BY cs.cs_line_no))
                          AS next_comment_start_line_no,
                       NVL (
                          cs.cmnt_end_line_no,
                          LAG (
                             cs.cmnt_end_line_no)
                          IGNORE NULLS
                          OVER (PARTITION BY cs.cs_name
                                ORDER BY cs.cs_line_no))
                          AS prev_comment_end_line_no,
                       NVL (
                          cs.cmnt_end_line_no,
                          LEAD (
                             cs.cmnt_end_line_no)
                          IGNORE NULLS
                          OVER (PARTITION BY cs.cs_name
                                ORDER BY cs.cs_line_no))
                          AS next_comment_end_line_no
                  FROM code_search_gtt cs)) s,
       all_source a
 WHERE     a.name = s.cs_name
       AND a.owner = s.cs_owner
       AND (   a.line BETWEEN keyword_start_line AND keyword_end_line
            OR a.line = s.cs_line_no)       
  and owner = 'TBMS_PREPAID'     
  and type != 'PACKAGE'
            )
-- order by a.name,a.line
;
   
--DBMS_OUTPUT.PUT_LINE(' INSERTED INTO SECOND GTT FOR -'||v_keyword || ' - FOR loop - '||v_exit_flag);

/*
MERGE INTO code_results_gtt cro 
USING 
( SELECT distinct cr_owner, cr_type,cr_name, cr_line_No, min(cr_keyword_start_line) over(partition by cr_owner,cr_type,cr_name, cr_keyword_end_line) as keyword_start_line,
         min(cr_sel_line_no) over(partition by cr_owner,cr_type,cr_name, cr_keyword_end_line) as sel_line_no,
         min(cr_iudm_line_no) over(partition by cr_owner,cr_type,cr_name, cr_keyword_end_line) as iudm_line_no, 
         min(cr_obj_dpndnt) over(partition by cr_owner,cr_type,cr_name, cr_keyword_end_line) as obj_dpndnt,                          
         min(cr_variable_keyword) over(partition by cr_owner,cr_type,cr_name, cr_keyword_end_line) as variable_keyword,        
         min(cr_obj_name) over(partition by cr_owner,cr_type,cr_name, cr_keyword_end_line) as obj_name,   
         min(cr_obj_line_no) over(partition by cr_owner,cr_type,cr_name, cr_keyword_end_line) as obj_line,                                                                                                                
         cr_keyword_end_line
    FROM code_results_gtt  cr    
)
cri 
ON (cro.cr_owner = cri.cr_owner and 
cro.cr_type = cri.cr_type and 
cro.cr_name = cri.cr_name and 
cro.cr_line_no = cri.cr_line_no and 
cro.cr_keyword_end_line = cri.cr_keyword_end_line 
 )
WHEN MATCHED THEN 
UPDATE 
SET 
cro.cr_keyword_start_line = cri.keyword_start_line, 
cro.cr_sel_line_no = cri.sel_line_no,
cro.cr_iudm_line_no = cri.iudm_line_no,
cro.cr_obj_dpndnt = cri.obj_dpndnt,
cro.cr_variable_keyword = cri.variable_keyword,
cro.cr_obj_name = cri.obj_name,
cro.cr_obj_line_no = cri.obj_line
; 
*/

MERGE INTO code_results_gtt cro USING (
select DISTINCT cr_type,cr_owner,cr_name, cr_line_no, 
       case when key_line is not null  then 1 else 0 
       end as flag_updt
from (
SELECT c.*, 
       max(case when upper(cr_text) like '%'||v_keyword||'%'  THEN cr_line_no else null end) over(partition by c.cr_owner,c.cr_type, c.cr_name,cr_keyword_end_line)
        as key_line
 FROM code_results_gtt c 
) 
) cri 
ON (cri.cr_owner = cro.cr_owner and cri.cr_type = cro.cr_type and cri.cr_name = cro.cr_name and cri.cr_line_no = cro.cr_line_No ) 
WHEN MATCHED THEN 
UPDATE SET cr_keyword_flag = flag_updt
;

commit;

--DBMS_OUTPUT.PUT_LINE(' MERGED INTO SECOND GTT FOR -'||v_keyword || ' - FOR loop - '||v_exit_flag);



-- MERGE THE COMMA LINES

/*
MERGE INTO code_results_gtt cro USING (
select DISTINCT cr4.cr_owner, cr4.cr_type,cr4.cr_name, cr4.cr_line_no, next_brac_start ,
next_brac_end ,prev_brac_start ,prev_brac_end ,insert_select_cols, next_text, prev_text,
actual_key_line ,
into_line ,
values_line ,
set_line ,
select_line ,
brac_start_pos ,
brac_end_pos ,        text_mod,   
       min(
           case when text_mod like '% FROM %' THEN
                CASE WHEN next_brac_start IS NOT NULL
                         AND next_brac_start <= next_brac_end
                         AND cr_line_no < next_brac_start
                         AND cr_line_no > select_line           
           then cr_line_no 
             WHEN (instr(UPPER(text_mod),' FROM ',1) NOT BETWEEN  brac_start_pos and brac_end_pos OR 
                           instr(UPPER(text_mod),' FROM ',-1) NOT BETWEEN  brac_start_pos and brac_end_pos 
                           ) THEN 
                           cr_line_no
                    END       
           when text_mod like '% FROM %' 
                AND next_brac_start IS NULL
                AND cr_line_no > NVL (next_brac_end, 0)
                AND cr_line_no > select_line 
            then cr_line_no end       
       )  over(partition by cr_name, cr_obj_dpndnt, cr_keyword_start_line, cr_keyword_end_line)
       as from_line,
       comma_line_mod,
       lag(comma_line_mod) ignore nulls over(partition by cr_name, cr_obj_dpndnt, cr_keyword_start_line, cr_keyword_end_line order by cr_line_no) as prev_comma_line,
       lead(comma_line_mod) ignore nulls over(partition by cr_name, cr_obj_dpndnt, cr_keyword_start_line, cr_keyword_end_line order by cr_line_no) as next_comma_line
from (
select cr3.*,
        CASE  WHEN  (insert_brac_start is not null or values_brac_start is not null or 
                    insert_brac_end is not null or values_brac_end is not null -- or select_brac_start is not null or select_brac_end is not null
                    )
                   AND comma_line is not null  
             THEN cr_line_No         
             WHEN     next_brac_start IS NOT NULL THEN
                     CASE WHEN  cr_line_no NOT BETWEEN prev_brac_start AND next_brac_end
                         AND comma_line IS NOT NULL
                         AND cr_line_no > nvl(select_line,0) 
                         AND prev_brac_start > nvl(select_line,0)
                    THEN
                     cr_line_no    
                     WHEN (instr(UPPER(text_mod),',',1) NOT BETWEEN  brac_start_pos and brac_end_pos OR 
                           instr(UPPER(text_mod),',',-1) NOT BETWEEN  brac_start_pos and brac_end_pos 
                           )  
                         AND next_brac_start = next_brac_end
                         AND comma_line IS NOT NULL
                     THEN 
                      cr_line_no
                     END
                    WHEN     next_brac_start IS NULL
                         AND cr_line_no > NVL (next_brac_end, 0)   -- check this
                       -- AND cr_line_no > nvl(select_line,0) 
                        AND comma_line is not null
                    THEN cr_line_No                  
                    END as comma_line_mod  ,
                    MIN(CASE WHEN select_line - cr_iudm_line_no <= 3 OR values_line - cr_iudm_line_no <= 3 THEN '*' END) 
                    OVER(partition by cr_name, cr_obj_dpndnt, cr_keyword_start_line, cr_keyword_end_line ) as insert_select_cols                                     
 from (
  select cr0.*,                  
        MIN(CASE WHEN VALUES_LINE IS NOT NULL AND cr_line_no <= values_line AND text_mod like '%(%' THEN cr_line_no END)        -- not considering the data in same line ()values()
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as insert_brac_start,
        MIN(CASE WHEN VALUES_LINE IS NOT NULL AND cr_line_no >= values_line AND text_mod like '%(%' THEN cr_line_no END)        -- not considering the data in same line ()values()
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as values_brac_start,
        MAX(CASE WHEN VALUES_LINE IS NOT NULL AND cr_line_no <= values_line AND text_mod like '%)%' THEN cr_line_no END)        -- not considering the data in same line ()values()
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as insert_brac_end,
        MAX(CASE WHEN VALUES_LINE IS NOT NULL AND cr_line_no >= values_line AND text_mod like '%)%' THEN cr_line_no END)        -- not considering the data in same line ()values()
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as values_brac_end,
        MIN(CASE WHEN SELECT_LINE IS NOT NULL AND cr_line_no <= select_line AND text_mod like '%(%' THEN cr_line_no END)        -- not considering the data in same line ()values()
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as select_brac_start,
        MAX(CASE WHEN SELECT_LINE IS NOT NULL AND cr_line_no >= SELECT_LINE AND text_mod like '%)%' THEN cr_line_no END)        -- not considering the data in same line ()values()
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as select_brac_end 
from (                               
select cr2.*,instr(UPPER(text_mod),',',1), instr(UPPER(text_mod),',',-1),
       max(case when text_mod like '% INTO %' then cr_line_no end)  
             over(partition by cr_name, nvl(cr_obj_dpndnt,''), cr_keyword_start_line, cr_keyword_end_line)
       as into_line,  
       max(case when cr_obj_dpndnt IS NOT NULL and (text_mod LIKE '% VALUES %' or text_mod LIKE '% VALUES(%') then cr_line_no end)  
             over(partition by cr_name, cr_obj_dpndnt, cr_keyword_start_line,cr_keyword_end_line ) 
       as values_line  ,
       max(case when cr_obj_dpndnt IS NOT NULL and text_mod like '% SET %' then cr_line_no end)  over(partition by cr_name, cr_obj_dpndnt, cr_keyword_start_line, cr_keyword_end_line)
       as set_line,
       min(case when text_mod like '%SELECT %' AND (text_mod not like '%*%' and ltrim(next_text) not like '*%') then cr_line_no end)  over(partition by cr_name, nvl(cr_obj_dpndnt,''), cr_keyword_start_line, cr_keyword_end_line)
       as select_line ,       
        instr(text_mod, '(',1) as brac_start_pos,
        instr(text_mod, ')',-1) as brac_end_pos
  from (
  select cr1.*,       
       lead(text_mod) ignore nulls over(partition by cr_owner, cr_name order by cr_line_no) as next_text,
       lag(text_mod) ignore nulls over(partition by cr_owner, cr_name order by cr_line_no) as prev_text
 from (     
select DISTINCT cr.*,
       ' ' ||regexp_replace(replace(upper(case when trim(cr.cr_text) like '--%' and trim(cr.cr_text) not like '%*//*%' then null 
                                    else cr.cr_text end),chr(10),' '),'[[:space:]]{2,}',' ')
         as text_mod  ,       
        nvl(case when cr_text like '%(%' then cr_line_no end, 
            lead(case when cr_text like '%(%' then cr_line_no end) ignore nulls over(partition by cr_name , cr_keyword_start_line, cr_keyword_end_line order by cr_line_no)) 
         as next_brac_start, 
        nvl(case when cr_text like '%)%' then cr_line_no end,
            lead(case when cr_text like '%)%' then cr_line_no end) ignore nulls over(partition by cr_name , cr_keyword_start_line, cr_keyword_end_line order by cr_line_no)) 
         as next_brac_end, 
        nvl(case when cr_text like '%(%' then cr_line_no end, 
            lead(case when cr_text like '%(%' then cr_line_no end) ignore nulls over(partition by cr_name , cr_keyword_start_line, cr_keyword_end_line order by cr_line_no)) 
         as prev_brac_start, 
        nvl(case when cr_text like '%)%' then cr_line_no end,
            lead(case when cr_text like '%)%' then cr_line_no end) ignore nulls over(partition by cr_name , cr_keyword_start_line, cr_keyword_end_line order by cr_line_no)) 
         as prev_brac_end,  
        case when cr_text LIKE '%CUST%' then cr_line_no end as actual_key_line,
        case when cr_text like '%,%' then cr_line_no end as comma_line
 from code_results_gtt cr
 where cr.cr_text IS NOT NULL 
  and rtrim(cr_text, chr(10)) is not null
and cr_keyword_flag = 1 
and cr_type != 'PACKAGE'
and cr_keyword_start_line is not null
) cr1
) cr2
) cr0
) cr3
) cr4
) cri 
ON (cro.cr_owner = cri.cr_owner and 
    cro.cr_type = cri.cr_type and 
    cro.cr_name = cri.cr_name and 
    cro.cr_line_no = cri.cr_line_no   )
WHEN MATCHED THEN 
UPDATE SET 
 cro.cr_next_brac_start = cri.next_brac_start ,
 cro.cr_next_brac_end = cri.next_brac_end ,
 cro.cr_key_line = cri.actual_key_line ,
 cro.cr_comma_line = cri.comma_line_mod ,
 cro.cr_into_line_no = cri.into_line ,
 cro.cr_values_line = cri.values_line ,
 cro.cr_set_line_no = cri.set_line ,
 cro.cr_select_line = cri.select_line ,
 cro.cr_brac_start_pos = cri.brac_start_pos ,
 cro.cr_brac_end_pos = cri.brac_end_pos ,
 cro.cr_from_line = cri.from_line ,
 cro.cr_prev_comma_line = cri.prev_comma_line ,
 cro.cr_next_comma_line = cri.next_comma_line,
 cro.cr_prev_brac_start = cri.prev_brac_start ,
 cro.cr_prev_brac_end = cri.prev_brac_end ,
 cro.cr_text = cri.text_mod,
 cro.cr_insert_select_cols  = cri.insert_select_cols , 
 cro.cr_next_text = cri.next_text ,
 cro.cr_prev_text = cri.prev_text 
 ;
 */
 
 MERGE INTO code_results_gtt cro USING (
SELECT cr_name, cr_type,cr_owner, cr_line_no,text_mod,text_brac_mod,text_comma_mod,/*cr_iudm_line_no,cr_keyword_start_line,cr_keyword_end_line,from_line,
--       actual_key_line, comma_line, */lag_brac_flag,brac_updt_flag,actual_key_line,insert_select_cols,
       brac_start_line_new2,brac_end_line_new, next_brac_start_new, next_brac_end_new2,next_brac_end_new,prev_brac_start_new, prev_brac_end_new, brac_start_pos_new, brac_end_pos_new,
       into_line, values_line, set_line, select_line, where_line, insert_brac_start, values_brac_start, insert_brac_end, values_brac_end, select_brac_start,select_brac_end,
       cr_keyword_start_line, cr_keyword_end_line , from_line
  FROM (  -- TEST 
SELECT cr9.*,
       CASE WHEN regexp_count(text_brac_mod,'\(') =
                        regexp_count(text_brac_mod,'\)') THEN 
                 CASE WHEN cr_line_no BETWEEN brac_start_line_new2 AND next_brac_end_new OR
                 lag_brac_flag IN (1)
                 THEN        
                 replace(text_brac_mod, ',','')
                 ELSE
                 replace(text_brac_mod,
                                    SUBSTR(  text_brac_mod ,brac_start_pos_new,brac_end_pos_new-brac_start_pos_new+1),
                                    regexp_replace(SUBSTR(  text_brac_mod,brac_start_pos_new,brac_end_pos_new-brac_start_pos_new+1),',','') 
                                   )
                 END                           
            WHEN lag_brac_flag != 2 THEN 
               CASE WHEN  cr_line_no BETWEEN brac_start_line_new2 AND next_brac_end_new 
            THEN 
                 replace(text_brac_mod, ',','')
             END    
            ELSE text_brac_mod END as text_comma_mod
            /*     CASE WHEN 
                        regexp_count(text_brac_mod,'\(') >
                        regexp_count(text_brac_mod,'\)')
                      THEN 
                        replace(text_brac_mod,
                                       SUBSTR(  text_brac_mod ,brac_start_pos_new),
                                       regexp_replace(SUBSTR(  text_brac_mod ,brac_start_pos_new),',','') 
                                      )
                      WHEN 
                        regexp_count(text_brac_mod,'\(') <
                        regexp_count(text_brac_mod,'\)') THEN
                        replace(text_brac_mod,
                                       SUBSTR(  text_brac_mod ,1,brac_end_pos_new),
                                       regexp_replace(SUBSTR(  text_brac_mod,1 ,brac_end_pos_new),',','') 
                                      )
                     WHEN regexp_count(text_brac_mod,'\(') =
                        regexp_count(text_brac_mod,'\)') THEN                        
                        CASE WHEN prev_brac_start_new > prev_brac_end_new AND 
                                cr_line_no BETWEEN prev_brac_start_new AND next_brac_end_new AND
                                next_brac_start_new > next_brac_end_new
                           THEN 
                                REPLACE(text_brac_mod,',','HI THEREEEEEEEEE')
                           ELSE 
                            replace(text_brac_mod,
                                    SUBSTR(  text_brac_mod ,brac_start_pos_new,brac_end_pos_new-brac_start_pos_new+1),
                                    regexp_replace(SUBSTR(  text_brac_mod,brac_start_pos_new,brac_end_pos_new-brac_start_pos_new+1),',','') 
                                   )
                        END          
                     WHEN  prev_brac_start_new > prev_brac_end_new AND 
                           cr_line_no BETWEEN prev_brac_start_new AND next_brac_end_new AND
                           next_brac_start_new > next_brac_end_new
                     THEN 
                                REPLACE(text_brac_mod,',','HI THEREEEEEEEEE')  
                     ELSE text_brac_mod                                            
                     END as text_comma_mod */       
  FROM ( 
SELECT cr8.*,       
       lag(case when brac_updt_flag = 0 then null else brac_updt_flag end) ignore nulls
       over(partition by    cr_name , cr_keyword_start_line, cr_keyword_end_line order by cr_line_no) as lag_brac_flag,       
       CASE WHEN brac_updt_flag = 2 THEN prev_brac_start_new ELSE nvl(brac_start_line_new, prev_brac_start_new)
        END as brac_start_line_new2 ,
       CASE WHEN brac_updt_flag = 2 THEN brac_end_line_new  ELSE next_brac_end_new
        END as next_brac_end_new2    
  FROM (  
SELECT cr7.*,
        case when brac_updt_flag > 0  AND text_brac_mod like '%(%' then cr_line_no end as brac_start_line_new, 
        case when brac_updt_flag > 0  AND text_brac_mod like '%)%' then cr_line_no end as brac_end_line_new,
        lead(case when brac_updt_flag > 0 AND text_brac_mod like '%(%' then cr_line_no end) 
        ignore nulls over(partition by cr_name , cr_keyword_start_line, cr_keyword_end_line order by cr_line_no)
         as next_brac_start_new, 
        lead(case when brac_updt_flag > 0  AND text_brac_mod like '%)%' then cr_line_no end) ignore nulls over(partition by cr_name , cr_keyword_start_line, cr_keyword_end_line order by cr_line_no) 
         as next_brac_end_new, 
        nvl(case when brac_updt_flag > 0  AND text_brac_mod like '%(%' then cr_line_no end,
        lag(case when brac_updt_flag > 0 AND text_brac_mod like '%(%' then cr_line_no end) ignore nulls over(partition by cr_name , cr_keyword_start_line, cr_keyword_end_line order by cr_line_no)) 
         as prev_brac_start_new, 
        lag(case when brac_updt_flag > 0 AND text_brac_mod like '%)%' then cr_line_no end) ignore nulls over(partition by cr_name , cr_keyword_start_line, cr_keyword_end_line order by cr_line_no) 
         as prev_brac_end_new,        
        instr(text_brac_mod, '(',1) as brac_start_pos_new,
        instr(text_brac_mod, ')',-1) as brac_end_pos_new                         
  FROM(        
SELECT cr6.*, 
       CASE WHEN regexp_count(text_brac_mod, '\(') = regexp_count(text_brac_mod, '\)') THEN 0 
            WHEN regexp_count(text_brac_mod, '\(') > regexp_count(text_brac_mod, '\)') THEN 1
            WHEN regexp_count(text_brac_mod, '\(') < regexp_count(text_brac_mod, '\)') THEN 2
        END as brac_updt_flag
  FROM (  
SELECT cr5.*,  
       CASE WHEN (nvl(brac_start_line,prev_brac_start) BETWEEN cr_iudm_line_no AND select_line) OR 
                 (cr_line_no = insert_brac_start) OR 
                 (cr_line_no = values_brac_start) OR 
                 (cr_line_no = insert_brac_end) OR
                 (cr_line_no = values_brac_end) THEN 
                 CASE WHEN text_mod LIKE '%(%' THEN 
                           replace(text_mod, '(','{') 
                      WHEN text_mod LIKE '%)%' THEN 
                           replace(text_mod, ')','}') 
                      ELSE text_mod 
                  END 
             ELSE text_mod     
             END as text_brac_mod/*,
       CASE WHEN (select_line IS NOT NULL AND cr_line_no BETWEEN cr_keyword_start_line AND from_line) OR 
                  VALUES_LINE IS NOT NULL  THEN 
                  text_mod 
                  ELSE replace(text_mod,',', 'HI THEREEEE ITS MEEEEEEEE')
                  END as text_comma_mod */
FROM (   
SELECT cr4.*,
       MIN(CASE WHEN VALUES_LINE IS NOT NULL AND cr_line_no <= values_line AND text_mod like '%(%' THEN cr_line_no END)        -- not considering the data in same line ()values()
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as insert_brac_start,
        MIN(CASE WHEN VALUES_LINE IS NOT NULL AND cr_line_no >= values_line AND text_mod like '%(%' THEN cr_line_no END)        -- not considering the data in same line ()values()
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as values_brac_start,
        MAX(CASE WHEN VALUES_LINE IS NOT NULL AND cr_line_no <= values_line AND text_mod like '%)%' THEN cr_line_no END)        -- not considering the data in same line ()values()
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as insert_brac_end,
        MAX(CASE WHEN VALUES_LINE IS NOT NULL AND cr_line_no >= values_line AND text_mod like '%)%' THEN cr_line_no END)        -- not considering the data in same line ()values()
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as values_brac_end,
        MIN(CASE WHEN SELECT_LINE IS NOT NULL AND cr_line_no <= select_line AND text_mod like '%(%' THEN cr_line_no END)        -- not considering the data in same line ()values()
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as select_brac_start,
        MAX(CASE WHEN SELECT_LINE IS NOT NULL AND cr_line_no >= SELECT_LINE AND text_mod like '%)%' THEN cr_line_no END)        -- not considering the data in same line ()values()
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as select_brac_end,
        MIN(CASE WHEN select_line IS NOT NULL AND text_mod LIKE '% FROM %' AND cr_line_no > select_line THEN cr_line_no END) 
        over(partition by cr_name, cr_owner, cr_obj_dpndnt,cr_keyword_start_line,cr_keyword_end_line ) as from_line                                     
  FROM (        
SELECT cr_owner,cr_type,cr_name, cr_line_no,text_mod,cr_obj_dpndnt,cr_iudm_line_no,cr_keyword_start_line,cr_keyword_end_line,
       actual_key_line, comma_line, prev_brac_start,brac_start_line,
       into_line, values_line, set_line, where_Line,
       CASE WHEN where_line IS NOT NULL AND select_line BETWEEN nvl(where_line,0) AND cr_keyword_end_line THEN NULL ELSE select_line END as select_line,
       prev_text, next_text,
                    MIN(CASE WHEN select_line - cr_iudm_line_no <= 3 OR values_line - cr_iudm_line_no <= 3 THEN '*' END) 
                    OVER(partition by cr_name, cr_obj_dpndnt, cr_keyword_start_line, cr_keyword_end_line ) as insert_select_cols 
  FROM (  
SELECT cr2.*,
       max(case when text_mod like '% INTO %' then cr_line_no end)  
             over(partition by cr_name, nvl(cr_obj_dpndnt,''), cr_keyword_start_line, cr_keyword_end_line)
       as into_line,  
       max(case when cr_obj_dpndnt IS NOT NULL and (text_mod LIKE '% VALUES %' or text_mod LIKE '% VALUES(%') then cr_line_no end)  
             over(partition by cr_name, cr_obj_dpndnt, cr_keyword_start_line,cr_keyword_end_line ) 
       as values_line  ,
       max(case when cr_obj_dpndnt IS NOT NULL and text_mod like '% SET %' then cr_line_no end)  over(partition by cr_name, cr_obj_dpndnt, cr_keyword_start_line, cr_keyword_end_line)
       as set_line,
       min(case when text_mod like '%SELECT %' AND (text_mod not like '%*%' and ltrim(next_text) not like '*%') then cr_line_no end)  over(partition by cr_name, nvl(cr_obj_dpndnt,''), cr_keyword_start_line, cr_keyword_end_line)
       as select_line,       
       min(case when text_mod like '%WHERE %'  then cr_line_no end)  over(partition by cr_name, nvl(cr_obj_dpndnt,''), cr_keyword_start_line, cr_keyword_end_line)
       as where_line
  FROM (     
select cr1.*,  
       lead(text_mod) ignore nulls over(partition by cr_owner, cr_name order by cr_line_no) as next_text,
       lag(text_mod) ignore nulls over(partition by cr_owner, cr_name order by cr_line_no) as prev_text  ,         
        case when text_mod LIKE '%'||v_keyword||'%' then cr_line_no end as actual_key_line,  -- add keyword here 
        case when text_mod like '%,%' then cr_line_no end as comma_line,
        lag(case when text_mod like '%(%' then cr_line_no end) ignore nulls over(partition by cr_name , cr_keyword_start_line, cr_keyword_end_line order by cr_line_no) 
         as prev_brac_start,
        case when text_mod like '%(%' then cr_line_no end as brac_start_line
 from (
select DISTINCT cr_owner, cr_type,cr_name, cr_line_no, 
                ' ' ||regexp_replace(replace(upper(case when trim(cr.cr_text) like '--%' and trim(cr.cr_text) not like '%*/%' then null 
                                    else cr.cr_text end),chr(10),' '),'[[:space:]]{2,}',' ') as text_mod  ,
                cr_obj_dpndnt, cr_iudm_line_no,cr_keyword_start_line, cr_keyword_end_line
  from code_results_gtt cr
 where cr.cr_text IS NOT NULL 
  and rtrim(cr_text, chr(10)) is not null
and cr_keyword_flag = 1 
and cr_type != 'PACKAGE'
-- and cr_name in ('POPULATE_HW3_DEVICES', 'AUTO_RELEASE_KITS','CREATE_PREACTIVATED_ACCOUNT')
) cr1
) cr2
) cr3
) cr4
) cr5
) cr6
) cr7
) cr8
) cr9
) -- TEST
) cri 
ON (cro.cr_owner = cri.cr_owner and 
    cro.cr_type = cri.cr_type and 
    cro.cr_name = cri.cr_name and 
    cro.cr_line_no = cri.cr_line_no   )
WHEN MATCHED THEN 
UPDATE SET 
-- cro.cr_next_brac_start = cri.next_brac_start ,
-- cro.cr_next_brac_end = cri.next_brac_end ,
 cro.cr_key_line = cri.actual_key_line ,
 cro.cr_text = cri.text_mod,
-- cro.cr_comma_line = cri.comma_line_mod ,
 cro.cr_into_line_no = cri.into_line ,
 cro.cr_values_line = cri.values_line ,
 cro.cr_set_line_no = cri.set_line ,
 cro.cr_select_line = cri.select_line , 
 cro.CR_COND_LINE_NO = cri.where_line,
/* cro.cr_brac_start_pos = cri.brac_start_pos ,
 cro.cr_brac_end_pos = cri.brac_end_pos , */
 cro.cr_from_line = cri.from_line ,
/* cro.cr_prev_comma_line = cri.prev_comma_line ,
 cro.cr_next_comma_line = cri.next_comma_line,
 cro.cr_prev_brac_start = cri.prev_brac_start ,
 cro.cr_prev_brac_end = cri.prev_brac_end , */
 cro.cr_text_mod = cri.text_comma_mod,
 cro.cr_insert_select_cols  = cri.insert_select_cols 
 ;
 COMMIT;
 
--DBMS_OUTPUT.PUT_LINE ('MERGED INTO RESULTS_GTT FOR THE COMMAS'|| ' - FOR loop - '||v_exit_flag); 
 
INSERT INTO  code_comma_gtt
SELECT DISTINCT cr2.cr_owner, cr_type, cr_name , cr_line_no, cr_keyword_start_line, cr_keyword_end_line, first_half_rn, second_half_rn,cr_key_line , cr_insert_select_cols
  FROM (
SELECT cr1.*,
       CASE WHEN first_half_cols IS NOT NULL THEN 
       row_number() over(partition by cr_owner, cr_name, nvl(cr_obj_dpndnt,''), cr_keyword_start_line, cr_keyword_end_line,first_half_cols order by cr_line_no)
       END as first_half_rn,
       CASE WHEN second_half_cols IS NOT NULL THEN      
       row_number() over(partition by cr_owner, cr_name, nvl(cr_obj_dpndnt,''), cr_keyword_start_line, cr_keyword_end_line,second_half_cols order by cr_line_no) 
       END as second_half_rn       
 FROM (
SELECT cr.*, 
       CASE WHEN cr_line_no BETWEEN cr_keyword_start_line AND cr_select_line-1 AND cr_comma_line_mod IS NOT NULL THEN 1
            WHEN cr_values_line IS NOT NULL AND cr_line_no BETWEEN cr_keyword_start_line AND cr_values_line -1 AND cr_comma_line_mod IS NOT NULL  THEN 1
            WHEN last_line_flag = 1 AND nvl(cr_values_line, cr_select_line) >= cr_line_no THEN 1 
       END as first_half_cols,       
       CASE WHEN cr_line_no BETWEEN cr_select_line AND cr_from_line AND cr_comma_line_mod IS NOT NULL THEN 2            
            WHEN cr_values_line IS NOT NULL AND cr_line_no BETWEEN cr_values_line AND cr_keyword_end_line AND cr_comma_line_mod IS NOT NULL  THEN 2
            WHEN last_line_flag = 1 AND nvl(cr_values_line, cr_select_line) <= cr_line_no THEN 2
       END as second_half_cols,      
       regexp_count(cr_text_mod,',') as comma_count,        
       CASE WHEN cr_key_line = cr_line_no THEN  regexp_count(substr(comma_count_text, 1,instr(comma_count_text, 'CUST')),',')  END as keyword_count_in_line  
  FROM (
SELECT DISTINCT c.*,CASE WHEN cr_text_mod LIKE '%,%' THEN cr_line_no END cr_comma_line_mod,
                case when (CASE WHEN cr_text_mod LIKE '%,%' THEN cr_line_no END)  is not null AND cr_brac_start_pos = 0 and cr_brac_end_pos = 0 then cr_text_mod
                     when (CASE WHEN cr_text_mod LIKE '%,%' THEN cr_line_no END) is not null and (cr_brac_start_pos > 0 or cr_brac_end_pos > 0) then 
                           replace(cr_text_mod,substr(cr_text_mod,  cr_brac_start_pos, decode(cr_brac_end_pos,0,length(cr_text_mod),cr_brac_end_pos)),'')
                     end as comma_count_text  , --- replace only , so that if actual keyword exists between () it wonts be replaced     
                CASE WHEN (
                          ltrim(lead_text) LIKE 'FROM %' OR
                          ltrim(cr_text_mod) LIKE '% FROM%' OR
                          ltrim(lead_text) LIKE '} %' OR 
                          ltrim(cr_text_mod) LIKE '%}%'
                          ) AND  cr_text_mod not like '%,%'
                          THEN 1 END as last_line_flag                                
FROM
(SELECT c.* ,
             lead(cr_text_mod) over(partition by cr_owner, cr_type,cr_name, cr_keyword_start_line, cr_keyword_end_line order by cr_line_no) as lead_text
   FROM code_results_gtt c
WHERE cr_keyword_flag = 1
--and cr_brac_end_pos is not null
-- and cr_name in ('POPULATE_HW3_DEVICES', 'AUTO_RELEASE_KITS','CREATE_PREACTIVATED_ACCOUNT')
--and 
) c
)cr
) cr1
) cr2
ORDER BY cr_name, cr_line_no
;

--DBMS_OUTPUT.PUT_LINE ('INSERTED COMMA INTO CODE_COMMA_GTT'|| ' - FOR loop - '||v_exit_flag); 

COMMIT;

MERGE INTO code_results_gtt cro
USING 
(
  SELECT DISTINCT c.* FROM code_comma_gtt c 
)
cri
ON (cro.cr_owner = cri.cr_owner and 
    cro.cr_type = cri.cr_type and 
    cro.cr_name = cri.cr_name and 
    cro.cr_line_no = cri.cr_line_no  )
WHEN MATCHED THEN 
UPDATE SET 
cro.cr_first_half_cols = cri.first_half_rn ,
cro.cr_second_half_cols = cri.second_half_rn
;

COMMIT;

--DBMS_OUTPUT.PUT_LINE ('MERGED INTO RESULTS_GTT FROM COMMA TABLE'|| ' - FOR loop - '||v_exit_flag); 


MERGE INTO code_results_gtt cro
USING 
(
  SELECT DISTINCT r.cr_owner,r.cr_type, r.cr_name, r.cr_line_no, r.CR_FIRST_HALF_COLS,trim(replace(r.cr_text_mod,',','')) as obj_column
     FROM code_comma_gtt c , code_results_gtt r 
  where c.cr_owner = r.cr_owner and 
    c.cr_type = r.cr_type and 
    c.cr_name = r.cr_name and 
   -- c.cr_line_no = r.cr_line_no and
    c.SECOND_HALF_RN = r.CR_FIRST_HALF_COLS and 
    c.key_line IS NOT NULL 
)
cri
ON (cro.cr_owner = cri.cr_owner and 
    cro.cr_type = cri.cr_type and 
    cro.cr_name = cri.cr_name and 
    cro.cr_line_no = cri.cr_line_no  )
WHEN MATCHED THEN 
UPDATE SET 
cro.cr_obj_column = cri.obj_column 
;

COMMIT; 

--DBMS_OUTPUT.PUT_LINE ('MERGED OBJECT COLUMN NAME '|| ' - FOR loop - '||v_exit_flag); 

UPDATE code_results_gtt set cr_variable_keyword = substr(cr_variable_keyword, 1,instr(cr_variable_keyword,'(')-1)
WHERE cr_variable_keyword LIKE '%(%';

commit;

INSERT INTO CODE_RESULTS_FINAL_GTT 
( CRF_SEARCH_KEYWORD ,
  CRF_SEARCH_LEVEL , 
  CRF_OWNER             ,
  CRF_TYPE             ,
  CRF_NAME             ,
  CRF_LINE_NO          ,
  CRF_KEY_LINE    ,
  CRF_TEXT             ,
  CRF_OBJ_DPNDNT       ,
  CRF_OBJ_COLUMN        ,
  CRF_VARIABLE_KEYWORD  ,
  CRF_KEYWORD_START_LINE  ,
  CRF_KEYWORD_END_LINE    ,
  CRF_OBJ_LINE_NO         ,
  CRF_VAR_PROCESS_FLAG ,
  CRF_OBJ_PROCESS_FLAG ,
  CRF_OBJ_NAME           
  )
 SELECT DISTINCT 
  v_keyword , 
  v_search_level, 
  CR_OWNER             ,
  CR_TYPE             ,
  CR_NAME             ,
  CR_LINE_NO          ,
  cr_key_line,
  CR_TEXT             ,
  CR_OBJ_DPNDNT       ,
  CR_OBJ_COLUMN        ,
  CR_VARIABLE_KEYWORD  ,
  CR_KEYWORD_START_LINE  ,
  CR_KEYWORD_END_LINE    ,
  CR_OBJ_LINE_NO         ,
  CR_VAR_PROCESS_FLAG ,
  CR_OBJ_PROCESS_FLAG ,
  CR_OBJ_NAME   
  FROM code_results_gtt cr 
  WHERE cr_keyword_flag = 1
  AND NOT EXISTS (SELECT 1 FROM code_results_final_gtt cf WHERE cr.cr_owner = cf.crf_owner 
  and cr.cr_type = cf.crf_type and cr.cr_name = cf.crf_name and nvl(cr.cr_variable_keyword,'$') = nvl(cf.crf_variable_keyword,'$')
  and nvl(cr.CR_OBJ_COLUMN,'$') = nvl(cf.CRF_OBJ_COLUMN,'$')  and nvl(cr.CR_OBJ_DPNDNT,'$') = nvl(cf.CRF_OBJ_DPNDNT,'$'))
  ; 
commit;


UPDATE code_results_final_gtt set crf_variable_keyword = substr(crf_variable_keyword, 1,instr(crf_variable_keyword,'(')-1)
WHERE crf_variable_keyword LIKE '%(%';

COMMIT;

MERGE INTO code_results_final_gtt cro USING (
select c.cr_name, c.cr_text ,
       MIN(
       case when cr_key_line IS NOT NULL THEN
            CASE
            when cr_values_line IS NOT NULL AND cr_key_line between cr_values_line and cr_keyword_end_line then 'INSERT FROM KEYWORD'
            when cr_select_line IS NOT NULL AND cr_iudm_line_no IS NOT NULL AND 
                 cr_key_line between cr_select_line and cr_from_line then 'INSERT FROM KEYWORD'
            when cr_values_line IS NOT NULL AND cr_key_line between cr_into_line_no and cr_values_line then 'INSERT INTO KEYWORD'  
            when cr_select_line IS NOT NULL AND cr_iudm_line_no IS NULL THEN 
                 CASE WHEN cr_into_line_no IS NOT NULL THEN 'SELECT INTO'
                      ELSE 'CURSOR SELECT'
                  END
            when cr_cond_line_no IS NOT NULL AND cr_iudm_line_no IS NOT NULL THEN 'USED IN WHERE FOR DML'
            ELSE 'TYPE/VARIABLE'
       END 
       END) 
  over(partition by cr_name, cr_keyword_start_line, cr_keyword_end_line)       
 as keyword_action ,cr_type,cr_owner, cr_line_no ,cr_key_line, cr_keyword_start_line, cr_keyword_end_line,cr_iudm_line_no, cr_select_line, cr_cond_line_no, cr_into_line_no, cr_values_line             
from code_results_gtt c where cr_keyword_flag = 1 
) cri
ON (
  cro.crf_name = cri.cr_name AND
  cro.crf_owner = cri.cr_owner AND
  cro.crf_type = cri.cr_type AND
  cro.crf_line_no = cri.cr_line_no 
)
WHEN MATCHED THEN 
UPDATE set cro.crf_keyword_action = cri.keyword_action 
;
COMMIT;

DELETE FROM code_results_final_gtt WHERE ltrim(crf_text) LIKE '/*%' or ltrim(crf_text) LIKE '**%' or ltrim(crf_text) LIKE '--%' or ltrim(crf_text) LIKE 'PROCEDURE%'
;
commit;

--DBMS_OUTPUT.PUT_LINE ('INSERT INTO FINAL TABLE DONE - FOR loop - '||v_exit_flag); 


--DBMS_OUTPUT.PUT_LINE ('V_PROCESS_FINISH CHECK - '|| v_process_finish ||' - FOR loop - '||v_exit_flag); 


IF v_process_finish = 1 THEN 

--DBMS_OUTPUT.PUT_LINE ('INSIDE V_PROCESS_FINISH IF '|| ' - FOR loop - '||v_exit_flag); 

BEGIN 
SELECT CRF_LINE_NO,CRF_OWNER,CRF_NAME,CRF_TYPE, UPPER(TRIM(crf_variable_keyword))  INTO v_variable_line, v_owner, v_name, v_type,v_keyword
FROM
(SELECT crf_owner, crf_name, crf_type,crf_line_no, crf_variable_keyword,
        row_number() over(order by crf_name, crf_line_no) as rn 
 FROM code_results_final_gtt where crf_variable_keyword is not null and crf_var_process_flag is null  and crf_variable_keyword != nvl(v_keyword,'$')
 ) where rn = 1;

--DBMS_OUTPUT.PUT_LINE ('GOT INPUTS FOR VARIABLE -'|| v_keyword ||' - FOR loop - '||v_exit_flag); 
EXCEPTION 
WHEN OTHERS THEN 
 v_variable_line := NULL;
END;
IF v_variable_line IS NOT NULL THEN 
   v_process_finish := 1;
   v_var_search_level := v_var_search_level + 1; 
   
   --DBMS_OUTPUT.PUT_LINE ('VARIABLE EXISTS-'|| v_keyword ||' - FOR loop - '||v_exit_flag); 

   UPDATE code_results_final_gtt SET crf_var_process_flag = 'Y' where -- crf_owner = v_owner and crf_name = v_name and 
   trim(crf_variable_keyword) = v_keyword
   ;
   COMMIT;

     --DBMS_OUTPUT.PUT_LINE ('VARIABLE EXISTS VAR FLAG UPDATE DONE -'|| v_keyword ||' - FOR loop - '||v_exit_flag); 
    
   
ELSE 
      --DBMS_OUTPUT.PUT_LINE ('process_finish is null - FOR loop - '||v_exit_flag); 

   v_process_finish := 0;
   v_keyword := NULL;
END IF;   

END IF;
  /* 
   --DBMS_OUTPUT.PUT_LINE ('BEFORE DETAILED_FLAG - '|| v_detailed_flag ||' - FOR loop - '||v_exit_flag); 

IF v_detailed_flag = 'Y' THEN 

   --DBMS_OUTPUT.PUT_LINE ('IN DETAILS PRINTING DETAILS - '|| v_detailed_flag ||' - FOR loop - '||v_exit_flag); 


FOR I IN (select  cr_owner, cr_name, cr_type,case when cr_obj_line_no is not null then upper(cr_text) else null end as object_name,cr_line_no, cr_text,
                 cr_obj_dpndnt from code_results_gtt where cr_keyword_flag = 1)
LOOP
  --DBMS_OUTPUT.put_line('Owner :'||i.cr_owner );
  --DBMS_OUTPUT.put_line('Name :'||i.cr_name );
  --DBMS_OUTPUT.put_line('Type :'||i.cr_type );
  --DBMS_OUTPUT.put_line('Object :'||i.object_name );
  --DBMS_OUTPUT.put_line('Line :'||i.cr_line_no );
  --DBMS_OUTPUT.put_line('Code :'||i.cr_text );
  --DBMS_OUTPUT.put_line('Dependants :'||i.cr_obj_dpndnt );
  --DBMS_OUTPUT.put_line('----------------------------------------' );
END LOOP;
ELSE 
   --DBMS_OUTPUT.PUT_LINE ('NO DETAILED_FLAG PRINTING DETAILS- '|| v_detailed_flag ||' - FOR loop - '||v_exit_flag); 

FOR I IN (select distinct cr_owner, cr_name, cr_type,case when cr_obj_line_no is not null then upper(cr_text) else null end as object_name,cr_obj_dpndnt from code_results_gtt)
LOOP
  --DBMS_OUTPUT.put_line('Owner :'||i.cr_owner );
  --DBMS_OUTPUT.put_line('Name :'||i.cr_name );
  --DBMS_OUTPUT.put_line('Type :'||i.cr_type );
  --DBMS_OUTPUT.put_line('Object :'||i.object_name );
  --DBMS_OUTPUT.put_line('Dependants :'||i.cr_obj_dpndnt );
  --DBMS_OUTPUT.put_line('----------------------------------------' );
END LOOP;
END IF;
*/
   --DBMS_OUTPUT.PUT_LINE ('BEFORE THE OBJ DEPENDANCY IF  - v_process_finish- '|| v_process_finish ||' - FOR loop - '||v_exit_flag); 


IF v_process_finish = 0 THEN 


   --DBMS_OUTPUT.PUT_LINE ('INSERTED INTO FINAL TABLE '|| v_process_finish ||' - FOR loop - '||v_exit_flag); 

     --DBMS_OUTPUT.PUT_LINE ('CHECKING THE DEPENDANT FLAG  '|| v_process_finish ||' - FOR loop - '||v_exit_flag); 


IF v_dependant_flag = 1  THEN 

    --DBMS_OUTPUT.PUT_LINE ('IN THE DEPENDANT FLAG  FOR keyword - '|| v_keyword||'  ---- ' ||v_dependant_flag ||' - FOR loop - '||v_exit_flag ); 
    
    BEGIN
    SELECT CRF_LINE_NO,CRF_OWNER,CRF_NAME,CRF_TYPE, UPPER(TRIM(crf_obj_column)) INTO v_object_line, v_owner, v_name, v_type,v_keyword
    FROM
    (SELECT crf_owner, crf_name, crf_type,crf_line_no, crf_obj_column,
            row_number() over(order by crf_name, crf_line_no) as rn 
     FROM CODE_RESULTS_FINAL_GTT where crf_obj_column is not null and crf_obj_process_flag is null and trim(crf_obj_column) != nvl(v_keyword,'$')
     ) where rn = 1;
     
         --DBMS_OUTPUT.PUT_LINE ('got object keyword-  '|| v_keyword ||' - FOR loop - '||v_exit_flag); 
     EXCEPTION 
     WHEN OTHERS THEN 
       v_object_line := NULL ; 
     END;
    
    IF v_object_line IS NOT NULL THEN 

         v_search_level := v_search_level + 1;         
            
         --DBMS_OUTPUT.PUT_LINE ('Object is not null-  '|| v_keyword ||' - FOR loop - '||v_exit_flag); 

       v_process_finish :=1;
       v_run_flag := 1;
       UPDATE CODE_RESULTS_FINAL_GTT SET crf_obj_process_flag = 'Y' where --crf_name = v_name  and 
       trim(crf_obj_column) = v_keyword
        ;
       COMMIT;    
       
     
                --DBMS_OUTPUT.PUT_LINE ('Updated the object flag-  '|| v_keyword ||' - FOR loop - '||v_exit_flag); 

    ELSE 
       v_run_flag := 0;
    END IF;   

   
ELSE 
            --DBMS_OUTPUT.PUT_LINE ('IN ELSE where v_run_flag = 0-  '|| v_keyword ||' - FOR loop - '||v_exit_flag); 

   v_run_flag := 0;
END IF;
END IF;

         --DBMS_OUTPUT.PUT_LINE ('changing v_exit_flag-  '|| v_keyword ||' - FOR loop - '||v_exit_flag); 


v_exit_flag := v_exit_flag - 1;
END LOOP;


END;
