create or repalce procedure test_proc (p_first_name varchar2(30),
                                       p_last_name varchar2(30),
									   p_phone_number varchar2(10)) as 
 l_cust_no number;
 l_elig_flag number ;
begin

 select cust_seq.nextval into l_cust_no from dual;
  
 insert into customers_data
  (cust_no, cust_first_name, cust_last_name,phone_number) values (l_cust_no, p_first_name, p_last_name, p_phone_number);
  
 select eligible_flag into l_elig_flag from eligibility_cust where cust_first_name = p_first_name; 
 
 insert into eligible_details 
 select * from eligibility_cust where cust_first_name = p_first_name; 
 
 update eligible_details set cust_no = l_cust_no where cust_first_name = p_first_name; 
 
 commit;
Exception 
when others then 
 dbms_output.put_line('cust number load failed for customer = '||l_cust_no) ; 
end;