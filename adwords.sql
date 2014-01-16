
  drop table QUERIES;
drop table KEYWORDS;
drop table ADVERTISERS;

drop table ADV_BUDGET;
drop table Q_KEYWORD;
drop table TEMP_BID;
drop table TEMP_FOR_DUPLICATES;
drop table TEMP_RANK;
drop table OUTPUT1;
drop table OUTPUT2;
drop table OUTPUT3;
drop table OUTPUT4;
drop table OUTPUT5;
drop table OUTPUT6;


  CREATE TABLE "QUERIES" ("QID" NUMBER(*,0), "QUERY" VARCHAR2(400));
  ALTER TABLE "QUERIES" ADD PRIMARY KEY ("QID") ENABLE;
  CREATE TABLE "ADVERTISERS" ("ADVERTISERID" NUMBER(*,0), "BUDGET" FLOAT(126), "CTC" FLOAT(126));
  ALTER TABLE "ADVERTISERS" ADD PRIMARY KEY ("ADVERTISERID") ENABLE;
  CREATE TABLE "KEYWORDS" ("ADVERTISERID" NUMBER(*,0), "KEYWORD" VARCHAR2(100), "BID" FLOAT(126));
  ALTER TABLE "KEYWORDS" ADD PRIMARY KEY ("ADVERTISERID", "KEYWORD") ENABLE;
  --ALTER TABLE "KEYWORDS" ADD FOREIGN KEY ("ADVERTISERID") REFERENCES "ADVERTISERS" ("ADVERTISERID") ENABLE;
  CREATE TABLE "ADV_BUDGET" ("ADVID" NUMBER, "INIT_BUDGET" FLOAT(126), "CURR_BUDGET" FLOAT(126), "CLICKS_COUNT" NUMBER, "NON_CLICKS_COUNT" NUMBER);
  CREATE TABLE "Q_KEYWORD" ("QID" NUMBER, "ADVID" NUMBER, "KEYWORD" VARCHAR2(400));
  CREATE TABLE "TEMP_BID" ("BID" FLOAT(126));
  CREATE TABLE "TEMP_FOR_DUPLICATES" ("KWORD" VARCHAR2(400), "QCOUNT" NUMBER, "KCOUNT" NUMBER);
  CREATE TABLE "TEMP_RANK" ("QID" NUMBER, "ADVID" NUMBER, "ADRANK" FLOAT(126), "BALANCE" FLOAT(126), "BID" FLOAT(126), "SIMILARITY" FLOAT(126));

  CREATE TABLE "OUTPUT1" ("QID" NUMBER, "ADRANK" FLOAT(126), "ADVERTISERID" NUMBER, "BALANCE" FLOAT(126), "BUDGET" FLOAT(126));
  CREATE TABLE "OUTPUT2" ("QID" NUMBER, "ADRANK" FLOAT(126), "ADVERTISERID" NUMBER, "BALANCE" FLOAT(126), "BUDGET" FLOAT(126));
  CREATE TABLE "OUTPUT3" ("QID" NUMBER, "ADRANK" FLOAT(126), "ADVERTISERID" NUMBER, "BALANCE" FLOAT(126), "BUDGET" FLOAT(126));
  CREATE TABLE "OUTPUT4" ("QID" NUMBER, "ADRANK" FLOAT(126), "ADVERTISERID" NUMBER, "BALANCE" FLOAT(126), "BUDGET" FLOAT(126));
  CREATE TABLE "OUTPUT5" ("QID" NUMBER, "ADRANK" FLOAT(126), "ADVERTISERID" NUMBER, "BALANCE" FLOAT(126), "BUDGET" FLOAT(126));
  CREATE TABLE "OUTPUT6" ("QID" NUMBER, "ADRANK" FLOAT(126), "ADVERTISERID" NUMBER, "BALANCE" FLOAT(126), "BUDGET" FLOAT(126));


CREATE OR REPLACE PROCEDURE sq(v_1 in number, v_2 in number, v_3 in number, v_4 in number, v_5 in number, v_6 in number)
IS
type array_t is table of varchar2(400);
c number;
array array_t;
 cursor c1 is
     select qid, query
     from queries; -- where qid > 0 and qid < 101;
BEGIN
  --create_tables;
  delete from q_keyword;
  commit;
  c := 0;
  FOR qid in c1
   LOOP
     tokenize(qid.qid, qid.query);
     c := c + 1;
   END LOOP;
   commit;
   task1(v_1);
   task2(v_2);
   task3(v_3);
   task4(v_4);
   task5(v_5);
   task6(v_6);
END;
/
CREATE OR REPLACE PROCEDURE tokenize(qid IN NUMBER, query IN VARCHAR2) IS
type array_t is table of varchar2(100);
array1 apex_application_global.vc_arr2;
array2 array_t;
str varchar2(1000);
BEGIN
  array1 := apex_util.string_to_table(query, ' ');
  for i in 1..array1.count loop
       w_exists(qid, array1(i));
   end loop;
END;
/
CREATE OR REPLACE PROCEDURE w_exists(qi IN number, keyword IN VARCHAR2)
IS
kword varchar2(400);
counter number;
cursor c2(k varchar2) is 
select * from keywords where keyword = k;
begin
 FOR tuple in c2(keyword)
   LOOP
     select count(keyword) into counter from q_keyword where qid = qi and advid = tuple.advertiserid;
    if counter = 0
     then 
     insert into q_keyword values(qi, tuple.advertiserid, keyword);
     kword := '';
    else 
      select keyword into kword from q_keyword where qid = qi and advid = tuple.advertiserid;
      kword := kword || ' ' || keyword;
      update q_keyword set keyword =  kword where qid = qi and advid = tuple.advertiserid;
     end if;
     kword := '';
    END LOOP;
end;
/
create or replace procedure load_table
is
v_advid number;
v_init_budget float;
v_curr_budget float;
v_ctc float;
v_clicks_count number;
v_non_clicks_clount number;
cursor c is
select * from advertisers order by advertiserid asc;
begin
delete from adv_budget;
FOR tuple in c
   LOOP
	select advertiserid, budget, ctc into v_advid, v_init_budget, v_ctc from advertisers where advertiserid = tuple.advertiserid;
	v_curr_budget := v_init_budget;
	v_clicks_count := v_ctc * 100;
        v_non_clicks_clount := 100 - v_clicks_count;
	insert into adv_budget values( v_advid, v_init_budget, v_curr_budget, v_clicks_count, v_non_clicks_clount );
end loop;
commit;
end;
/
create or replace PROCEDURE task1(v_tk_n in number)
IS
type array_number is table of number;
type array_string is table of varchar2(400);
array1 array_number;
array2 array_number;
tokens apex_application_global.vc_arr2;
tokens1 apex_application_global.vc_arr2;
tokens_nd array_string;
count_nd number;
tokens1_nd array_string;
count_unmatched number;
count_temp number;
v_rank_temp number;
a number;
b number;
c number;
topk number;
v_curr_balance float;
v_init_budget float;
v_curr_budget float;
v_clicks_count number;
v_non_clicks_count number;
similarity float;
v_ctc float;
v_bid float;
v_temp float;
v_rank float;
qs float;
q varchar2(400);
k varchar2(400);
cursor c6( tk number) is
select * from (select * from temp_rank order by adrank desc, advid asc) where rownum < ( tk + 1 );
cursor c7 is
select distinct kword from temp_for_duplicates;
cursor c3(qi number) is
select advid, keyword from q_keyword where qid = qi;
cursor c4 is
select distinct qid from q_keyword order by qid asc;
cursor c5 is
select distinct kword from temp_for_duplicates;
begin
  load_table;
  delete from output1;
  commit;
  FOR tuple in c4
   LOOP
     select query into q from queries where qid = tuple.qid;
     tokens := apex_util.string_to_table(q, ' ');
 for i in 1..tokens.count loop
insert into temp_for_duplicates values(tokens(i), 0, 0);
 end loop;
 select count(*) into count_nd from (select distinct kword from temp_for_duplicates);
 tokens_nd := array_string();
 tokens_nd.extend(count_nd);
 count_nd := 1;
 FOR t in c5
     LOOP
    tokens_nd(count_nd) := t.kword;
count_nd := count_nd + 1;
 end loop;
 array1 := array_number();
     array2 := array_number();
     array1.extend(tokens_nd.count);
     array2.extend(tokens_nd.count);
 for i in 1..tokens_nd.count loop
select count(kword) into array1(i) from temp_for_duplicates where kword = tokens_nd(i);
 end loop;
 delete from temp_for_duplicates;
 commit;
      FOR tuple1 in c3(tuple.qid)
       LOOP
        select keyword into k from q_keyword where qid = tuple.qid and advid = tuple1.advid;
        tokens1 := apex_util.string_to_table(k, ' ');
for j in 1..tokens_nd.count loop
array2(j) := 0;
end loop;
count_temp := 0;
        for i in 1..tokens_nd.count loop
          for j in 1..tokens1.count loop
     if tokens_nd(i) = tokens1(j) then
	            if array2(i) != 1 then
					count_temp := count_temp + 1;
				end if;
                array2(i) := 1;
              end if;
          end loop;
  insert into temp_for_duplicates values(tokens(i), array1(i), array2(i));
          end loop;
  select count(keyword) into count_unmatched from keywords where advertiserid = tuple1.advid;
  count_unmatched := count_unmatched - count_temp;
  for i in 1..count_unmatched loop
     insert into temp_for_duplicates values(' ' , 0, 1);
  end loop;
  select sum( qcount * kcount ) into a from temp_for_duplicates;
  select sum( qcount * qcount ) into b from temp_for_duplicates;
  select sum( kcount * kcount ) into c from temp_for_duplicates;
  similarity := a / ( SQRT( b ) * SQRT( c ) );
  delete from temp_for_duplicates;
  commit;
  select ctc into v_ctc from advertisers where advertiserid = tuple1.advid;
  for i in 1..tokens1.count loop
	insert into temp_for_duplicates values ( tokens1(i), 0, 0);
  end loop;
  v_bid := 0;
  FOR t2 in c7
       LOOP
	select bid into v_temp from keywords where advertiserid = tuple1.advid and keyword = t2.kword;
	v_bid := v_bid + v_temp;
  end loop;
  delete from temp_for_duplicates;
  commit;
  qs := v_ctc * similarity;
  v_rank := v_bid * qs;
  select curr_budget into v_curr_balance from adv_budget where advid = tuple1.advid;
  if v_curr_balance = v_bid or v_curr_balance > v_bid then
  insert into temp_rank values( tuple.qid, tuple1.advid, v_rank, v_curr_balance, v_bid, similarity);
  end if;
        end loop;
		v_rank_temp := 1;
FOR t1 in c6( v_tk_n )
       LOOP
		select init_budget, curr_budget, clicks_count, non_clicks_count into 
		v_init_budget, v_curr_budget, v_clicks_count, v_non_clicks_count from adv_budget where advid = t1.advid;
		if v_clicks_count > 0 then
			v_clicks_count := v_clicks_count - 1;
			v_curr_budget := v_curr_budget - t1.bid;
		else
			if v_non_clicks_count > 0 then
				v_non_clicks_count := v_non_clicks_count - 1;
			else 
				select ctc into v_ctc from advertisers where advertiserid = t1.advid;
				v_clicks_count := v_ctc * 100;
				v_non_clicks_count := 100 - ( v_ctc * 100 );
				v_clicks_count := v_clicks_count - 1;
				v_curr_budget := v_curr_budget - t1.bid;
			end if;
		end if;
		update adv_budget set curr_budget = v_curr_budget, clicks_count = v_clicks_count, non_clicks_count = v_non_clicks_count where advid = t1.advid;
		insert into output1 values( t1.qid, v_rank_temp, t1.advid, v_curr_budget, v_init_budget );
		v_rank_temp := v_rank_temp + 1;
end loop;
delete from temp_rank;
   END LOOP;
   commit;
end;
/
create or replace 
PROCEDURE task2( topk in number )
IS
type array_number is table of number;
type array_string is table of varchar2(400);
array1 array_number;
array2 array_number;
tokens apex_application_global.vc_arr2;
tokens1 apex_application_global.vc_arr2;
tokens_nd array_string;
count_nd number;
tokens1_nd array_string;
count_unmatched number;
count_temp number;
v_rank_temp number;
a number;
b number;
c number;
v_k number;
v_temp_bid float;
v_curr_balance float;
v_init_budget float;
v_curr_budget float;
v_clicks_count number;
v_non_clicks_count number;
similarity float;
v_ctc float;
v_bid float;
v_temp float;
v_rank float;
qs float;
q varchar2(400);
k varchar2(400);
cursor c6( tk number) is
select * from (select * from temp_rank order by adrank desc, advid asc) where rownum < ( tk + 1 );
cursor c7 is
select distinct kword from temp_for_duplicates;
cursor c3(qi number) is
select advid, keyword from q_keyword where qid = qi;
cursor c4 is
select distinct qid from q_keyword order by qid asc;
cursor c5 is
select distinct kword from temp_for_duplicates;
begin
  load_table;
  delete from output2;
  commit;
  FOR tuple in c4
   LOOP
     select query into q from queries where qid = tuple.qid;
     tokens := apex_util.string_to_table(q, ' ');
 for i in 1..tokens.count loop
insert into temp_for_duplicates values(tokens(i), 0, 0);
 end loop;
 select count(*) into count_nd from (select distinct kword from temp_for_duplicates);
 tokens_nd := array_string();
 tokens_nd.extend(count_nd);
 count_nd := 1;
 FOR t in c5
     LOOP
    tokens_nd(count_nd) := t.kword;
count_nd := count_nd + 1;
 end loop;
 array1 := array_number();
     array2 := array_number();
     array1.extend(tokens_nd.count);
     array2.extend(tokens_nd.count);
 for i in 1..tokens_nd.count loop
select count(kword) into array1(i) from temp_for_duplicates where kword = tokens_nd(i);
 end loop;
 delete from temp_for_duplicates;
 commit;
      FOR tuple1 in c3(tuple.qid)
       LOOP
        select keyword into k from q_keyword where qid = tuple.qid and advid = tuple1.advid;
        tokens1 := apex_util.string_to_table(k, ' ');
for j in 1..tokens_nd.count loop
array2(j) := 0;
end loop;
count_temp := 0;
        for i in 1..tokens_nd.count loop
          for j in 1..tokens1.count loop
     if tokens_nd(i) = tokens1(j) then
	            if array2(i) != 1 then
					count_temp := count_temp + 1;
				end if;
                array2(i) := 1;
              end if;
          end loop;
  insert into temp_for_duplicates values(tokens(i), array1(i), array2(i));
          end loop;
  select count(keyword) into count_unmatched from keywords where advertiserid = tuple1.advid;
  count_unmatched := count_unmatched - count_temp;
  for i in 1..count_unmatched loop
     insert into temp_for_duplicates values(' ' , 0, 1);
  end loop;
  select sum( qcount * kcount ) into a from temp_for_duplicates;
  select sum( qcount * qcount ) into b from temp_for_duplicates;
  select sum( kcount * kcount ) into c from temp_for_duplicates;
  similarity := a / ( SQRT( b ) * SQRT( c ) );
  delete from temp_for_duplicates;
  commit;
  select ctc into v_ctc from advertisers where advertiserid = tuple1.advid;
  for i in 1..tokens1.count loop
	insert into temp_for_duplicates values ( tokens1(i), 0, 0);
  end loop;
  v_bid := 0;
  FOR t2 in c7
       LOOP
	select bid into v_temp from keywords where advertiserid = tuple1.advid and keyword = t2.kword;
	v_bid := v_bid + v_temp;
  end loop;
  delete from temp_for_duplicates;
  commit;
  qs := v_ctc * similarity;
  v_rank := v_bid * qs;
  select curr_budget into v_curr_balance from adv_budget where advid = tuple1.advid;
  if v_curr_balance = v_bid or v_curr_balance > v_bid then
  insert into temp_rank values( tuple.qid, tuple1.advid, v_rank, v_curr_balance, v_bid, similarity);
  insert into temp_bid values( v_bid );
  end if;
        end loop;
		v_rank_temp := 1;
FOR t1 in c6( topk )
       LOOP
		select init_budget, curr_budget, clicks_count, non_clicks_count into 
		v_init_budget, v_curr_budget, v_clicks_count, v_non_clicks_count from adv_budget where advid = t1.advid;
		select count(*) into v_k from temp_bid where bid < t1.bid;
		if v_k > 0 then
			SELECT bid into v_temp_bid FROM (select bid, rownum rnum from
             (select * from temp_bid where bid < t1.bid ORDER BY bid DESC) where rownum <= 1 )
              WHERE rnum >= 1;
	    else 
			v_temp_bid := t1.bid;
		end if;
		if v_clicks_count > 0 then
			v_clicks_count := v_clicks_count - 1;
			v_curr_budget := v_curr_budget - v_temp_bid;
		else
			if v_non_clicks_count > 0 then
				v_non_clicks_count := v_non_clicks_count - 1;
			else 
				select ctc into v_ctc from advertisers where advertiserid = t1.advid;
				v_clicks_count := v_ctc * 100;
				v_non_clicks_count := 100 - ( v_ctc * 100 );
				v_clicks_count := v_clicks_count - 1;
				v_curr_budget := v_curr_budget - v_temp_bid;
			end if;
		end if;
		update adv_budget set curr_budget = v_curr_budget, clicks_count = v_clicks_count, non_clicks_count = v_non_clicks_count where advid = t1.advid;
		insert into output2 values( t1.qid, v_rank_temp, t1.advid, v_curr_budget, v_init_budget );
		v_rank_temp := v_rank_temp + 1;
end loop;
delete from temp_bid;
delete from temp_rank;
   END LOOP;
   commit;
end;
/
create or replace 
PROCEDURE task3(v_tk_n in number)
IS
type array_number is table of number;
type array_string is table of varchar2(400);
array1 array_number;
array2 array_number;
tokens apex_application_global.vc_arr2;
tokens1 apex_application_global.vc_arr2;
tokens_nd array_string;
count_nd number;
tokens1_nd array_string;
count_unmatched number;
count_temp number;
a number;
b number;
c number;
topk number;
v_curr_balance float;
v_init_budget float;
v_curr_budget float;
v_clicks_count number;
v_non_clicks_count number;
similarity float;
v_rank_temp number;
v_ctc float;
v_bid float;
v_temp float;
v_rank float;
qs float;
q varchar2(400);
k varchar2(400);
cursor c6( tk number) is
select * from (select * from temp_rank order by adrank desc, advid asc) where rownum < ( tk + 1 );
cursor c7 is
select distinct kword from temp_for_duplicates;
cursor c3(qi number) is
select advid, keyword from q_keyword where qid = qi;
cursor c4 is
select distinct qid from q_keyword order by qid asc;
cursor c5 is
select distinct kword from temp_for_duplicates;
begin
  load_table;
  delete from output3;
  commit;
  FOR tuple in c4
   LOOP
     select query into q from queries where qid = tuple.qid;
     tokens := apex_util.string_to_table(q, ' ');
 for i in 1..tokens.count loop
insert into temp_for_duplicates values(tokens(i), 0, 0);
 end loop;
 select count(*) into count_nd from (select distinct kword from temp_for_duplicates);
 tokens_nd := array_string();
 tokens_nd.extend(count_nd);
 count_nd := 1;
 FOR t in c5
     LOOP
    tokens_nd(count_nd) := t.kword;
count_nd := count_nd + 1;
 end loop;
 array1 := array_number();
     array2 := array_number();
     array1.extend(tokens_nd.count);
     array2.extend(tokens_nd.count);
 for i in 1..tokens_nd.count loop
select count(kword) into array1(i) from temp_for_duplicates where kword = tokens_nd(i);
 end loop;
 delete from temp_for_duplicates;
 commit;
     FOR tuple1 in c3(tuple.qid)
       LOOP
        select keyword into k from q_keyword where qid = tuple.qid and advid = tuple1.advid;
        tokens1 := apex_util.string_to_table(k, ' ');
for j in 1..tokens_nd.count loop
array2(j) := 0;
end loop;
count_temp := 0;
        for i in 1..tokens_nd.count loop
           for j in 1..tokens1.count loop
     if tokens_nd(i) = tokens1(j) then
	            if array2(i) != 1 then
					count_temp := count_temp + 1;
				end if;
                array2(i) := 1;
             end if;
          end loop;
  insert into temp_for_duplicates values(tokens(i), array1(i), array2(i));
          end loop;
  select count(keyword) into count_unmatched from keywords where advertiserid = tuple1.advid;
  count_unmatched := count_unmatched - count_temp;
  for i in 1..count_unmatched loop
     insert into temp_for_duplicates values(' ' , 0, 1);
  end loop;
  select sum( qcount * kcount ) into a from temp_for_duplicates;
  select sum( qcount * qcount ) into b from temp_for_duplicates;
  select sum( kcount * kcount ) into c from temp_for_duplicates;
  similarity := a / ( SQRT( b ) * SQRT( c ) );
  delete from temp_for_duplicates;
  commit;
  select ctc into v_ctc from advertisers where advertiserid = tuple1.advid;
  for i in 1..tokens1.count loop
	insert into temp_for_duplicates values ( tokens1(i), 0, 0);
  end loop;
  v_bid := 0;
  FOR t2 in c7
       LOOP
	select bid into v_temp from keywords where advertiserid = tuple1.advid and keyword = t2.kword;
	v_bid := v_bid + v_temp;
  end loop;
  delete from temp_for_duplicates;
  commit;
  select curr_budget into v_curr_balance from adv_budget where advid = tuple1.advid;
  qs := v_ctc * similarity;
  v_rank := v_curr_balance * qs;
  if v_curr_balance = v_bid or v_curr_balance > v_bid then
  insert into temp_rank values( tuple.qid, tuple1.advid, v_rank, v_curr_balance, v_bid, similarity);
  end if;
        end loop;
		v_rank_temp := 1;
FOR t1 in c6( v_tk_n )
       LOOP
	   
		select init_budget, curr_budget, clicks_count, non_clicks_count into 
		v_init_budget, v_curr_budget, v_clicks_count, v_non_clicks_count from adv_budget where advid = t1.advid;
		if v_clicks_count > 0 then
			v_clicks_count := v_clicks_count - 1;
			v_curr_budget := v_curr_budget - t1.bid;
		else
			if v_non_clicks_count > 0 then
				v_non_clicks_count := v_non_clicks_count - 1;
			else 
			    select ctc into v_ctc from advertisers where advertiserid = t1.advid;
				v_clicks_count := v_ctc * 100;
				v_non_clicks_count := 100 - ( v_ctc * 100 );
				v_clicks_count := v_clicks_count - 1;
				v_curr_budget := v_curr_budget - t1.bid;
			end if;
		end if;
		update adv_budget set curr_budget = v_curr_budget,
		clicks_count = v_clicks_count, non_clicks_count = v_non_clicks_count where advid = t1.advid;
		insert into output3 values( t1.qid, v_rank_temp, t1.advid, v_curr_budget, v_init_budget );
		v_rank_temp := v_rank_temp + 1;
end loop;
delete from temp_rank;
   END LOOP;
   commit;
end;
/
create or replace 
PROCEDURE task4( topk in number )
IS
type array_number is table of number;
type array_string is table of varchar2(400);
array1 array_number;
array2 array_number;
tokens apex_application_global.vc_arr2;
tokens1 apex_application_global.vc_arr2;
tokens_nd array_string;
count_nd number;
tokens1_nd array_string;
count_unmatched number;
count_temp number;
v_rank_temp number;
a number;
b number;
c number;
v_k number;
v_temp_bid float;
v_curr_balance float;
v_init_budget float;
v_curr_budget float;
v_clicks_count number;
v_non_clicks_count number;
similarity float;
v_ctc float;
v_bid float;
v_temp float;
v_rank float;
qs float;
q varchar2(400);
k varchar2(400);
cursor c6( tk number) is
select * from (select * from temp_rank order by adrank desc, advid asc) where rownum < ( tk + 1 );
cursor c7 is
select distinct kword from temp_for_duplicates;
cursor c3(qi number) is
select advid, keyword from q_keyword where qid = qi;
cursor c4 is
select distinct qid from q_keyword order by qid asc;
cursor c5 is
select distinct kword from temp_for_duplicates;
begin
  load_table;
  delete from output4;
  commit;
  FOR tuple in c4
   LOOP
     select query into q from queries where qid = tuple.qid;
     tokens := apex_util.string_to_table(q, ' ');
 for i in 1..tokens.count loop
insert into temp_for_duplicates values(tokens(i), 0, 0);
 end loop;
 select count(*) into count_nd from (select distinct kword from temp_for_duplicates);
 tokens_nd := array_string();
 tokens_nd.extend(count_nd);
 count_nd := 1;
 FOR t in c5
     LOOP
    tokens_nd(count_nd) := t.kword;
count_nd := count_nd + 1;
 end loop;
 array1 := array_number();
     array2 := array_number();
     array1.extend(tokens_nd.count);
     array2.extend(tokens_nd.count);
 for i in 1..tokens_nd.count loop
select count(kword) into array1(i) from temp_for_duplicates where kword = tokens_nd(i);
 end loop;
 delete from temp_for_duplicates;
 commit;
     FOR tuple1 in c3(tuple.qid)
       LOOP
        select keyword into k from q_keyword where qid = tuple.qid and advid = tuple1.advid;
        tokens1 := apex_util.string_to_table(k, ' ');
for j in 1..tokens_nd.count loop
array2(j) := 0;
end loop;
count_temp := 0;
        for i in 1..tokens_nd.count loop
           for j in 1..tokens1.count loop
     if tokens_nd(i) = tokens1(j) then
	            if array2(i) != 1 then
					count_temp := count_temp + 1;
				end if;
                array2(i) := 1;
             end if;
          end loop;
  insert into temp_for_duplicates values(tokens(i), array1(i), array2(i));
          end loop;
  select count(keyword) into count_unmatched from keywords where advertiserid = tuple1.advid;
  count_unmatched := count_unmatched - count_temp;
  for i in 1..count_unmatched loop
     insert into temp_for_duplicates values(' ' , 0, 1);
  end loop;
  select sum( qcount * kcount ) into a from temp_for_duplicates;
  select sum( qcount * qcount ) into b from temp_for_duplicates;
  select sum( kcount * kcount ) into c from temp_for_duplicates;
  similarity := a / ( SQRT( b ) * SQRT( c ) );
  delete from temp_for_duplicates;
  commit;
  select ctc into v_ctc from advertisers where advertiserid = tuple1.advid;
  for i in 1..tokens1.count loop
	insert into temp_for_duplicates values ( tokens1(i), 0, 0);
  end loop;
  v_bid := 0;
  FOR t2 in c7
       LOOP
	select bid into v_temp from keywords where advertiserid = tuple1.advid and keyword = t2.kword;
	v_bid := v_bid + v_temp;
  end loop;
  delete from temp_for_duplicates;
  commit;
  select curr_budget into v_curr_balance from adv_budget where advid = tuple1.advid;
  qs := v_ctc * similarity;
  v_rank := v_curr_balance * qs;
  if v_curr_balance = v_bid or v_curr_balance > v_bid then
  insert into temp_rank values( tuple.qid, tuple1.advid, v_rank, v_curr_balance, v_bid, similarity);
  insert into temp_bid values( v_bid );
  end if;
        end loop;
		v_rank_temp := 1;
FOR t1 in c6( topk )
       LOOP
	   
		select init_budget, curr_budget, clicks_count, non_clicks_count into 
		v_init_budget, v_curr_budget, v_clicks_count, v_non_clicks_count from adv_budget where advid = t1.advid;
		select count(*) into v_k from temp_bid where bid < t1.bid;
		if v_k > 0 then
			SELECT bid into v_temp_bid FROM (select bid, rownum rnum from
             (select * from temp_bid where bid < t1.bid ORDER BY bid DESC) where rownum <= 1 )
              WHERE rnum >= 1;
	    else 
			v_temp_bid := t1.bid;
		end if;
		if v_clicks_count > 0 then
			v_clicks_count := v_clicks_count - 1;
			v_curr_budget := v_curr_budget - v_temp_bid;
		else
			if v_non_clicks_count > 0 then
				v_non_clicks_count := v_non_clicks_count - 1;
			else 
			    select ctc into v_ctc from advertisers where advertiserid = t1.advid;
				v_clicks_count := v_ctc * 100;
				v_non_clicks_count := 100 - ( v_ctc * 100 );
				v_clicks_count := v_clicks_count - 1;
				v_curr_budget := v_curr_budget - v_temp_bid;
			end if;
		end if;
		update adv_budget set curr_budget = v_curr_budget, 
		clicks_count = v_clicks_count, non_clicks_count = v_non_clicks_count where advid = t1.advid;
		insert into output4 values( t1.qid, v_rank_temp, t1.advid, v_curr_budget, v_init_budget );
		v_rank_temp := v_rank_temp + 1;
end loop;
delete from temp_bid;
delete from temp_rank;
   END LOOP;
   commit;
end;
/
create or replace 
PROCEDURE task5(v_tk_n in number)
IS
type array_number is table of number;
type array_string is table of varchar2(400);
array1 array_number;
array2 array_number;
tokens apex_application_global.vc_arr2;
tokens1 apex_application_global.vc_arr2;
tokens_nd array_string;
count_nd number;
tokens1_nd array_string;
count_unmatched number;
count_temp number;
a number;
b number;
c number;
topk number;
v_bud_frac float;
v_magic_number float;
v_curr_balance float;
v_init_budget float;
v_curr_budget float;
v_clicks_count number;
v_non_clicks_count number;
similarity float;
v_rank_temp number;
v_ctc float;
v_bid float;
v_temp float;
v_rank float;
qs float;
q varchar2(400);
k varchar2(400);
cursor c6( tk number) is
select * from (select * from temp_rank order by adrank desc, advid asc) where rownum < ( tk + 1 );
cursor c7 is
select distinct kword from temp_for_duplicates;
cursor c3(qi number) is
select advid, keyword from q_keyword where qid = qi;
cursor c4 is
select distinct qid from q_keyword order by qid asc;
cursor c5 is
select distinct kword from temp_for_duplicates;
begin
  load_table;
  delete from output5;
  commit;
  FOR tuple in c4
   LOOP
     select query into q from queries where qid = tuple.qid;
     tokens := apex_util.string_to_table(q, ' ');
 for i in 1..tokens.count loop
insert into temp_for_duplicates values(tokens(i), 0, 0);
 end loop;
 select count(*) into count_nd from (select distinct kword from temp_for_duplicates);
 tokens_nd := array_string();
 tokens_nd.extend(count_nd);
 count_nd := 1;
 FOR t in c5
     LOOP
    tokens_nd(count_nd) := t.kword;
count_nd := count_nd + 1;
 end loop;
 array1 := array_number();
     array2 := array_number();
     array1.extend(tokens_nd.count);
     array2.extend(tokens_nd.count);
 for i in 1..tokens_nd.count loop
select count(kword) into array1(i) from temp_for_duplicates where kword = tokens_nd(i);
 end loop;
 delete from temp_for_duplicates;
 commit;
     FOR tuple1 in c3(tuple.qid)
       LOOP
        select keyword into k from q_keyword where qid = tuple.qid and advid = tuple1.advid;
        tokens1 := apex_util.string_to_table(k, ' ');
for j in 1..tokens_nd.count loop
array2(j) := 0;
end loop;
count_temp := 0;
        for i in 1..tokens_nd.count loop
           for j in 1..tokens1.count loop
     if tokens_nd(i) = tokens1(j) then
	            if array2(i) != 1 then
					count_temp := count_temp + 1;
				end if;
                array2(i) := 1;
             end if;
          end loop;
  insert into temp_for_duplicates values(tokens(i), array1(i), array2(i));
          end loop;
  select count(keyword) into count_unmatched from keywords where advertiserid = tuple1.advid;
  count_unmatched := count_unmatched - count_temp;
  for i in 1..count_unmatched loop
     insert into temp_for_duplicates values(' ' , 0, 1);
  end loop;
  select sum( qcount * kcount ) into a from temp_for_duplicates;
  select sum( qcount * qcount ) into b from temp_for_duplicates;
  select sum( kcount * kcount ) into c from temp_for_duplicates;
  similarity := a / ( SQRT( b ) * SQRT( c ) );
  delete from temp_for_duplicates;
  commit;
  select ctc into v_ctc from advertisers where advertiserid = tuple1.advid;
  for i in 1..tokens1.count loop
	insert into temp_for_duplicates values ( tokens1(i), 0, 0);
  end loop;
  v_bid := 0;
  FOR t2 in c7
       LOOP
	select bid into v_temp from keywords where advertiserid = tuple1.advid and keyword = t2.kword;
	v_bid := v_bid + v_temp;
  end loop;
  delete from temp_for_duplicates;
  commit;
  select curr_budget, init_budget into v_curr_balance, v_bud_frac from adv_budget where advid = tuple1.advid;
  qs := v_ctc * similarity;
  v_bud_frac :=  v_curr_balance/v_bud_frac;
  v_magic_number := v_bid * ( 1 - (POWER(2.71828183, -v_bud_frac)) );
  v_rank := v_magic_number * qs;
  if v_curr_balance = v_bid or v_curr_balance > v_bid then
  insert into temp_rank values( tuple.qid, tuple1.advid, v_rank, v_curr_balance, v_bid, similarity);
  end if;
        end loop;
		v_rank_temp := 1;
FOR t1 in c6( v_tk_n )
       LOOP
	   
		select init_budget, curr_budget, clicks_count, non_clicks_count into 
		v_init_budget, v_curr_budget, v_clicks_count, v_non_clicks_count from adv_budget where advid = t1.advid;
		if v_clicks_count > 0 then
			v_clicks_count := v_clicks_count - 1;
			v_curr_budget := v_curr_budget - t1.bid;
		else
			if v_non_clicks_count > 0 then
				v_non_clicks_count := v_non_clicks_count - 1;
			else 
			    select ctc into v_ctc from advertisers where advertiserid = t1.advid;
				v_clicks_count := v_ctc * 100;
				v_non_clicks_count := 100 - ( v_ctc * 100 );
				v_clicks_count := v_clicks_count - 1;
				v_curr_budget := v_curr_budget - t1.bid;
			end if;
		end if;
		update adv_budget set curr_budget = v_curr_budget,
		clicks_count = v_clicks_count, non_clicks_count = v_non_clicks_count where advid = t1.advid;
		insert into output5 values( t1.qid, v_rank_temp, t1.advid, v_curr_budget, v_init_budget );
		v_rank_temp := v_rank_temp + 1;
end loop;
delete from temp_rank;
   END LOOP;
   commit;
end;
/
create or replace 
PROCEDURE task6( topk in number )
IS
type array_number is table of number;
type array_string is table of varchar2(400);
array1 array_number;
array2 array_number;
tokens apex_application_global.vc_arr2;
tokens1 apex_application_global.vc_arr2;
tokens_nd array_string;
count_nd number;
tokens1_nd array_string;
count_unmatched number;
count_temp number;
v_rank_temp number;
a number;
b number;
c number;
v_k number;
v_bud_frac float;
v_magic_number float;
v_temp_bid float;
v_curr_balance float;
v_init_budget float;
v_curr_budget float;
v_clicks_count number;
v_non_clicks_count number;
similarity float;
v_ctc float;
v_bid float;
v_temp float;
v_rank float;
qs float;
q varchar2(400);
k varchar2(400);
cursor c6( tk number) is
select * from (select * from temp_rank order by adrank desc, advid asc) where rownum < ( tk + 1 );
cursor c7 is
select distinct kword from temp_for_duplicates;
cursor c3(qi number) is
select advid, keyword from q_keyword where qid = qi;
cursor c4 is
select distinct qid from q_keyword order by qid asc;
cursor c5 is
select distinct kword from temp_for_duplicates;
begin
  load_table;
  delete from output6;
  commit;
  FOR tuple in c4
   LOOP
     select query into q from queries where qid = tuple.qid;
     tokens := apex_util.string_to_table(q, ' ');
 for i in 1..tokens.count loop
insert into temp_for_duplicates values(tokens(i), 0, 0);
 end loop;
 select count(*) into count_nd from (select distinct kword from temp_for_duplicates);
 tokens_nd := array_string();
 tokens_nd.extend(count_nd);
 count_nd := 1;
 FOR t in c5
     LOOP
    tokens_nd(count_nd) := t.kword;
count_nd := count_nd + 1;
 end loop;
 array1 := array_number();
     array2 := array_number();
     array1.extend(tokens_nd.count);
     array2.extend(tokens_nd.count);
 for i in 1..tokens_nd.count loop
select count(kword) into array1(i) from temp_for_duplicates where kword = tokens_nd(i);
 end loop;
 delete from temp_for_duplicates;
 commit;
     FOR tuple1 in c3(tuple.qid)
       LOOP
        select keyword into k from q_keyword where qid = tuple.qid and advid = tuple1.advid;
        tokens1 := apex_util.string_to_table(k, ' ');
for j in 1..tokens_nd.count loop
array2(j) := 0;
end loop;
count_temp := 0;
        for i in 1..tokens_nd.count loop
           for j in 1..tokens1.count loop
     if tokens_nd(i) = tokens1(j) then
	            if array2(i) != 1 then
					count_temp := count_temp + 1;
				end if;
                array2(i) := 1;
             end if;
          end loop;
  insert into temp_for_duplicates values(tokens(i), array1(i), array2(i));
          end loop;
  select count(keyword) into count_unmatched from keywords where advertiserid = tuple1.advid;
  count_unmatched := count_unmatched - count_temp;
  for i in 1..count_unmatched loop
     insert into temp_for_duplicates values(' ' , 0, 1);
  end loop;
  select sum( qcount * kcount ) into a from temp_for_duplicates;
  select sum( qcount * qcount ) into b from temp_for_duplicates;
  select sum( kcount * kcount ) into c from temp_for_duplicates;
  similarity := a / ( SQRT( b ) * SQRT( c ) );
  delete from temp_for_duplicates;
  commit;
  select ctc into v_ctc from advertisers where advertiserid = tuple1.advid;
  for i in 1..tokens1.count loop
	insert into temp_for_duplicates values ( tokens1(i), 0, 0);
  end loop;
  v_bid := 0;
  FOR t2 in c7
       LOOP
	select bid into v_temp from keywords where advertiserid = tuple1.advid and keyword = t2.kword;
	v_bid := v_bid + v_temp;
  end loop;
  delete from temp_for_duplicates;
  commit;
  select curr_budget, init_budget into v_curr_balance, v_bud_frac from adv_budget where advid = tuple1.advid;
  qs := v_ctc * similarity;
  v_bud_frac :=  v_curr_balance/v_bud_frac;
  v_magic_number := v_bid * ( 1 - (POWER(2.71828183, -v_bud_frac)) );
  v_rank := v_magic_number * qs;
  if v_curr_balance = v_bid or v_curr_balance > v_bid then
  insert into temp_rank values( tuple.qid, tuple1.advid, v_rank, v_curr_balance, v_bid, similarity);
  insert into temp_bid values( v_bid );
  end if;
        end loop;
		v_rank_temp := 1;
FOR t1 in c6( topk )
       LOOP
	   
		select init_budget, curr_budget, clicks_count, non_clicks_count into 
		v_init_budget, v_curr_budget, v_clicks_count, v_non_clicks_count from adv_budget where advid = t1.advid;
		select count(*) into v_k from temp_bid where bid < t1.bid;
		if v_k > 0 then
			SELECT bid into v_temp_bid FROM (select bid, rownum rnum from
             (select * from temp_bid where bid < t1.bid ORDER BY bid DESC) where rownum <= 1 )
              WHERE rnum >= 1;
	    else 
			v_temp_bid := t1.bid;
		end if;
		if v_clicks_count > 0 then
			v_clicks_count := v_clicks_count - 1;
			v_curr_budget := v_curr_budget - v_temp_bid;
		else
			if v_non_clicks_count > 0 then
				v_non_clicks_count := v_non_clicks_count - 1;
			else 
			    select ctc into v_ctc from advertisers where advertiserid = t1.advid;
				v_clicks_count := v_ctc * 100;
				v_non_clicks_count := 100 - ( v_ctc * 100 );
				v_clicks_count := v_clicks_count - 1;
				v_curr_budget := v_curr_budget - v_temp_bid;
			end if;
		end if;
		update adv_budget set curr_budget = v_curr_budget,
		clicks_count = v_clicks_count, non_clicks_count = v_non_clicks_count where advid = t1.advid;
		insert into output6 values( t1.qid, v_rank_temp, t1.advid, v_curr_budget, v_init_budget );
		v_rank_temp := v_rank_temp + 1;
end loop;
delete from temp_bid;
delete from temp_rank;
   END LOOP;
   commit;
end;
/
exit;