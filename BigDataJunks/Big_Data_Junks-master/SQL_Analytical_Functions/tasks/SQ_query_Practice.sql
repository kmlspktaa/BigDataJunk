/* 
    Author: Abuchi Okeke
    Description: Executing analytical functions on sakila-database
    */
    

-- Task 1
-- Basic Analytical Functions

-- AVG() as an analytical function: average amount of payment by each staff ID order by the customer ID
select payment_id, customer_id, staff_id, rental_id, amount,
avg(amount) over (partition by staff_id order by customer_id) as average_amount
from payment;

-- COUNT() as an analytical function: number of records or rows based on the film rating
select title, release_year, length, rating,
count(*) over (partition by rating order by release_year) as number_of_rows_by_rating
from film;

-- MAX() and MIN() as an analytical function: maximum and minimum payment amount partitioned by customer_id
select payment_id, customer_id, staff_id, amount,
max(amount) over (partition by customer_id order by staff_id) as max_amount_by_customer_id,
min(amount) over (partition by customer_id order by staff_id) as minimum_amount_by_customer_id
from payment;

-- SUM() as an analytical function: show total amount by customer_id
select payment_id, customer_id, staff_id, amount,
sum(amount) over (partition by customer_id order by staff_id) as total_amount_by_customer_id
from payment;

-- Advance analytical functions.

-- LAG() and LEAD(): show previous and the next amount to each of the customer_id amount
select payment_id, customer_id, staff_id, amount,
lag(amount,1,0) over (partition by customer_id order by staff_id) as prev_amount,
lead(amount,1,0) over (partition by customer_id order by staff_id) as next_amount
from payment;

-- Task 2

/* create a new table (emp) to practice rank and dense_rank */
create table emp(eno int, ename varchar(20), edno int, sal int);

insert into emp values (100, 'abc', 10, 1000);
insert into emp values (101, 'def', 10, 1200);
insert into emp values (102, 'ghi', 20, 1000);
insert into emp values (103, 'jkl', 10, 1200);
insert into emp values (104, 'mno', 20, 1300);
insert into emp values (105, 'pqr', 10, 1200);
insert into emp values (106, 'stu', 10, 900);
insert into emp values (107, 'vwx', 30, 700);
insert into emp values (106, 'yz', 30, 900);
select * from emp;

select eno, ename, edno, sal,
rank() over( partition by edno order by sal desc) as 'rank',
dense_rank() over ( partition by edno order by sal desc) as 'dense_rank'
from emp;

drop table emp;

-- Task 3
/* Installation completed and running */
