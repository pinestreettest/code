drop table z_revenue;
drop table perpay_analytics.z_revenue_check;

----------------- REVENUE QUERY -----------------
 -- Create Shell Table for all companies
 create table z_revenue_check as
 select  distinct
         company.name,
         loanstatus.loan_id,
         loanstatus.status,
         loan.amount,
         loan.credit,
         loan.credit_line,
         loan.principal_balance,
         cast(loan.created as date) as app_start_dt,
         cast(loanstatus.created as date) as src_dt,
         loanstatus.id as src_id,
         checkout.meta
 from loanstatus
 join loan on loanstatus.loan_id = loan.id
 join borrower on borrower.id = loan.borrower_id
 join job on borrower.id = job.borrower_id
 join company on company.id = job.company_id
 join charge on loan.id = charge.loan_id
 join checkout on charge.checkout_id = checkout.id
 where loanstatus.status in ('approved')
 and cast(loan.created as date) > '2016-04-08'
 and job.status = 'primary';

 -- Pull all approved loans
 create table z_revenue_check_1 as
 select  distinct
         company.name,
         loanstatus.loan_id,
         loanstatus.status as refunds,
         loan.principal_balance,
         cast(loanstatus.created as date) as refund_dt
 from loanstatus
 join loan on loanstatus.loan_id = loan.id
 join borrower on borrower.id = loan.borrower_id
 join job on borrower.id = job.borrower_id
 join company on company.id = job.company_id
 where loanstatus.status in ('refunded')
 and cast(loan.created as date) > '2016-04-08'
 and job.status = 'primary';

create table z_revenue_check_2 as
 select
    *
 from
 (select *,
         rank() over (partition by loan_id order by refund_dt desc) as record_rank
  from z_revenue_check_1 order by loan_id, refund_dt desc) as ranked
  where ranked.record_rank = 1;

create table z_revenue_check_3 as
  select
      a.*,
      b.message
from z_revenue_check a left join loancomment b
on a.loan_id = b.loan_id
where message in ('Holiday Exception - Loan fulfilled with credit/debit card first payment and screenshot','Conditional repayment - prepayment fulfillment for type document only based.',
                  'Conditional repayment - prepayment fulfillment for type document based and prepayment.');

alter table z_revenue_check_3 add column holiday_preship numeric;
update z_revenue_check_3 set holiday_preship = 1;

create table z_revenue_check_4 as
 select distinct
         a.*,
         b.refunds,
         b.refund_dt,
         c.holiday_preship
 from z_revenue_check a
 left join z_revenue_check_2 b on a.loan_id = b.loan_id
 left join z_revenue_check_3 c on a.loan_id = c.loan_id;

create table z_revenue_check_5 as
 select
    *
 from
 (select *,
         rank() over (partition by loan_id order by src_dt, src_id) as record_rank
  from z_revenue_check_4 order by loan_id, src_dt, src_id) as ranked
  where ranked.record_rank = 1;

drop table z_revenue_check;
drop table z_revenue_check_1;
drop table z_revenue_check_2;
drop table z_revenue_check_3;
drop table z_revenue_check_4;
alter table z_revenue_check_5 rename to z_revenue_check;

--- Add credit information ---

create table z_revenue_check_1 as
  select distinct
    a.loan_id,
    b.amount_redeemed,
    b.description
  from z_revenue_check a
  left join borrower_credit b on a.loan_id = b.loan_id
  where b.status in ('used');

create table z_revenue_check_2 as
  select distinct
    a.*,
    b.amount_redeemed,
    b.description
  from z_revenue_check a
  left join z_revenue_check_1 b on a.loan_id = b.loan_id;

drop table z_revenue_check;
drop table z_revenue_check_1;
alter table z_revenue_check_2 rename to z_revenue_check;

------------------------------------
--- Add Increment ID ---

create table z_revenue_check_1 as
  select
    loan_id,
    split_part(meta,'order_number',2) as meta
  from z_revenue_check;

create table z_revenue_check_2 as
  select
    loan_id,
    substring(meta, 4, 14) as meta
  from z_revenue_check_1;

create table z_revenue_check_3 as
  select
    loan_id,
    meta,
    position('-' in meta) as position
  from z_revenue_check_2;

create table z_revenue_check_4 as
  select
    loan_id,
    case when position = 10 then substring(meta, 1, 11)
    else substring(meta, 1, 9) end as increment_id
  from z_revenue_check_3;

create table z_revenue_check_5 as
  select
    a.*,
    b.increment_id
  from z_revenue_check a
  left join z_revenue_check_4 b on a.loan_id = b.loan_id;

drop table z_revenue_check;
drop table z_revenue_check_1;
drop table z_revenue_check_2;
drop table z_revenue_check_3;
drop table z_revenue_check_4;
alter table z_revenue_check_5 rename to z_revenue_check;

------------------------------------
--- Add Coupon Information ---

create table z_revenue_check_1 as
  select distinct
    a.*,
    b.coupon_code,
    b.base_discount_amount as coupon_amount
  from z_revenue_check a
  left join analytics_magento_sales_flat_order b on a.increment_id = b.increment_id;

drop table z_revenue_check;
alter table z_revenue_check_1 rename to z_revenue_check;
------------------------------------
--- Add Level Information ---

create table z_revenue_check_1 as
  select distinct
    a.*,
    case when b.total_deposits_count_at_approval = 0 then '0'
         when b.total_deposits_amount_at_approval >= 2500 or b.total_at_approval_complete >= 3 then '4'
         when b.total_deposits_amount_at_approval >= 1000 or b.total_at_approval_complete >= 2 then '3'
         when b.total_deposits_amount_at_approval >=  300 and b.total_deposits_count_at_approval >= 6 then '2'
    else '1' end as level_at_approval
  from z_revenue_check a
  left join analytics_loan_etl b on a.loan_id = b.loan_id;

drop table z_revenue_check;
alter table z_revenue_check_1 rename to z_revenue;
------------------------------------
--- Summarize for Revenue Report ---

create table z_revenue_check_2 as
  select distinct
    src_dt,
    level_at_approval,
    credit_line,
    holiday_preship,
    loan_id,
    amount
  from z_revenue;

create table perpay_analytics.z_revenue_check as
  select
    src_dt,
    level_at_approval,
    credit_line,
    holiday_preship,
    count(loan_id) as revenue_count,
    sum(amount) as revenue_amount
  from z_revenue_check_2
  group by
    src_dt,
    level_at_approval,
    credit_line,
    holiday_preship;

drop table z_revenue_check_2;
------------------------------------
