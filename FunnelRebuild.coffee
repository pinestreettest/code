drop table z_funnel;
drop table perpay_analytics.z_funnel_report;
drop table perpay_analytics.z_credit_tracking;

----------------------------------------
----------------------------------------
----------------------------------------

-- LOANS STARTED BY COMPANY
create table z_funnel as
select distinct
        borrower.id as borrower_id,
        borrower.account_id,
        company.name,
        company.uuid,
        company.payroll_provider_verified,
        payroll_provider.code,
        payroll_provider.type,
        extract(month from loan.created) as month,
        extract(year from loan.created) as year,
        loanstatus.loan_id as loan_id,
        loan.credit_line,
        loan.amount,
        loan.created as start_dt,
        cast(loanstatus.created as date) as app_start_dt,
        checkout.meta,
        loanstatus.id
from loanstatus
join loan on loanstatus.loan_id = loan.id
join borrower on borrower.id = loan.borrower_id
join job on borrower.id = job.borrower_id
join company on company.id = job.company_id
join payroll_provider on company.payroll_provider_id = payroll_provider.id
join charge on loan.id = charge.loan_id
join checkout on charge.checkout_id = checkout.id
where loanstatus.status in ('application_started')
and cast(loan.created as date) > '2016-04-08'
and job.status = 'primary'
order by loanstatus.loan_id;

create table z_funnel_1 as
select
   *
from
(select *,
        rank() over (partition by loan_id order by id) as record_rank
 from z_funnel order by loan_id, id) as ranked
 where ranked.record_rank = 1;

drop table z_funnel;
alter table z_funnel_1 rename to z_funnel;

----------------------------------------
--- Grab current loan status ---

create table z_funnel_1 as
select  company.name,
        company.uuid,
        extract(month from loan.created) as month,
        extract(year from loan.created) as year,
        loanstatus.loan_id as loan_id,
        loanstatus.status,
        loanstatus.created as status_dt,
        loanstatus.id
from loanstatus
join loan on loanstatus.loan_id = loan.id
join borrower on borrower.id = loan.borrower_id
join job on borrower.id = job.borrower_id
join company on company.id = job.company_id
where loanstatus.status in ('application_started','pending','denied','awaiting_payment','approved','repayment')
and cast(loan.created as date) > '2016-04-08'
and job.status = 'primary'
order by loanstatus.loan_id;

create table z_funnel_2 as
select
   *
from
(select *,
        rank() over (partition by loan_id order by id desc) as record_rank
 from z_funnel_1 order by loan_id, id desc) as ranked
 where ranked.record_rank = 1;

create table z_funnel_3 as
  select a.*,
          b.status as current_status
from z_funnel a left join z_funnel_2 b
on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_1;
drop table z_funnel_2;
alter table z_funnel_3 rename to z_funnel;

------------------------------------------------
--- Determine UW Status of the Loan ---

create table z_funnel_1 as
  select  company.name,
          company.uuid,
          extract(month from loan.created) as month,
          extract(year from loan.created) as year,
          loanstatus.loan_id as loan_id,
          loanstatus.status,
          loanstatus.created as status_dt,
          loanstatus.id
  from loanstatus
  join loan on loanstatus.loan_id = loan.id
  join borrower on borrower.id = loan.borrower_id
  join job on borrower.id = job.borrower_id
  join company on company.id = job.company_id
  where loanstatus.status in ('denied','awaiting_payment','verification')
  and cast(loan.created as date) > '2016-04-08'
  and job.status = 'primary'
  order by loanstatus.loan_id;

create table z_funnel_2 as
  select *
from z_funnel_1
where status in ('verification');

alter table z_funnel_2 add column verifications numeric;
update z_funnel_2 set verifications = 1;

create table z_funnel_3 as
  select loan_id,
          sum(verifications) as verifications
from z_funnel_2
group by loan_id;

create table z_funnel_4 as
  select
    a.*,
    b.verifications
  from z_funnel a
  left join z_funnel_3 b on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_2;
drop table z_funnel_3;
alter table z_funnel_4 rename to z_funnel;
----------------

create table z_funnel_2 as
select
   *
from
(select *,
        rank() over (partition by loan_id order by id desc) as record_rank
from z_funnel_1 order by loan_id, id desc) as ranked
where ranked.record_rank = 1;

create table z_funnel_3 as
  select
    a.*,
    b.status as uw_status,
    b.status_dt as uw_status_dt
  from z_funnel a
  left join z_funnel_2 b on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_1;
drop table z_funnel_2;
alter table z_funnel_3 rename to z_funnel;

------------------------------------------------

--- Determine First Completion Status ---

create table z_funnel_1 as
  select  company.name,
          company.uuid,
          extract(month from loan.created) as month,
          extract(year from loan.created) as year,
          loanstatus.loan_id as loan_id,
          loanstatus.status,
          loanstatus.created as app_complete_dt,
          loanstatus.id
  from loanstatus
  join loan on loanstatus.loan_id = loan.id
  join borrower on borrower.id = loan.borrower_id
  join job on borrower.id = job.borrower_id
  join company on company.id = job.company_id
  where loanstatus.status in ('pending','denied','awaiting_payment','repayment')
  and cast(loan.created as date) > '2016-04-08'
  and job.status = 'primary'
  order by loanstatus.loan_id;

create table z_funnel_2 as
select
   *
from
(select *,
        rank() over (partition by loan_id order by id) as record_rank
 from z_funnel_1 order by loan_id, id) as ranked
 where ranked.record_rank = 1;

 create table z_funnel_3 as
   select distinct
     a.*,
     b.status as app_complete_status,
     b.app_complete_dt as app_complete_status_dt
 from z_funnel a
 left join z_funnel_2 b on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_1;
drop table z_funnel_2;
alter table z_funnel_3 rename to z_funnel;

--- Determine First Approval Status ---

create table z_funnel_1 as
  select  company.name,
          company.uuid,
          extract(month from loan.created) as month,
          extract(year from loan.created) as year,
          loanstatus.loan_id as loan_id,
          loanstatus.status,
          loanstatus.created as app_approval_dt,
          loanstatus.id
  from loanstatus
  join loan on loanstatus.loan_id = loan.id
  join borrower on borrower.id = loan.borrower_id
  join job on borrower.id = job.borrower_id
  join company on company.id = job.company_id
  where loanstatus.status in ('approved')
  and cast(loan.created as date) > '2016-04-08'
  and job.status = 'primary'
  order by loanstatus.loan_id;

create table z_funnel_2 as
select
   *
from
(select *,
        rank() over (partition by loan_id order by id) as record_rank
 from z_funnel_1 order by loan_id, id) as ranked
 where ranked.record_rank = 1;

 create table z_funnel_3 as
   select
     a.*,
     b.status as app_approval_status,
     b.app_approval_dt as app_approval_status_dt
 from z_funnel a
 left join z_funnel_2 b on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_1;
drop table z_funnel_2;
alter table z_funnel_3 rename to z_funnel;
------------------------------------------------
------------------------------------
--- Add Increment ID ---

create table z_funnel_1 as
  select
    loan_id,
    split_part(meta,'order_number',2) as meta
  from z_funnel;

create table z_funnel_2 as
  select
    loan_id,
    substring(meta, 4, 14) as meta
  from z_funnel_1;

create table z_funnel_3 as
  select
    loan_id,
    meta,
    position('-' in meta) as position
  from z_funnel_2;

create table z_funnel_4 as
  select
    loan_id,
    case when position = 10 then substring(meta, 1, 11)
    else substring(meta, 1, 9) end as increment_id
  from z_funnel_3;

create table z_funnel_5 as
  select
    a.*,
    b.increment_id
  from z_funnel a
  left join z_funnel_4 b on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_1;
drop table z_funnel_2;
drop table z_funnel_3;
drop table z_funnel_4;
alter table z_funnel_5 rename to z_funnel;

------------------------------------
--- Add Coupon Information ---

create table z_funnel_1 as
  select distinct
    a.*,
    case when b.coupon_code is null then 'None'
    when b.coupon_code in ('') then 'None'
    when b.coupon_code in ('CYBER65','cyber65','cyBer65','Cyber65','cYBER65','CYbER65','CYBEr65','CYBER65OPS','cyber65ops','Cyber65ops') then 'Cyber65'
    when b.coupon_code in ('CYBER75','Cyber75','cyber75','Cyber75ops','CYBER75OPS','Cyber75Ops','cyber75ops') then 'Cyber75'
    when b.coupon_code in ('EARLYBIRD20','earlybird20') then 'Earlybird20'
    when b.coupon_code in ('FIRST25','first25','First25') then 'First25'
    when b.coupon_code in ('Facebook20') then 'Facebook20'
    when b.coupon_code in ('MVP30','Mvp30','mvp30') then 'MVP30'
    when b.coupon_code in ('VIP50','Vip50','vip50','VIP50ops','VIP50OPS') then 'VIP50'
    when b.coupon_code in ('Nifty50','NIFTY50') then 'Nifty50'
    when b.coupon_code in ('ONEDAY100','oneday100','Oneday100','ONEDAY100OPS','Oneday100Ops') then 'OneDay100'
    when b.coupon_code in ('ONEDAY17','Oneday17','oneday17','OneDay17','oNEDAY17','ONEDay17','oneday17ops','ONEDAY17OPS') then 'OneDay17'
    when b.coupon_code in ('oneday18','OnedAy18','ONeDAY18','ONEDAY18','Oneday18','oneDay18','OneDay18','oneday18ops','ONEDAY18ops','ONEDAY18OPS','Oneday18ops') then 'OneDay18'
    when b.coupon_code in ('ONEDAY50','oneday50','OneDay50','Oneday50','oneday50ops','ONEDAY50OPS') then 'OneDay50'
    else 'Custom' end as coupon_code,
    b.base_discount_amount as coupon_amount
  from z_funnel a
  left join analytics_magento_sales_flat_order b on a.increment_id = b.increment_id;

drop table z_funnel;
alter table z_funnel_1 rename to z_funnel;
------------------------------------
--- Calculate Level ---

create table z_funnel_1 as
  select  company.name,
          company.uuid,
          borrower.id as borrower_id,
          extract(month from loan.created) as month,
          extract(year from loan.created) as year,
          loanstatus.loan_id as loan_id,
          loanstatus.status,
          loanstatus.created as status_dt,
          loanstatus.id
  from loanstatus
  join loan on loanstatus.loan_id = loan.id
  join borrower on borrower.id = loan.borrower_id
  join job on borrower.id = job.borrower_id
  join company on company.id = job.company_id
  where loanstatus.status in ('complete')
  and cast(loan.created as date) > '2016-04-08'
  and loan.principal_balance = 0
  and job.status = 'primary'
  order by loanstatus.loan_id;

create table z_funnel_2 as
  select
    a.loan_id,
    count(distinct(b.loan_id)) as completed_loans
  from z_funnel a
  left join z_funnel_1 b on a.borrower_id = b.borrower_id
  where b.status_dt < a.app_start_dt
  group by a.loan_id;

create table z_funnel_3 as
  select
    a.*,
    b.completed_loans
  from z_funnel a
  left join z_funnel_2 b on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_1;
drop table z_funnel_2;
alter table z_funnel_3 rename to z_funnel;
------------------------------------

create table z_funnel_1 as
  select
    a.loan_id,
    count(b.amount) as deposit_count,
    sum(b.amount) as deposit_amount
  from z_funnel a
  left join deposit b on a.account_id = b.account_id
  where cast(b.created as date) < a.app_start_dt
  and b.status in ('valid')
  group by a.loan_id;

create table z_funnel_2 as
  select
    a.*,
    b.deposit_count,
    b.deposit_amount
from z_funnel a
left join z_funnel_1 b on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_1;
alter table z_funnel_2 rename to z_funnel;
------------------------------------

--- Add Level Information ---

create table z_funnel_1 as
  select
    loan_id,
    case when deposit_count = 0 then '0'
         when deposit_count is null then '0'
         when deposit_amount >= 2500 or completed_loans >= 3 then '4'
         when deposit_amount >= 1000 or completed_loans >= 2 then '3'
         when deposit_amount >=  300 and deposit_count >= 6 then '2'
    else '1' end as level_at_approval,
    case when name in ('Other > Add a new company') then 'Other'
    else 'Verified' end as company_group
  from z_funnel;

create table z_funnel_2 as
  select distinct
    a.*,
    b.level_at_approval,
    b.company_group
  from z_funnel a
  left join z_funnel_1 b on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_1;
alter table z_funnel_2 rename to z_funnel;
------------------------------------

--- Grab loans that were auto approved ---

create table z_funnel_1 as
select
  a.loan_id,
  b.message,
  b.created,
  b.id
from z_funnel a
left join loancomment b on a.loan_id = b.loan_id
where message in ('AUTOMATIC APPROVAL TEST','AUTOMATIC APPROVAL - CL','AUTOMATIC APPROVAL - MULTI','AUTOMATIC APPROVAL - RETURNING','AUTOMATIC APPROVAL - CONSERVATIVE 1','AUTOMATIC APPROVAL - CONSERVATIVE 2','AUTOMATIC APPROVAL');

create table z_funnel_2 as
select
   *
from
(select *,
        rank() over (partition by loan_id order by id) as record_rank
 from z_funnel_1 order by loan_id, id) as ranked
 where ranked.record_rank = 1;

create table z_funnel_3 as
  select
    a.*,
    b.message as autodecision_status,
    b.created as autodecision_status_dt
from z_funnel a
left join z_funnel_2 b on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_1;
drop table z_funnel_2;
alter table z_funnel_3 rename to z_funnel;
------------------------------------------------

--- Determine Post-AP Status ---

create table z_funnel_1 as
  select  company.name,
          company.uuid,
          extract(month from loan.created) as month,
          extract(year from loan.created) as year,
          loanstatus.loan_id as loan_id,
          loanstatus.status,
          loanstatus.created as app_complete_dt,
          loanstatus.id
  from loanstatus
  join loan on loanstatus.loan_id = loan.id
  join borrower on borrower.id = loan.borrower_id
  join job on borrower.id = job.borrower_id
  join company on company.id = job.company_id
  where loanstatus.status in ('approved','canceled')
  and cast(loan.created as date) > '2016-04-08'
  and job.status = 'primary'
  order by loanstatus.loan_id;

create table z_funnel_2 as
select
   *
from
(select *,
        rank() over (partition by loan_id order by id) as record_rank
 from z_funnel_1 order by loan_id, id) as ranked
 where ranked.record_rank = 1;

 create table z_funnel_3 as
   select distinct
     a.*,
     case when b.status is null then 'pending'
     else b.status end as post_ap_status
 from z_funnel a
 left join z_funnel_2 b on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_1;
drop table z_funnel_2;
alter table z_funnel_3 rename to z_funnel;
------------------------------------------------

--- Grab latest spending limit at app start date ---

create table z_funnel_1 as
select
  a.borrower_id,
  a.loan_id,
  a.start_dt,
  b.amount,
  b.is_estimate,
  b.created
from z_funnel a
left join spending_limit b on a.borrower_id = b.borrower_id
where cast(b.created as date) <= cast(a.start_dt as date);

create table z_funnel_2 as
  select
    *
  from
  (select *,
          rank() over (partition by loan_id order by created desc) as record_rank
   from z_funnel_1 order by loan_id, created desc) as ranked
   where ranked.record_rank = 1;

create table z_funnel_3 as
  select
    a.*,
    b.amount as spending_limit,
    b.is_estimate
from z_funnel a
left join z_funnel_2 b on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_1;
drop table z_funnel_2;
alter table z_funnel_3 rename to z_funnel;
------------------------------------------------

--- Create timing metrics ---

create table z_funnel_1 as
  select
    loan_id,

    case when app_complete_status_dt is null then 0
    when datediff(day, app_start_dt, app_complete_status_dt) <= 5 then 1
    else 0 end as completion_5day,
    case when app_complete_status_dt is null then 0
    when datediff(day, app_start_dt, app_complete_status_dt) <= 15 then 1
    else 0 end as completion_15day,
    case when app_complete_status_dt is null then 0
    when datediff(day, app_start_dt, app_complete_status_dt) <= 30 then 1
    else 0 end as completion_30day,

    case when uw_status_dt is null then 0
    when uw_status not in ('awaiting_payment') then 0
    when datediff(day, app_start_dt, uw_status_dt) <= 5 then 1
    else 0 end as awaitpay_5day,
    case when uw_status_dt is null then 0
    when uw_status not in ('awaiting_payment') then 0
    when datediff(day, app_start_dt, uw_status_dt) <= 15 then 1
    else 0 end as awaitpay_15day,
    case when uw_status_dt is null then 0
    when uw_status not in ('awaiting_payment') then 0
    when datediff(day, app_start_dt, uw_status_dt) <= 30 then 1
    else 0 end as awaitpay_30day,

    case when app_approval_status_dt is null then 0
    when datediff(day, app_start_dt, app_approval_status_dt) <= 15 then 1
    else 0 end as repayment_15day,
    case when app_approval_status_dt is null then 0
    when datediff(day, app_start_dt, app_approval_status_dt) <= 30 then 1
    else 0 end as repayment_30day,
    case when app_approval_status_dt is null then 0
    when datediff(day, app_start_dt, app_approval_status_dt) <= 45 then 1
    else 0 end as repayment_45day

  from z_funnel;

create table z_funnel_2 as
  select
    a.*,
    b.completion_5day,
    b.completion_15day,
    b.completion_30day,
    b.awaitpay_5day,
    b.awaitpay_15day,
    b.awaitpay_30day,
    b.repayment_15day,
    b.repayment_30day,
    b.repayment_45day
  from z_funnel a
  left join z_funnel_1 b on a.loan_id = b.loan_id;

drop table z_funnel;
drop table z_funnel_1;
alter table z_funnel_2 rename to z_funnel;
------------------------------------------------

--- Summarize for Funnel Report ---

create table perpay_analytics.z_funnel_report as
  select
    app_start_dt,
    current_status,
    level_at_approval,
    post_ap_status,
    company_group,
    payroll_provider_verified,
    count(loan_id) as loan_count,
    sum(completion_5day) as completion_5day,
    sum(completion_15day) as completion_15day,
    sum(completion_30day) as completion_30day,
    sum(awaitpay_5day) as awaitpay_5day,
    sum(awaitpay_15day) as awaitpay_15day,
    sum(awaitpay_30day) as awaitpay_30day,
    sum(repayment_15day) as repayment_15day,
    sum(repayment_30day) as repayment_30day,
    sum(repayment_45day) as repayment_45day
  from z_funnel
  where app_start_dt >= '2016-08-01'
  group by
    app_start_dt,
    current_status,
    level_at_approval,
    post_ap_status,
    company_group,
    payroll_provider_verified;

------------------------------------------------

--- Summarize for Credit Tracking Report ---

create table perpay_analytics.z_credit_tracking as
  select
    app_start_dt,
    level_at_approval,
    coupon_code,
    coupon_amount,
    count(loan_id) as loan_count,
    sum(amount) as loan_amount,
    sum(completion_15day) as completion_15day,
    sum(awaitpay_15day) as awaitpay_15day,
    sum(repayment_30day) as repayment_30day
from z_funnel
where app_start_dt >= '2016-08-01'
group by
    app_start_dt,
    level_at_approval,
    coupon_code,
    coupon_amount;

------------------------------------------------
