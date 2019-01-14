
drop table perpay_analytics.z_verif_rpt_loan;

--- Grab all completed applications ---

-- Create table for first completion status
create table z_verif_rpt as
  select  borrower.id as borrower_id,
          company.name,
          loan.created as loan_start_dt,
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
and cast(loan.created as date) >= '2017-01-01'
  and job.status = 'primary'
  order by loanstatus.loan_id;

create table z_verif_rpt1 as
  select
    *
  from
  (select *,
          rank() over (partition by loan_id order by id) as record_rank
   from z_verif_rpt order by loan_id, id) as ranked
   where ranked.record_rank = 1;

drop table z_verif_rpt;
alter table z_verif_rpt1 rename to z_verif_rpt;

create table z_verif_rpt1 as
  select
    a.*,
    b.reason
  from z_verif_rpt a
  left join verification_history b on a.loan_id = b.object_id
  where b.content_type_id = 53;

--- Break Out Reasons ---

--- Multiple Orders ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_multiple_orders
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.order.multiple_orders' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_multiple_orders
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- Reduce Amount ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_reduce_amount
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.order.reduce_amount' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_reduce_amount
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- Duplicate Orders ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_duplicate_orders
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.order.duplicate_orders' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_duplicate_orders
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- Conditional Prepay ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_conditional_prepay
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.conditional_approval.prepay' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_conditional_prepay
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- Verify Email ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_verify_email
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.user_email.verify_email' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_verify_email
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- Pay Down Balance ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_paydown_balance
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.other.pay_down_balance' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_paydown_balance
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- Pay Off Balance ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_payoff_balance
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.other.pay_off_balance' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_payoff_balance
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- Underpaying ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_underpaying
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.other.underpaying' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_underpaying
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- Invalid Company ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_invalid_company
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.other.invalid_company' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_invalid_company
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- Payment Plan ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_payment_plan
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.other.payment_ plan' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_payment_plan
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- Custom ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_custom
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.other.custom' in b.reason) > 0
  or position ('loan.verification.order.custom' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_custom
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- Bad Payment History ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_badpay_history
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.other.bad_payment_history' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_badpay_history
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- General Question ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_general_question
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.other.general_question' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_general_question
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
-------------------------
--- Bank & Card ---
create table z_verif_rpt2 as
  select distinct
    a.loan_id,
    count(b.reason) as loan_bank_or_card
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where position ('loan.verification.bank_account.custom' in b.reason) > 0
  or position ('loan.verification.bank_account.add_and_verify' in b.reason) > 0
  or position ('loan.verification.credit_card.add_credit_card' in b.reason) > 0
  group by a.loan_id;

create table z_verif_rpt3 as
  select
    a.*,
    b.loan_bank_or_card
  from z_verif_rpt a
  left join z_verif_rpt2 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt2;
alter table z_verif_rpt3 rename to z_verif_rpt;
drop table z_verif_rpt1;
-------------------------

--- Find First Status After Completion ---

create table z_verif_rpt1 as
  select  borrower.id as borrower_id,
          company.name,
          loan.created as loan_start_dt,
          loanstatus.loan_id as loan_id,
          loanstatus.status,
          loanstatus.created as status_dt,
          loanstatus.id
  from loanstatus
  join loan on loanstatus.loan_id = loan.id
  join borrower on borrower.id = loan.borrower_id
  join job on borrower.id = job.borrower_id
  join company on company.id = job.company_id
  where loanstatus.status in ('awaiting_payment','denied','verification','canceled')
and cast(loan.created as date) >= '2017-01-01'
  and job.status = 'primary'
  order by loanstatus.loan_id;

create table z_verif_rpt2 as
  select
    a.loan_id,
    a.app_complete_dt,
    b.status as uw_outcome,
    b.status_dt as uw_outcome_dt
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where b.status_dt > a.app_complete_dt;

create table z_verif_rpt3 as
  select
    *
  from
  (select *,
          rank() over (partition by loan_id order by uw_outcome_dt) as record_rank
   from z_verif_rpt2 order by loan_id, uw_outcome_dt) as ranked
   where ranked.record_rank = 1;

create table z_verif_rpt4 as
  select
   a.*,
   b.uw_outcome,
   b.uw_outcome_dt
  from z_verif_rpt a
  left join z_verif_rpt3 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt1;
drop table z_verif_rpt2;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt;

--- Find Repayment Status ---

create table z_verif_rpt1 as
  select  borrower.id as borrower_id,
          company.name,
          loan.created as loan_start_dt,
          loanstatus.loan_id as loan_id,
          loanstatus.status,
          loanstatus.created as status_dt,
          loanstatus.id
  from loanstatus
  join loan on loanstatus.loan_id = loan.id
  join borrower on borrower.id = loan.borrower_id
  join job on borrower.id = job.borrower_id
  join company on company.id = job.company_id
  where loanstatus.status in ('approved')
and cast(loan.created as date) >= '2017-01-01'
  and job.status = 'primary'
  order by loanstatus.loan_id;

create table z_verif_rpt2 as
  select
    a.loan_id,
    a.app_complete_dt,
    b.status as repayment_outcome,
    b.status_dt
  from z_verif_rpt a
  left join z_verif_rpt1 b on a.loan_id = b.loan_id
  where b.status_dt > a.app_complete_dt;

create table z_verif_rpt3 as
  select
    *
  from
  (select *,
          rank() over (partition by loan_id order by status_dt) as record_rank
   from z_verif_rpt2 order by loan_id, status_dt) as ranked
   where ranked.record_rank = 1;

create table z_verif_rpt4 as
  select
   a.*,
   b.repayment_outcome
  from z_verif_rpt a
  left join z_verif_rpt3 b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
drop table z_verif_rpt1;
drop table z_verif_rpt2;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt;

--- Grab level ---

create table perpay_analytics.z_verif_rpt_loan as
  select
    a.*,
    b.level_at_approval
  from z_verif_rpt a
  left join z_funnel b on a.loan_id = b.loan_id;

drop table z_verif_rpt;
