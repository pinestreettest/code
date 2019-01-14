drop table perpay_analytics.z_verif_rpt_brwr;

--- Grab all borrowers completing a loan in a given month ---

create table z_verif_rpt as
  select distinct
    borrower_id,
    name,
    loan_start_dt,
    extract(month from loan_start_dt) as month,
    extract(year from loan_start_dt) as year,
    loan_id,
    app_complete_dt,
    uw_outcome,
    uw_outcome_dt,
    repayment_outcome,
    level_at_approval
  from perpay_analytics.z_verif_rpt_loan;

  create table z_verif_rpt1 as
    select distinct
      borrower_id,
      year,
      month,
      count(loan_id) as loans
    from z_verif_rpt
    group by borrower_id, year, month;

  create table z_verif_rpt2 as
    select
      a.*,
      b.reason,
      b.created
    from z_verif_rpt a
    left join verification_history b on a.borrower_id = b.object_id
    where b.content_type_id = 43
    and cast(b.created as date) >= cast(a.loan_start_dt as date)
    and cast(b.created as date) <= cast(a.uw_outcome_dt as date);

----
--- Add verification reasons ---

--- Bad Angle ---
create table z_verif_rpt3 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(reason) as brwr_bad_angle
  from z_verif_rpt2
  where position ('borrower.verification.paystub.bad_angle' in reason) > 0
  group by borrower_id, year, month;

create table z_verif_rpt4 as
  select
    a.*,
    b.brwr_bad_angle
from z_verif_rpt1 a
left join z_verif_rpt3 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt1;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt1;
--------------------
--- Bankruptcy ---
create table z_verif_rpt3 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(reason) as brwr_bankrupt
  from z_verif_rpt2
  where position ('borrower.verification.paystub.bankruptcy' in reason) > 0
  group by borrower_id, year, month;

create table z_verif_rpt4 as
  select
    a.*,
    b.brwr_bankrupt
from z_verif_rpt1 a
left join z_verif_rpt3 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt1;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt1;
--------------------
--- Date Issues ---
create table z_verif_rpt3 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(reason) as brwr_dated_stub
  from z_verif_rpt2
  where position ('borrower.verification.paystub.employment_date' in reason) > 0
  or position ('borrower.verification.paystub.last_year' in reason) > 0
  or position ('borrower.verification.paystub.outdated' in reason) > 0
  group by borrower_id, year, month;

create table z_verif_rpt4 as
  select
    a.*,
    b.brwr_dated_stub
from z_verif_rpt1 a
left join z_verif_rpt3 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt1;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt1;
--------------------
--- Partial ---
create table z_verif_rpt3 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(reason) as brwr_partial
  from z_verif_rpt2
  where position ('borrower.verification.paystub.partial' in reason) > 0
  or position ('borrower.verification.paystub.partial_bottom' in reason) > 0
  or position ('borrower.verification.paystub.partial_left' in reason) > 0
  or position ('borrower.verification.paystub.partial_right' in reason) > 0
  or position ('borrower.verification.paystub.partial_top' in reason) > 0
  or position ('borrower.verification.paystub.unreadable' in reason) > 0
  group by borrower_id, year, month;

create table z_verif_rpt4 as
  select
    a.*,
    b.brwr_partial
from z_verif_rpt1 a
left join z_verif_rpt3 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt1;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt1;
--------------------
--- Net Pay ---
create table z_verif_rpt3 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(reason) as brwr_net_pay
  from z_verif_rpt2
  where position ('borrower.verification.paystub.zero_net_pay' in reason) > 0
  or position ('borrower.verification.paystub.low_net_pay' in reason) > 0
  group by borrower_id, year, month;

create table z_verif_rpt4 as
  select
    a.*,
    b.brwr_net_pay
from z_verif_rpt1 a
left join z_verif_rpt3 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt1;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt1;
--------------------
--- Missing Info ---
create table z_verif_rpt3 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(reason) as brwr_missing_info
  from z_verif_rpt2
  where position ('borrower.verification.paystub.missing_employer_name' in reason) > 0
  or position ('borrower.verification.paystub.missing_info' in reason) > 0
  group by borrower_id, year, month;

create table z_verif_rpt4 as
  select
    a.*,
    b.brwr_missing_info
from z_verif_rpt1 a
left join z_verif_rpt3 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt1;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt1;
--------------------
--- Password Protected ---
create table z_verif_rpt3 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(reason) as brwr_pswrd_prtct
  from z_verif_rpt2
  where position ('borrower.verification.paystub.password_protected' in reason) > 0
  group by borrower_id, year, month;

create table z_verif_rpt4 as
  select
    a.*,
    b.brwr_pswrd_prtct
from z_verif_rpt1 a
left join z_verif_rpt3 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt1;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt1;
--------------------
--- Wrong Doc ---
create table z_verif_rpt3 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(reason) as brwr_wrong_doc
  from z_verif_rpt2
  where position ('borrower.verification.paystub.wrong_document' in reason) > 0
  or position ('borrower.verification.paystub.summary' in reason) > 0
  or position ('borrower.verification.paystub.paystub_name' in reason) > 0
  group by borrower_id, year, month;

create table z_verif_rpt4 as
  select
    a.*,
    b.brwr_wrong_doc
from z_verif_rpt1 a
left join z_verif_rpt3 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt1;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt1;
--------------------
--- Wrong Emp ---
create table z_verif_rpt3 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(reason) as brwr_wrong_emp
  from z_verif_rpt2
  where position ('borrower.verification.paystub.wrong_employer' in reason) > 0
  group by borrower_id, year, month;

create table z_verif_rpt4 as
  select
    a.*,
    b.brwr_wrong_emp
from z_verif_rpt1 a
left join z_verif_rpt3 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt1;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt1;
--------------------
--- Help ---
create table z_verif_rpt3 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(reason) as brwr_cntc_help
  from z_verif_rpt2
  where position ('borrower.verification.paystub.contact_for_help' in reason) > 0
  group by borrower_id, year, month;

create table z_verif_rpt4 as
  select
    a.*,
    b.brwr_cntc_help
from z_verif_rpt1 a
left join z_verif_rpt3 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt1;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt1;
--------------------
--- Custom ---
create table z_verif_rpt3 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(reason) as brwr_custom
  from z_verif_rpt2
  where position ('borrower.verification.paystub.custom' in reason) > 0
  group by borrower_id, year, month;

create table z_verif_rpt4 as
  select
    a.*,
    b.brwr_custom
from z_verif_rpt1 a
left join z_verif_rpt3 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt1;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt1;
--------------------
--- Other ---
create table z_verif_rpt3 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(reason) as brwr_other
  from z_verif_rpt2
  where position ('borrower.verification.paystub.social_security_income' in reason) > 0
  or position ('borrower.verification.paystub.tyson' in reason) > 0
  or position ('borrower.verification.paystub.usps' in reason) > 0
  group by borrower_id, year, month;

create table z_verif_rpt4 as
  select
    a.*,
    b.brwr_other
from z_verif_rpt1 a
left join z_verif_rpt3 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt1;
drop table z_verif_rpt3;
alter table z_verif_rpt4 rename to z_verif_rpt1;
--------------------
drop table z_verif_rpt2;

--- Grab loans that made it to approval ---

create table z_verif_rpt2 as
  select
    distinct
      borrower_id,
      year,
      month,
      count(loan_id) as approved_loans
  from z_verif_rpt
  where repayment_outcome is not null
  group by borrower_id, year, month;

  create table perpay_analytics.z_verif_rpt_brwr as
    select
      a.*,
      b.approved_loans
  from z_verif_rpt1 a
  left join z_verif_rpt2 b on a.borrower_id = b.borrower_id and a.year = b.year and a.month = b.month;

drop table z_verif_rpt;
drop table z_verif_rpt1;
drop table z_verif_rpt2;
