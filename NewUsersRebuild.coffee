
---- NEW USERS QUERY ----
drop table z_new_users;
drop table perpay_analytics.z_user_attribution;
drop table perpay_analytics.z_signup_flow_1;
drop table perpay_analytics.z_new_users_summary;

-- LOANS STARTED BY COMPANY
create table z_new_users as
select distinct
        "user".id as user_id,
        borrower.id as borrower_id,
        company.name,
        company.uuid as company_uuid,
        cast("user".date_joined as date) as date_joined
from job
join borrower on borrower.id = job.borrower_id
join company on company.id = job.company_id
join "user" on borrower.user_id = "user".id
where cast("user".date_joined as date) > '2016-04-06'
and job.status = 'primary';

-- LOANS STARTED BY COMPANY
create table z_new_users1 as
select
        borrower.id as borrower_id,
        company.name,
        extract(month from loan.created) as month,
        extract(year from loan.created) as year,
        loanstatus.loan_id as loan_id,
        loan.created as start_dt,
        loan.amount,
        loanstatus.status,
        loanstatus.created as status_dt
from loanstatus
join loan on loanstatus.loan_id = loan.id
join borrower on borrower.id = loan.borrower_id
join job on borrower.id = job.borrower_id
join company on company.id = job.company_id
where loanstatus.status in ('approved')
and cast(loan.created as date) > '2016-04-06'
and job.status = 'primary'
order by loanstatus.loan_id;

--- Grab loans approved within 30 days ---
create table z_new_users2 as
  select
    a.date_joined,
    b.*
  from z_new_users a left join z_new_users1 b
  on a.borrower_id = b.borrower_id
  where datediff(day, a.date_joined, cast(b.status_dt as date)) <= 30;

create table z_new_users3 as
  select
    borrower_id,
    loan_id,
    status_dt
  from
  (select *,
          rank() over (partition by loan_id order by status_dt) as record_rank
   from z_new_users2 order by loan_id, status_dt) as ranked
   where ranked.record_rank = 1;

create table z_new_users4 as
 select
   borrower_id,
   count(loan_id) as approved_loans
 from z_new_users3
 group by borrower_id;

create table z_new_users5 as
  select
    a.*,
    b.approved_loans as approved_loans_30
from z_new_users a
left join z_new_users4 b on a.borrower_id = b.borrower_id;

drop table z_new_users;
drop table z_new_users2;
drop table z_new_users3;
drop table z_new_users4;
alter table z_new_users5 rename to z_new_users;
-------------------------------------

--- Grab first approved loan date ---

create table z_new_users2 as
 select
   borrower_id,
   loan_id,
   status_dt as first_approval_dt
 from
 (select *,
         rank() over (partition by borrower_id order by status_dt) as record_rank
  from z_new_users1 order by borrower_id, status_dt) as ranked
  where ranked.record_rank = 1;

create table z_new_users3 as
  select
    a.*,
    b.first_approval_dt
  from z_new_users a
  left join z_new_users2 b on a.borrower_id = b.borrower_id;

drop table z_new_users;
drop table z_new_users2;
alter table z_new_users3 rename to z_new_users;
-------------------------------------

--- Grab lifetime Revenue ---

create table z_new_users2 as
  select
    a.borrower_id,
    sum(b.amount) as lifetime_revenue
  from z_new_users a
  left join z_new_users1 b on a.borrower_id = b.borrower_id
  group by a.borrower_id;

create table z_new_users3 as
  select
    a.*,
    b.lifetime_revenue
from z_new_users a
left join z_new_users2 b on a.borrower_id = b.borrower_id;

drop table z_new_users;
drop table z_new_users1;
drop table z_new_users2;
alter table z_new_users3 rename to z_new_users;
-------------------------------------

--- Add whether user was referred ---

create table z_new_users1 as
  select distinct
    a.user_id,
    b.referral_id,
    b.target_object_id
from z_new_users a
left join referrals_referralresponse b on a.user_id = b.user_id
where b.action in ('CONFIRMED');

alter table z_new_users1 add column referred numeric;
update z_new_users1 set referred = 1;

create table z_new_users2 as
  select
    a.*,
    b.referred,
    b.referral_id,
    case when b.referral_id is null then 'non-referral'
    when b.referral_id = 108729 then 'system referral'
    else 'user referral' end as referral_type
  from z_new_users a
  left join z_new_users1 b on a.user_id = b.user_id;

drop table z_new_users;
drop table z_new_users1;
alter table z_new_users2 rename to z_new_users;
-------------------------------------

--- Add Referral Credits ---

create table z_new_users1 as
  select
    a.borrower_id,
    b.id,
    b.amount,
    b.description
from z_new_users a
left join borrower_credit b on a.borrower_id = b.borrower_id
where b.description in ('Referral credit');

create table z_new_users2 as
select
  *
from
(select *,
        rank() over (partition by borrower_id order by id) as record_rank
 from z_new_users1 order by borrower_id, id) as ranked
 where ranked.record_rank = 1;

create table z_new_users3 as
  select distinct
    a.*,
    b.amount as referral_amt
from z_new_users a
left join z_new_users2 b on a.borrower_id = b.borrower_id;

drop table z_new_users;
drop table z_new_users1;
drop table z_new_users2;
alter table z_new_users3 rename to z_new_users;
------------------------------------------
--- Add whether a user uploaded a paystub ---

create table z_new_users1 as
  select
    a.borrower_id,
    b.created as paystub_upload_dt
from z_new_users a
left join paystub b on a.borrower_id = b.borrower_id;

create table z_new_users2 as
  select
    *
  from
  (select *,
          rank() over (partition by borrower_id order by paystub_upload_dt) as record_rank
 from z_new_users1 order by borrower_id, paystub_upload_dt) as ranked
 where ranked.record_rank = 1;

 create table z_new_users3 as
  select
    a.*,
    b.paystub_upload_dt
from z_new_users a
left join z_new_users2 b on a.borrower_id = b.borrower_id;

drop table z_new_users;
drop table z_new_users1;
drop table z_new_users2;
alter table z_new_users3 rename to z_new_users;
------------------------------------------
--- Grab amplitude data ---

create table z_new_users1 as
select
  user_id,
  date_joined
from z_new_users
where date_joined > '2018-06-15';

create table z_new_users2 as
  select distinct
    a.*,
    b.event_type,
    b.user_properties,
    b.event_time
from z_new_users1 a
left join amplitude b on a.user_id = b.user_id;

create table z_new_users3 as
  select
    *
  from
  (select *,
          rank() over (partition by user_id order by event_time) as record_rank
   from z_new_users2 order by user_id, event_time) as ranked
   where ranked.record_rank = 1;

create table z_new_users4 as
  select distinct
    a.*,
    b.user_properties
  from z_new_users a
  left join z_new_users3 b on a.user_id = b.user_id;

drop table z_new_users;
drop table z_new_users1;
drop table z_new_users2;
drop table z_new_users3;
alter table z_new_users4 rename to z_new_users;
------------------------------------------

--- Clean up UTM Tags for joining purposes ---

create table z_new_users1 as
  select distinct
    borrower_id,
    user_properties,
    substring(user_properties from position('initial_utm_campaign' in user_properties) +22 for 75) as utm_to_trim
from z_new_users
where (position('initial_utm_campaign' in user_properties)) > 0;

create table z_new_users2 as
  select distinct
    borrower_id,
    user_properties,
    utm_to_trim,
    substring(utm_to_trim from 2 for position(',' in utm_to_trim )) as utm_to_trim2
  from z_new_users1;

create table z_new_users3 as
  select
    borrower_id,
    user_properties,
    regexp_replace(utm_to_trim2,',','') as utm_to_trim
  from z_new_users2;

update z_new_users3 set utm_to_trim = replace(utm_to_trim,'''','');

create table z_new_users4 as
  select distinct
    a.*,
    b.user_properties as full_utm_tag,
    b.utm_to_trim as utm
from z_new_users a
left join z_new_users3 b on a.borrower_id = b.borrower_id;

drop table z_new_users;
drop table z_new_users1;
drop table z_new_users2;
drop table z_new_users3;
alter table z_new_users4 rename to z_new_users;
------------------------------------------

--- Bring in AdWords CPR on a daily, per borrower basis ---

create table z_new_users1 as
  select distinct
    utm,
    date_joined,
    count(borrower_id) as signups
from z_new_users
where utm is not null
group by utm, date_joined;

create table z_new_users2 as
  select
    *
  from
  (select *,
          rank() over (partition by adid, day order by _sdc_extracted_at desc) as record_rank
from perpay_adwords.ad_performance_report order by adid, day, _sdc_extracted_at desc) as ranked
where ranked.record_rank = 1;

create table z_new_users3 as
 select
  b.utm_campaign,
  a.adid,
  a.clicks,
  a.cost,
  cast(a.day as date) as src_dt
from z_new_users2 a
left join z_utm_campaigns b on a.adid = b.ad_id
where a.adid is not null;

create table z_new_users4 as
  select distinct
    a.*,
    b.cost
from z_new_users1 a
left join z_new_users3 b on a.utm = b.utm_campaign and a.date_joined = b.src_dt;

alter table z_new_users4 add column cpr_total float;
update z_new_users4 set cpr_total = cast(cost as float) / 1000000;

create table z_new_users5 as
  select distinct
    a.*,
    b.signups,
    case when b.cpr_total is null then 0
    else b.cpr_total end as adwords_daily_cost
from z_new_users a
left join z_new_users4 b on a.utm = b.utm and a.date_joined = b.date_joined;

drop table z_new_users;
drop table z_new_users1;
drop table z_new_users2;
drop table z_new_users3;
drop table z_new_users4;
alter table z_new_users5 rename to z_new_users;
------------------------------------------

--- Bring in Facebook CPR on a daily, per borrower basis ---

create table z_new_users1 as
  select distinct
    utm,
    date_joined,
    count(borrower_id) as signups
from z_new_users
where utm is not null
group by utm, date_joined;

create table z_new_users2 as
 select
  b.utm_campaign,
  a.ad_id as adid,
  a.clicks,
  a.spend as cost,
  cast(a.date_start as date) as src_dt
from facebook_ads.ads_insights a
left join z_utm_campaigns b on a.ad_id = b.ad_id
where a.ad_id is not null;

create table z_new_users3 as
  select distinct
    a.*,
    b.cost as cpr_total
from z_new_users1 a
left join z_new_users2 b on a.utm = b.utm_campaign and a.date_joined = b.src_dt;

create table z_new_users4 as
  select distinct
    a.*,
    case when b.cpr_total is null then 0
    else b.cpr_total end as fb_daily_cost
from z_new_users a
left join z_new_users3 b on a.utm = b.utm and a.date_joined = b.date_joined;

drop table z_new_users;
drop table z_new_users1;
drop table z_new_users2;
drop table z_new_users3;
alter table z_new_users4 rename to z_new_users;
------------------------------------------

--- Add Ad-level information to records ---

create table z_new_users1 as
  select
    a.*,
    b.platform,
    b.campaign_name,
    b.adset_name,
    b.ad_name
  from z_new_users a
  left join z_utm_campaigns b on a.utm = b.utm_campaign;

drop table z_new_users;
alter table z_new_users1 rename to z_new_users;
------------------------------------------

--- Add Conversion Tag ---

create table z_new_users1 as
  select
  borrower_id,
  case when first_approval_dt is null then 0
  else 1 end as converted,
  case when approved_loans_30 is null then 0
  else 1 end as converted_30
from z_new_users;

create table z_new_users2 as
  select
    a.*,
    b.converted,
    b.converted_30
from z_new_users a
left join z_new_users1 b on a.borrower_id = b.borrower_id;

drop table z_new_users;
drop table z_new_users1;
alter table z_new_users2 rename to z_new_users;
------------------------------------------

--- Determine if user completed all steps of signup flow ---

create table z_new_users1 as
  select
    a.*,
    case when b.email is null then 1
    else 0 end as missing_email,
    case when b.mobile_phone is null then 1
    when b.mobile_phone in ('') then 1
    else 0 end as missing_phone,
    case when b.phone_verified is null then 1
    when b.phone_verified in ('false') then 1
    else 0 end as missing_phone_verif,
    case when c.estimated_net_pay is null then 1
    else 0 end as missing_net_pay
  from z_new_users a
  left join "user" b on a.user_id = b.id
  left join borrower c on a.borrower_id = c.id;

create table z_new_users2 as
  select
    a.*,
    case when missing_phone = 0 and missing_phone_verif = 0 and missing_net_pay = 0 then 1
    else 0 end as full_signup
  from z_new_users1 a;

drop table z_new_users;
drop table z_new_users1;
alter table z_new_users2 rename to z_new_users;
------------------------------------------

--- Add section indicating farthest a user has made it ---

create table z_new_users2 as
select distinct
        borrower.id as borrower_id,
        min(loanstatus.created) as application_started_dt
from loanstatus
join loan on loanstatus.loan_id = loan.id
join borrower on borrower.id = loan.borrower_id
join job on borrower.id = job.borrower_id
join company on company.id = job.company_id
where loanstatus.status in ('application_started')
and cast(loan.created as date) > '2016-04-06'
and job.status = 'primary'
group by borrower.id;

create table z_new_users3 as
select distinct
        borrower.id as borrower_id,
        min(loanstatus.created) as application_completed_dt
from loanstatus
join loan on loanstatus.loan_id = loan.id
join borrower on borrower.id = loan.borrower_id
join job on borrower.id = job.borrower_id
join company on company.id = job.company_id
where loanstatus.status in ('pending','denied','awaiting_payment','approved','repayment')
and cast(loan.created as date) > '2016-04-06'
and job.status = 'primary'
group by borrower.id;

create table z_new_users4 as
select distinct
        borrower.id as borrower_id,
        min(loanstatus.created) as application_approved_dt
from loanstatus
join loan on loanstatus.loan_id = loan.id
join borrower on borrower.id = loan.borrower_id
join job on borrower.id = job.borrower_id
join company on company.id = job.company_id
where loanstatus.status in ('awaiting_payment')
and cast(loan.created as date) > '2016-04-06'
and job.status = 'primary'
group by borrower.id;

create table z_new_users5 as
select distinct
        borrower.id as borrower_id,
        min(loanstatus.created) as application_repayment_dt
from loanstatus
join loan on loanstatus.loan_id = loan.id
join borrower on borrower.id = loan.borrower_id
join job on borrower.id = job.borrower_id
join company on company.id = job.company_id
where loanstatus.status in ('approved')
and cast(loan.created as date) > '2016-04-06'
and job.status = 'primary'
group by borrower.id;

alter table z_new_users2 add column app_started numeric;
alter table z_new_users3 add column app_completed numeric;
alter table z_new_users4 add column app_approved numeric;
alter table z_new_users5 add column app_repayment numeric;

update z_new_users2 set app_started = 1;
update z_new_users3 set app_completed = 1;
update z_new_users4 set app_approved = 1;
update z_new_users5 set app_repayment = 1;

create table z_new_users6 as
  select
    a.*,
    b.app_started,
    b.application_started_dt,
    c.app_completed,
    c.application_completed_dt,
    e.app_approved,
    e.application_approved_dt,
    d.app_repayment,
    d.application_repayment_dt
from z_new_users a
left join z_new_users2 b on a.borrower_id = b.borrower_id
left join z_new_users3 c on a.borrower_id = c.borrower_id
left join z_new_users4 e on a.borrower_id = e.borrower_id
left join z_new_users5 d on a.borrower_id = d.borrower_id;

drop table z_new_users;
drop table z_new_users2;
drop table z_new_users3;
drop table z_new_users4;
drop table z_new_users5;
alter table z_new_users6 rename to z_new_users;
------------------------------------------

--- Grad borrower choice at landing page ---

create table z_new_users1 as
  select distinct
    a.user_id,
    b.event_type,
    b.event_time
from z_new_users a
left join amplitude b on a.user_id = b.user_id
where event_type in ('landing.shop_now','landing.view_profile');

create table z_new_users2 as
select
   *
from
(select *,
        rank() over (partition by user_id order by event_time) as record_rank
from z_new_users1 order by user_id, event_time desc) as ranked
where ranked.record_rank = 1;

create table z_new_users3 as
  select distinct
    a.*,
    b.event_type
from z_new_users a
left join z_new_users2 b on a.user_id = b.user_id;

drop table z_new_users;
drop table z_new_users1;
drop table z_new_users2;
alter table z_new_users3 rename to z_new_users;
------------------------------------------

--- Create timing metrics ---

create table z_new_users1 as
  select
    user_id,

    case when application_started_dt is null then 0
    when datediff(day, date_joined, application_started_dt) <= 5 then 1
    else 0 end as app_start_5,

    case when application_completed_dt is null then 0
    when datediff(day, date_joined, application_completed_dt) <= 15 then 1
    else 0 end as app_complete_15,

    case when application_approved_dt is null then 0
    when datediff(day, date_joined, application_approved_dt) <= 15 then 1
    else 0 end as app_approved_15,

    case when application_repayment_dt is null then 0
    when datediff(day, date_joined, application_repayment_dt) <= 30 then 1
    else 0 end as app_repayment_30
from z_new_users;

create table z_new_users2 as
  select
    a.*,
    b.app_start_5,
    b.app_complete_15,
    b.app_approved_15,
    b.app_repayment_30
  from z_new_users a
  left join z_new_users1 b on a.user_id = b.user_id;

drop table z_new_users;
drop table z_new_users1;
alter table z_new_users2 rename to z_new_users;
------------------------------------------

--- Create Status Tags ---

create table z_new_users1 as
  select
    a.*,

    case when full_signup is null or full_signup = 0 then 0 else 1 end as full_signup_ind,
    case when app_started is null then 0 else 1 end as app_started_ind,
    case when app_completed is null then 0 else 1 end as app_completed_ind,
    case when app_approved is null then 0 else 1 end as app_approved_ind,
    case when app_repayment is null then 0 else 1 end as app_repayment_ind
from z_new_users a;

drop table z_new_users;
alter table z_new_users1 rename to z_new_users;
------------------------------------------
--- Add Company Verification Date ---

create table z_new_users1 as
  select
    a.*,
    b.co_verification_date,
    case when b.co_verification_date is not null then 'Y' else 'N' end as is_verified
  from z_new_users a
  left join perpay_analytics.perpay_reporting_company b on a.company_uuid = b.company_uuid;

drop table z_new_users;
alter table z_new_users1 rename to z_new_users;
------------------------------------------

--- Summarize for User Attribution Report ---

create table perpay_analytics.z_user_attribution as
  select
    date_joined,
    cast(first_approval_dt as date) as first_approval_dt,
    platform,
    campaign_name,
    adset_name,
    ad_name,
    referred,
    referral_type,
    count(borrower_id) as signups,
    sum(converted) as borrowers,
    sum(converted_30) as borrowers_30,
    sum(lifetime_revenue) as lifetime_revenue,
    avg(fb_daily_cost) as fb_daily_cost,
    avg(adwords_daily_cost) as adwords_daily_cost
  from z_new_users
    group by
    date_joined,
    cast(first_approval_dt as date),
    platform,
    campaign_name,
    adset_name,
    ad_name,
    referred,
    referral_type;

------------------------------------------

--- Summarize for Signup Flow Report ---

create table perpay_analytics.z_signup_flow_1 as
select
    date_joined,
    platform,
    is_verified,
    campaign_name,
    adset_name,
    ad_name,
    event_type,
    full_signup_ind,
    app_started_ind,
    app_completed_ind,
    app_approved_ind,
    app_repayment_ind,
    count(borrower_id) as signups,
    sum(missing_email) as missing_email,
    sum(missing_phone) as missing_phone,
    sum(missing_phone_verif) as missing_phone_verif,
    sum(missing_net_pay) as missing_net_pay,
    sum(full_signup) as full_signup,
    sum(app_started) as app_started,
    sum(app_completed) as app_completed,
    sum(app_approved) as app_approved,
    sum(app_repayment) as app_repayment,
    sum(app_start_5) as app_start_5,
    sum(app_complete_15) as app_complete_15,
    sum(app_approved_15) as app_approved_15,
    sum(app_repayment_30) as app_repayment_30
from z_new_users
group by
    date_joined,
    platform,
    is_verified,
    campaign_name,
    adset_name,
    ad_name,
    event_type,
    full_signup_ind,
    app_started_ind,
    app_completed_ind,
    app_approved_ind,
    app_repayment_ind;

------------------------------------------

--- Summarize for Signups and Active Payers ---

create table perpay_analytics.z_new_users_summary as
  select
    date_joined,
    cast(first_approval_dt as date) as first_approval_dt,
    count(borrower_id) as signups,
    sum(converted) as borrower,
    sum(converted_30) as approved_loans_30
  from z_new_users
    group by
    date_joined,
    cast(first_approval_dt as date);

------------------------------------------
