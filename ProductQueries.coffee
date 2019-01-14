
drop table z_product;
drop table perpay_analytics.z_product_dash_summary;
drop table perpay_analytics.z_item_detail_summary;
drop table perpay_analytics.z_item_detail_summary_2;
drop table perpay_analytics.z_vendor_scorecard_summary;
drop table perpay_analytics.z_vendor_scorecard_summary_2;
drop table perpay_analytics.z_price_tracking;
drop table perpay_analytics.z_prodcost_v_limit;
drop table perpay_analytics.z_price_monitoring;

create table z_vendor_scorecard_summary as
  select distinct
    a.increment_id,
    a.shipping_address_id,
    b.order_id,
    b.item_id,
    b.sku,
    case when b.name in ('Nintendo - BLACK 3DS XL') then 'Nintendo - Black 3DS XL'
    when b.name in ('Bushnell - Northstar 700mm x 3" Telescope') then 'Bushnell - NorthStar 700mm x 3" Telescope'
    else b.name end as name,
    d.brand,
    b.price,
    b.base_cost,
    b.price * b.qty_ordered as sales_demand,
    b.base_cost * b.qty_ordered as cost,
    b.qty_ordered,
    b.qty_canceled,
    b.qty_shipped,
    b.udropship_vendor,
    d.categoryFull,
    d.category1,
    d.category2,
    d.category3,
    d.category4,
    d.category5,
    d.category6,
    d.upc,
    d.price as current_price,
    d.cost as current_cost,
    d.ship_price,
    d.stockqty as current_inventory,
    d.msrp,
    d.created as sku_create_dt,
    d.status as current_status
  from analytics_magento_sales_flat_order a
  left join analytics_magento_sales_flat_order_item b on a.entity_id = b.order_id
  left join analytics_magento_catalog_product_entity c on b.product_id = c.entity_id
  left join analytics_magento_v_inventory_run_all d on c.entity_id = d.product_id;


create table z_vendor_scorecard_summary1 as
  select
    increment_id,
    count(item_id) as divisor
  from z_vendor_scorecard_summary
  where item_id is not null
  group by increment_id;

create table z_product as
  select
    a.increment_id,
    b.base_shipping_amount/a.divisor as base_shipping_amount,
    b.tax_amount/a.divisor as tax_amount,
    b.base_shipping_tax_amount/a.divisor as base_shipping_tax_amount,
    ((b.base_discount_amount*-1)+b.shipping_discount_amount)/a.divisor as discount
  from z_vendor_scorecard_summary1 a
  left join analytics_magento_sales_flat_order b on a.increment_id = b.increment_id;

create table z_vendor_scorecard_summary3 as
  select
    a.*,
    b.base_shipping_amount,
    b.tax_amount,
    b.base_shipping_tax_amount,
    b.discount
  from z_vendor_scorecard_summary a
  left join z_product b on a.increment_id = b.increment_id;

drop table z_vendor_scorecard_summary;
drop table z_vendor_scorecard_summary1;
drop table z_product;
alter table z_vendor_scorecard_summary3 rename to z_vendor_scorecard_summary;

--- Add original vendor ---

create table z_vendor_scorecard_summary1 as
  select
    a.*,
    b.vendor_name as original_vendor
  from z_vendor_scorecard_summary a
  left join analytics_magento_udropship_vendor b on a.udropship_vendor = b.vendor_id;

drop table z_vendor_scorecard_summary;
alter table z_vendor_scorecard_summary1 rename to z_vendor_scorecard_summary;

--- Add vendor on PO ---

create table z_vendor_scorecard_summary1 as
  select
    a.*,
    b.base_cost * qty as po_cost,
    c.increment_id as po_number,
    c.udropship_status,
    c.udropship_vendor as udropship_vendor_po,
    c.created_at
from z_vendor_scorecard_summary a
left join analytics_magento_udropship_po_item b on a.item_id = b.order_item_id
left join analytics_magento_udropship_po c on b.parent_id = c.entity_id;

create table z_product as
  select
    *
  from
  (select *,
          rank() over (partition by item_id order by created_at desc) as record_rank
   from z_vendor_scorecard_summary1 order by item_id, created_at desc) as ranked
   where ranked.record_rank = 1;

create table z_vendor_scorecard_summary3 as
  select
    a.*,
    b.vendor_name as final_vendor
  from z_product a
  left join analytics_magento_udropship_vendor b on a.udropship_vendor_po = b.vendor_id;

drop table z_vendor_scorecard_summary;
drop table z_vendor_scorecard_summary1;
drop table z_product;
alter table z_vendor_scorecard_summary3 rename to z_vendor_scorecard_summary;

--- Add Shipping Information ---

create table z_vendor_scorecard_summary1 as
  select
    a.*,
    b.region,
    b.postcode
from z_vendor_scorecard_summary a
left join analytics_magento_sales_flat_order_address b on a.shipping_address_id = b.entity_id;

drop table z_vendor_scorecard_summary;
alter table z_vendor_scorecard_summary1 rename to z_vendor_scorecard_summary;

--- Add Perpay data ---

create table z_vendor_scorecard_summary1 as
  select distinct
    a.*,
    b.app_start_dt,
    b.app_approval_status_dt as approval_dt,
    b.is_estimate,
    b.spending_limit,
    b.level_at_approval
  from z_vendor_scorecard_summary a
  left join z_funnel b on a.increment_id = b.increment_id
  where cast(b.start_dt as date) >= '2016-08-01'
  and a.name is not null;

alter table z_vendor_scorecard_summary1 drop column record_rank;

create table z_product as
  select
    *
  from
  (select *,
          rank() over (partition by item_id order by app_start_dt, approval_dt) as record_rank
   from z_vendor_scorecard_summary1 order by item_id, app_start_dt, approval_dt) as ranked
   where ranked.record_rank = 1;

drop table z_vendor_scorecard_summary;
drop table z_vendor_scorecard_summary1;
alter table z_product rename to z_vendor_scorecard_summary;

--- Clean up missing category data ---

create table z_vendor_scorecard_summary1 as
select distinct
  increment_id,
  order_id,
  item_id,
  sku,
  case when original_vendor in ('Payrollshopping') then 'Warranty'
       when original_vendor in ('ProductSecure') then 'Warranty'
       when original_vendor is null then 'Missing'
       when sku is null then 'Missing'
       when original_vendor in ('Furniture') then 'Home'
       when sku in ('BBY-BB10784905') then 'Home'
       when sku in ('PAYROLL-CUSTOM-1') then 'CustomProduct'
       when sku in ('BBY-BB10784911') then 'Home'
       when sku in ('BBY-BB10796834') then 'Home'
       when sku in ('BBY-BB10941294') then 'Home'
       when sku in ('BBY-BB10944903') then 'Home'
       when sku in ('BBY-BB10970355') then 'Home'
       when sku in ('FOS-ADH6169') then 'Fashion'
       when sku in ('FOS-DZ1436') then 'Fashion'
       when sku in ('FOS-DZ1206') then 'Fashion'
       when sku in ('FOS-DZ4180') then 'Fashion'
       when sku in ('FOS-FTW1126') then 'Electronics'
       when sku in ('FOS-MK8077') then 'Fashion'
       when sku in ('DH-E3770') then 'Home'
       when sku in ('DH-8560PK') then 'Electronics'
       when sku in ('BBY-BB20485311') then 'Electronics'
       when sku in ('BBY-BB19850770') then 'Electronics'
       when sku in ('LIN-PFTL99715') then 'Lifestyle'
       when sku in ('LIN-GA0268-001') then 'Lifestyle'
       when sku in ('PS-BDH1200FVAV') then 'Home'
       when sku in ('DH-ADMSF108F') then 'Home'
       when sku in ('BBY-BB19768415|BB19509916 |BB19738033|BB19626681|BB19509931') then 'Electronics'
       when sku in ('BBY-BB20004948|BB19505190|BB19761240|BB19737997|BB12435661|BB19509931') then 'Electronics'
       when sku in ('BBY-BB19882925|BB19509916 |BB19738033|BB19626681|BB19509931') then 'Electronics'
       when sku in ('BBY-BB19840784|BB19509916 |BB19738033|BB19626681|BB19509931') then 'Electronics'
       when sku in ('BBY-BB20004953|BB19505190|BB19761240|BB19737997|BB12435661|BB19509931') then 'Electronics'
       when sku in ('BBY-BB19905779|BB19509916 |BB19738033|BB19626681|BB19509931') then 'Electronics'
       when sku in ('DH-27A230') then 'Electronics'
       when original_vendor in ('FragranceNet') then 'Beauty'
       when original_vendor in ('Malouf') then 'Home'
       when category1 in ('iPhone 6S') then 'Electronics'
       else category1 end as category
from z_vendor_scorecard_summary;

create table z_product as
  select distinct
    a.*,
    b.category
  from z_vendor_scorecard_summary a
  left join z_vendor_scorecard_summary1 b on a.increment_id = b.increment_id and a.item_id = b.item_id and a.sku = b.sku
  where a.increment_id is not null;

drop table z_vendor_scorecard_summary;
drop table z_vendor_scorecard_summary1;

----------------------------------------------
----------------------------------------------
----------------------------------------------
--- Summarize for Product Dashboard Report ---

create table perpay_analytics.z_product_dash_summary as
  select
    category,
    cast(app_start_dt as date) as app_start_dt,
    cast(approval_dt as date) as approval_dt,
    level_at_approval,
    sum(sales_demand) as sales_demand,
    sum(cost) as cost,
    sum(po_cost) as po_cost,
    sum(base_shipping_amount) as base_shipping_amount,
    sum(tax_amount) as tax_amount,
    sum(discount) as discount
from z_product
group by category,
         cast(app_start_dt as date),
         cast(approval_dt as date),
         level_at_approval;


--- Summarize for Item Detail Report ---

create table perpay_analytics.z_item_detail_summary as
  select
    name,
    original_vendor,
    sku,
    sku_create_dt,
    upc,
    brand,
    category1,
    category2,
    category3,
    category4,
    current_status,
    sum(current_price)/count(item_id) as current_price,
    sum(current_cost)/count(item_id) as current_cost,
    sum(current_inventory)/count(item_id) as current_inventory,
    sum(msrp)/count(item_id) as msrp,
    sum(qty_shipped) as qty_shipped,
    sum(qty_ordered) as qty_ordered
from z_product
group by
    name,
    original_vendor,
    sku,
    sku_create_dt,
    upc,
    brand,
    category1,
    category2,
    category3,
    category4,
    current_status;

create table perpay_analytics.z_item_detail_summary_2 as
  select
    name,
    original_vendor,
    sku,
    sku_create_dt,
    upc,
    brand,
    category1,
    category2,
    category3,
    category4,
    current_status,
    level_at_approval,
    cast(app_start_dt as date) as app_start_dt,
    cast(approval_dt as date) as approval_dt,
    sum(qty_ordered) as qty_ordered,
    sum(qty_shipped) as qty_shipped,
    sum(sales_demand) as sales_demand,
    sum(po_cost) as po_cost
  from z_product
  group by
    name,
    original_vendor,
    sku,
    sku_create_dt,
    upc,
    brand,
    category1,
    category2,
    category3,
    category4,
    current_status,
    level_at_approval,
    cast(app_start_dt as date),
    cast(approval_dt as date);


--- Summarize for Vendor Scorecard Report ---

create table perpay_analytics.z_vendor_scorecard_summary as
  select
    name,
    original_vendor,
    final_vendor,
    sku,
    sku_create_dt,
    upc,
    brand,
    category1,
    category2,
    category3,
    category4,
    current_status,
    sum(current_price)/count(item_id) as current_price,
    sum(current_cost)/count(item_id) as current_cost,
    sum(current_inventory)/count(item_id) as current_inventory,
    sum(msrp)/count(item_id) as msrp,
    sum(qty_shipped) as qty_shipped,
    sum(qty_ordered) as qty_ordered
from z_product
group by
    name,
    original_vendor,
    final_vendor,
    sku,
    sku_create_dt,
    upc,
    brand,
    category1,
    category2,
    category3,
    category4,
    current_status;

create table perpay_analytics.z_vendor_scorecard_summary_2 as
  select
    name,
    original_vendor,
    final_vendor,
    sku,
    sku_create_dt,
    upc,
    brand,
    category1,
    category2,
    category3,
    category4,
    current_status,
    level_at_approval,
    cast(app_start_dt as date) as app_start_dt,
    cast(approval_dt as date) as approval_dt,
    sum(qty_ordered) as qty_ordered,
    sum(qty_shipped) as qty_shipped,
    sum(sales_demand) as sales_demand,
    sum(po_cost) as po_cost
  from z_product
  group by
    name,
    original_vendor,
    final_vendor,
    sku,
    sku_create_dt,
    upc,
    brand,
    category1,
    category2,
    category3,
    category4,
    current_status,
    level_at_approval,
    cast(app_start_dt as date),
    cast(approval_dt as date);

--- Summarize for Price Elasticity Report ---

create table perpay_analytics.z_price_tracking as
  select
    sku,
    cast(app_start_dt as date) as app_start_dt,
    price,
    base_cost,
    sum(qty_shipped) as qty_shipped,
    sum(qty_ordered) as qty_ordere
  from z_product
  where sku in ('BBY-BB20658098','BBY-BB20485661','BBY-BB20485666','BBY-BB20485559','BBY-BB21011218','BBY-BB20844402','BBY-BB20844403','BBY-BB20844401',
                'BBY-BB20485337','BBY-BB20485319','BBY-BB21011190','BBY-BB20844392','BBY-BB20844393','BBY-BB20844404','BBY-BB21011184','BBY-BB20844399',
                'BBY-BB20844400','BBY-BB20844398','BBY-BB20485282','BBY-BB20844390','BBY-BB20844391','BBY-BB20844389','BBY-BB20843991','BBY-BB20843995',
                'BBY-BB20843989','BBY-BB20843998','BBY-BB20843997','BBY-BB20843990','BBY-BB20843996','BBY-BB20843994','BBY-BB20843992','BBY-BB20844000',
                'BBY-BB20843999','BBY-BB20843993','BBY-BB20862518','BBY-BB20862499','BBY-BB20862519','BBY-BB20862497','BBY-BB20862495','BBY-BB20862522',
                'BBY-BB20862521','BBY-BB20862523','BBY-BB20862498','BBY-BB20999104','BBY-BB20862500','BBY-BB20999056','BBY-BB20862520','BBY-BB20862502',
                'BBY-BB20862526','BBY-BB20862503','BBY-BB20862501','BBY-BB20862531','BBY-BB20862496','BBY-BB20999069','BBY-BB20862529','BBY-BB20862528',
                'BBY-BB20862527','BBY-BB20862530','BBY-BB20862505','BBY-BB20999077','BBY-BB20862525','BBY-BB20862524','BBY-BB20862532','BBY-BB20862533')
  group by
      sku,
      cast(app_start_dt as date),
      price,
      base_cost;

--- Summarize for Product Cost vs. Spending Limit

create table perpay_analytics.z_prodcost_v_limit as
  select
    name,
    original_vendor,
    sku,
    sku_create_dt,
    upc,
    brand,
    category1,
    category2,
    category3,
    category4,
    current_status,
    level_at_approval,
    cast(app_start_dt as date) as app_start_dt,
    cast(approval_dt as date) as approval_dt,
    current_price,
    is_estimate,
    spending_limit,
    sum(qty_ordered) as qty_ordered,
    sum(qty_shipped) as qty_shipped,
    sum(sales_demand) as sales_demand,
    sum(po_cost) as po_cost
  from z_product
  group by
    name,
    original_vendor,
    sku,
    sku_create_dt,
    upc,
    brand,
    category1,
    category2,
    category3,
    category4,
    current_status,
    level_at_approval,
    cast(app_start_dt as date),
    cast(approval_dt as date),
    current_price,
    is_estimate,
    spending_limit;

--- Summarize for Price Monitoring Report ---

create table perpay_analytics.z_price_monitoring as
  select
    brand,
    original_vendor,
    name,
    sku,
    category,
    msrp,
    current_price,
    ship_price,
    current_cost,
    sku_create_dt,
    current_status,
    cast(app_start_dt as date) as app_start_dt,
    cast(approval_dt as date) as approval_dt,
    sum(qty_ordered) as qty_ordered,
    sum(qty_shipped) as qty_shipped,
    sum(sales_demand) as sales_demand
  from z_product
  group by
    brand,
    original_vendor,
    name,
    sku,
    category,
    msrp,
    current_price,
    ship_price,
    current_cost,
    sku_create_dt,
    current_status,
    cast(app_start_dt as date),
    cast(approval_dt as date);

--- Summarize for Accounting Report ---
drop table perpay_analytics.z_accounting_report;

create table perpay_analytics.z_accounting_report as
  select
    increment_id,
    name,
    category,
    cast(app_start_dt as date) as app_start_dt,
    cast(approval_dt as date) as approval_dt,
    level_at_approval,
    region,
    postcode,
    original_vendor,
    final_vendor,
    udropship_status,
    sum(sales_demand) as sales_demand,
    sum(cost) as cost,
    sum(po_cost) as po_cost,
    sum(base_shipping_amount) as base_shipping_amount,
    sum(tax_amount) as tax_amount,
    sum(base_shipping_tax_amount) as base_shipping_tax_amount,
    sum(discount) as discount,
    sum(qty_ordered) as qty_ordered,
    sum(qty_canceled) as qty_canceled
from z_product
where cast(approval_dt as date) >= (CURRENT_DATE - 60)
group by increment_id,
         name,
         category,
         cast(app_start_dt as date),
         cast(approval_dt as date),
         level_at_approval,
         region,
         postcode,
         original_vendor,
         final_vendor,
         udropship_status;
