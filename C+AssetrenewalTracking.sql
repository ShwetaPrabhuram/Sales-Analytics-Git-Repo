with exp_assets as (
Select distinct Internal_ID
, date_trunc('day',Exp_date) Exp_date
, date_trunc('month',exp_date) Month_of_Expiry
, expiring_value_acv
, full_rate_card_price
, proposed_price_v1
from "AwsDataCatalog"."analytics_sprabhuram"."connectionrenewals_expiring_nov22_jan23_w_prropprice"
where true
and date_trunc('month',exp_date) = date('2022-12-01')
-- and Internal_ID = '02i3a000008C1I9AAK'
)
, current_data as (
SELECT id
, cx.Exp_date Expiry_Date
, cx.Month_of_Expiry
, cx.Month_of_Expiry - INTERVAL  '1' MONTH as Month_before_expiry
, cx.expiring_value_acv
, cx.full_rate_card_price
, cx.proposed_price_v1
, status Current_status
, line_type Current_line_type
, isdeleted Current_isdeleted
, cast(annual_contract_value as double) CurrentACV
, date_trunc('day', cast(purchasedate as date)) Current_purchasedate
, date_trunc('day', start_date) Current_start_date
, date_trunc('day', end_date) Current_end_date
FROM "entsys_prod"."es_dm"."asset_snapshot" a
join exp_assets cx
on cx."Internal_ID" = a.id
where true
and concat(year,mm,dd) = (select max(concat(year,mm,dd)) FROM "entsys_prod"."es_dm"."asset_snapshot")
)
, renewal_data_exp_contract as (
Select distinct id
, case when renewal_status = 'Pending Fulfillment' then first_value(date_trunc('day',lastmodifieddate)) OVER (PARTITION BY id, renewal_status,end_date ORDER BY start_date DESC, lastmodifieddate ASC ) end AS latest_pendingfulfillment_date
, case when renewal_status = 'Pending Fulfillment' then first_value(date_trunc('day',renewal_start_date)) OVER (PARTITION BY id, renewal_status ,end_date ORDER BY start_date DESC, lastmodifieddate ASC ) end AS future_renewal_start_date
, case when renewal_status = 'Pending Fulfillment' then first_value(date_trunc('day',renewal_end_date)) OVER (PARTITION BY id, renewal_status,end_date ORDER BY start_date DESC, lastmodifieddate ASC ) end AS future_renewal_end_date
, case when renewal_status = 'Pending Fulfillment' then first_value(case when renewal_price = '' then cast(0.0 as double) else cast(renewal_price as double) end) OVER (PARTITION BY id, renewal_status,end_date ORDER BY start_date DESC, lastmodifieddate ASC ) end AS future_renewal_price_actual
, case when renewal_status = 'Pending Fulfillment' then first_value(renewal_price) OVER (PARTITION BY id, renewal_status, end_date ORDER BY start_date DESC, lastmodifieddate ASC ) end AS future_renewal_price_actual_asis
FROM "entsys_prod"."es_dm"."asset_snapshot" a
join exp_assets cx
on cx."Internal_ID" = a.id
where true
-- and date_trunc('day',end_date) = cx.Exp_date
and date_trunc('month',end_date) = cx.Month_of_Expiry
and date_trunc('day',lastmodifieddate) > date_trunc('day',start_date) + INTERVAL  '10' DAY
)

, renewal_data_other as (
Select distinct id
, case when renewal_status = 'Pending Fulfillment' then first_value(date_trunc('day',lastmodifieddate)) OVER (PARTITION BY id, renewal_status ORDER BY start_date DESC, lastmodifieddate ASC ) end AS latest_pendingfulfillment_date_overall
, first_value(cast (annual_contract_value as double)) OVER (PARTITION BY id ORDER BY start_date DESC, lastmodifieddate ASC ) AS starting_acv_of_latest_contract
, case when status = 'Expired' then first_value(date_trunc('day',lastmodifieddate)) OVER (PARTITION BY id, status ORDER BY start_date DESC, lastmodifieddate ASC ) end AS expire_date
, case when status = 'Cancelled' then first_value(date_trunc('day',lastmodifieddate)) OVER (PARTITION BY id, status ORDER BY start_date DESC, lastmodifieddate ASC ) end AS cancel_date
, case when status = 'Cancelled' then first_value(cancellation_reason) OVER (PARTITION BY id, status ORDER BY start_date DESC, lastmodifieddate ASC ) end AS cancellation_reason
FROM "entsys_prod"."es_dm"."asset_snapshot" a
join exp_assets cx
on cx."Internal_ID" = a.id
)


, staging_data as (
select r.id
, Expiry_Date
, Month_of_Expiry
, Month_before_expiry
, expiring_value_acv
, full_rate_card_price
, proposed_price_v1
, Current_status
, Current_isdeleted
, CurrentACV
, Current_line_type
, Current_purchasedate
, Current_start_date
, Current_end_date
, max(latest_pendingfulfillment_date_overall) latest_pendingfulfillment_date_overall
, max(latest_pendingfulfillment_date) latest_pendingfulfillment_date
, max(future_renewal_start_date) future_renewal_start_date
, max(future_renewal_end_date) future_renewal_end_date
, max(future_renewal_price_actual) future_renewal_price_actual
, max(future_renewal_price_actual_asis) future_renewal_price_actual_asis
, max(starting_acv_of_latest_contract) starting_acv_of_latest_contract
, max(expire_date) expire_date
, max(cancel_date) cancel_date
, max(cancellation_reason) cancellation_reason
from current_data c
join renewal_data_exp_contract r
on c.id = r.id
join renewal_data_other ro
on c.id = ro.id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
, final_data as(
Select  date_trunc('week', latest_pendingfulfillment_date)  Week_of_future_renewal
, id
, Expiry_Date
, Month_of_Expiry
, Month_before_expiry
, expiring_value_acv
, full_rate_card_price
, proposed_price_v1
, Current_status
, Current_isdeleted
, CurrentACV
, Current_line_type
, Current_purchasedate
, Current_start_date
, Current_end_date
, latest_pendingfulfillment_date
, latest_pendingfulfillment_date_overall
, future_renewal_start_date
, future_renewal_end_date
, (date_diff('month', future_renewal_start_date, future_renewal_end_date) +1) Duration_of_future_contract_months
, future_renewal_price_actual
, future_renewal_price_actual * (12*1.0/(date_diff('month', future_renewal_start_date, future_renewal_end_date)+1)) future_renewal_price_annualized
,future_renewal_price_actual_asis
, expire_date
, cancel_date
, cancellation_reason
, case when latest_pendingfulfillment_date>= Month_before_expiry and date_trunc('month', latest_pendingfulfillment_date) <= Month_of_Expiry then 1 else 0 end as Agree_to_future_renewal
, case when latest_pendingfulfillment_date>= Month_before_expiry and date_trunc('month', latest_pendingfulfillment_date)<= Month_of_Expiry then future_renewal_price_actual else 0 end as Future_Renewal_Actual
, case when latest_pendingfulfillment_date>= Month_before_expiry and date_trunc('month', latest_pendingfulfillment_date)<= Month_of_Expiry then future_renewal_price_actual * (12*1.0/(date_diff('month', future_renewal_start_date, future_renewal_end_date)+1)) else 0 end as Future_Renewal_ACV
, case when Current_start_date > Expiry_Date then 1 else 0 end as Renewed
, case when Current_start_date > Expiry_Date then starting_acv_of_latest_contract end as Renewed_ACV
, case when Current_start_date > Expiry_Date and Current_status = 'Cancelled' then 1 else 0 end as Renewed_but_cancelled
, case when Current_start_date > Expiry_Date and Current_status = 'Cancelled' then date_diff('day', Current_start_date, cancel_date) end as Days_to_cancel
, case when Current_start_date > Expiry_Date and Current_status = 'Cancelled' and (date_diff('day', Current_start_date, cancel_date)<1 and date_diff('day', Current_start_date, cancel_date) >=0) then 1 else 0 end as Renewed_but_cancelled_same_day
, case when Cancel_Date <= Expiry_Date  and Current_status = 'Cancelled' then 1 else 0 end as Cancelled_Expiring_Contract
from staging_data s
)
, price_compare as (
Select f.*
, case when (Renewed_ACV - proposed_price_v1 <2) and (Renewed_ACV - proposed_price_v1 > -2) then 1 else 0 end as Renewed_at_Proposed_Price
, case when Renewed_ACV - proposed_price_v1 <-2 then 1 else 0 end as Renewed_at_LessThan_Proposed_Price
, case when Renewed_ACV - proposed_price_v1 >2 then 1 else 0 end as Renewed_at_MoreThan_Proposed_Price
from final_data f
)


, weekly_view as (
select Week_of_future_renewal
, count(distinct id) Expiring_Assets
, sum(expiring_value_acv) Expiring_ACV
, sum(Agree_to_future_renewal) Future_renewal_Assets
, sum(Future_Renewal_ACV) Future_Renewal_ACV
, sum(Future_Renewal_Actual) Future_Renewal_Actual
, sum(Cancelled_Expiring_Contract) Cancelled_Expiring_Contract
, sum(Renewed) Renewed_Assets
, sum(Renewed_ACV) Renewed_ACV
, sum(Renewed_but_cancelled) Renewed_but_cancelled_Assets
, sum(Renewed_but_cancelled_same_day) Renewed_but_cancelled_same_day_Assets
from price_compare
group by 1
)

select cast(Week_of_future_renewal as date) Week_of_future_renewal

, sum(Expiring_Assets) OVER() Tot_Expiring_Assets
, round(1.0*sum(Expiring_ACV) OVER(),0) Tot_Expiring_ACV

, round(sum(Future_renewal_Assets) OVER (ORDER BY Week_of_future_renewal)*100.0/ sum(Expiring_Assets) OVER(),2) Cumulative_Promised_ARR
, round(sum(Future_Renewal_ACV) OVER (ORDER BY Week_of_future_renewal)*100/ sum(Expiring_ACV) OVER(),2) Cumulative_Promised_VRR

, round(sum(Renewed_Assets) OVER (ORDER BY Week_of_future_renewal)*100.0/ sum(Expiring_Assets) OVER(),2) Cumulative_ActualRenewed_ARR
, round(sum(Renewed_ACV) OVER (ORDER BY Week_of_future_renewal)*100/ sum(Expiring_ACV) OVER(),2) Cumulative_ActualRenewed_VRR

, sum(Cancelled_Expiring_Contract) OVER (ORDER BY Week_of_future_renewal) Cumulative_Cancelled_Expiring_Contract

, sum(Expiring_Assets) OVER (ORDER BY Week_of_future_renewal) Cumulative_Expiring_Assets
, round(1.0*sum(Expiring_ACV) OVER (ORDER BY Week_of_future_renewal),0) Cumulative_Expiring_ACV

, sum(Future_renewal_Assets) OVER (ORDER BY Week_of_future_renewal) Cumulative_Promisedrenewal_Assets
, round(1.0*sum(Future_Renewal_ACV) OVER (ORDER BY Week_of_future_renewal),0) Cumulative_PromisedRenewal_ACV

, sum(Renewed_Assets) OVER (ORDER BY Week_of_future_renewal) Cumulative_ActualRenewed_Assets
, sum(Renewed_ACV) OVER (ORDER BY Week_of_future_renewal) Cumulative_ActualRenewed_ACV


, Expiring_Assets as Weekly_Expiring_Assets
,round(1.0* Expiring_ACV,0) as Weekly_Expiring_ACV

, round(Renewed_Assets*100.0/ sum(Expiring_Assets) OVER(),2) Weekly_Promised_ARR
, round(Renewed_ACV*100/ sum(Expiring_ACV) OVER(),2) Weekly_Promised_VRR

, Future_renewal_Assets as Weekly_Future_renewal_Assets
,round(1.0* Future_Renewal_ACV,0) as Weekly_Future_Renewal_ACV
-- , round(1.0*Future_Renewal_Actual,0) as Future_Renewal_Actual

, Renewed_Assets as Weekly_Renewed_Assets
,round(1.0* Renewed_ACV,2) as Weekly_Renewed_ACV

, Cancelled_Expiring_Contract as Weekly_Cancelled_Expiring_Contract
from weekly_view