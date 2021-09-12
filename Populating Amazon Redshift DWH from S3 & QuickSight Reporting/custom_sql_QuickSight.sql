select  factTrans.amt, factTrans.is_fraud,
dimCust.gender, dimAddress.state, dimTime.day, dimTime.month, dimTime.quarter, dimMerchant.category
from dev.public.fact_transaction factTrans
join dev.public.dim_customer dimCust
on dimCust.cust_id = factTrans.cust_id
join dev.public.dim_address dimAddress
on dimAddress.addr_id = factTrans.addr_id
join dev.public.dim_time dimTime
on dimTime.time_id = factTrans.time_id
join dev.public.dim_merchant dimMerchant
on dimMerchant.merchant_id = factTrans.merchant_id
