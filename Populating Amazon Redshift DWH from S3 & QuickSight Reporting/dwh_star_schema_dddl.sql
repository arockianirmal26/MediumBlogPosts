create table dim_customer
(
cust_id bigint primary key identity(0, 1),
first nvarchar(200), 
last nvarchar(200), 
cc_num bigint, 
gender nvarchar(200),
job nvarchar(200), 
dob nvarchar(200)
);


create table dim_address
(
addr_id bigint primary key  identity(0, 1),
street nvarchar(200),
zip bigint, 
lat decimal(8,2), 
long decimal(8,2), 
city nvarchar(200), 
state nvarchar(200), 
city_pop bigint
);

create table dim_merchant
(
merchant_id bigint primary key identity(0, 1),
merchant nvarchar(200), 
category nvarchar(200), 
merch_lat decimal(8,2), 
merch_long decimal(8,2)
);

create table dim_time
(
time_id bigint primary key identity(0, 1),
--trans_date_trans_time timestamp, 
hour int ,
day int ,
month nvarchar(3), 
quarter nvarchar(2),
year int
);

create table fact_transaction
(
cust_id bigint ,
addr_id bigint ,
merchant_id bigint ,
time_id bigint ,
amt decimal(8,2), 
is_fraud bigint, 
primary key (cust_id, addr_id, merchant_id, time_id),
foreign key(cust_id) references dim_customer(cust_id),
foreign key(addr_id) references dim_address(addr_id),
foreign key(merchant_id) references dim_merchant(merchant_id),
foreign key(time_id) references dim_time(time_id)
);
