USE [Periscope_Data]
GO

/****** Object:  StoredProcedure [dbo].[pricingMeeting_ExclCat]    Script Date: 12/15/2017 4:40:37 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE procedure [dbo].[pricingMeeting_ExclCat]
as
	declare @q1_last date, @year_first date, @q1_first date, @q2_last date, @q2_first date,
		@q3_last date, @q3_first date, @q4_last date, @b2_cut_off date;
	declare @types table(idx int identity(1,1), type varchar(12));
	declare @i int;
	declare @type varchar(15);

insert into @types
values ('ZIP'),('AGENT'),('COUNTY');




set @q1_last = eomonth(dateadd(month,-1,current_timestamp));
set @year_first = eomonth(dateadd(year,-1,@q1_last));
set @q1_first = dateadd(day,1,eomonth(dateadd(quarter,-1,@q1_last)));
set @q2_last = eomonth(dateadd(quarter,-1,@q1_last));
set @q2_first = dateadd(day,1,eomonth(dateadd(quarter,-2,@q1_last)));
set @q3_last = eomonth(dateadd(quarter,-2,@q1_last));
set @q3_first = dateadd(day,1,eomonth(dateadd(quarter,-3,@q1_last)));
set @q4_last = eomonth(dateadd(quarter,-3,@q1_last));
set @b2_cut_off = '2016-12-13';



if object_id('Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT','U') is not null
	drop table Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT;
create table Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT
(STATE varchar(10),
PROGRAM VARCHAR(7),
CLUTCH VARCHAR(25),
NB_RN varchar(2),
TYPE VARCHAR(50),
GROUPING VARCHAR(25),
COUNTY VARCHAR(25),
EARNED_PREMIUM_PIP DECIMAL(12,2) DEFAULT 0,
EARNED_PREMIUM_PD DECIMAL(12,2) DEFAULT 0,
EARNED_PREMIUM_BI DECIMAL(12,2) DEFAULT 0,
EARNED_PREMIUM_CMP DECIMAL(12,2) DEFAULT 0,
EARNED_PREMIUM_COL DECIMAL(12,2) DEFAULT 0,

OL_EARNED_PREMIUM_PIP DECIMAL(12,2) DEFAULT 0,
OL_EARNED_PREMIUM_PD DECIMAL(12,2) DEFAULT 0,
OL_EARNED_PREMIUM_BI DECIMAL(12,2) DEFAULT 0,
OL_EARNED_PREMIUM_CMP DECIMAL(12,2) DEFAULT 0,
OL_EARNED_PREMIUM_COL DECIMAL(12,2) DEFAULT 0,

Q1_PIP INTEGER DEFAULT 0,
Q1_PD INTEGER DEFAULT 0,
Q1_BI INTEGER DEFAULT 0,
Q1_CMP INTEGER DEFAULT 0,
Q1_COL INTEGER DEFAULT 0,

YEAR_EARNED_PREMIUM_PIP DECIMAL(12,2) DEFAULT 0,
YEAR_EARNED_PREMIUM_PD DECIMAL(12,2) DEFAULT 0,
YEAR_EARNED_PREMIUM_BI DECIMAL(12,2) DEFAULT 0,
YEAR_EARNED_PREMIUM_CMP DECIMAL(12,2) DEFAULT 0,
YEAR_EARNED_PREMIUM_COL DECIMAL(12,2) DEFAULT 0,

OL_YEAR_EARNED_PREMIUM_PIP DECIMAL(12,2) DEFAULT 0,
OL_YEAR_EARNED_PREMIUM_PD DECIMAL(12,2) DEFAULT 0,
OL_YEAR_EARNED_PREMIUM_BI DECIMAL(12,2) DEFAULT 0,
OL_YEAR_EARNED_PREMIUM_CMP DECIMAL(12,2) DEFAULT 0,
OL_YEAR_EARNED_PREMIUM_COL DECIMAL(12,2) DEFAULT 0,

YEAR_PIP INTEGER DEFAULT 0,
YEAR_PD INTEGER DEFAULT 0,
YEAR_BI INTEGER DEFAULT 0,
YEAR_CMP INTEGER DEFAULT 0,
YEAR_COL INTEGER DEFAULT 0,

YEAR_PIP_INCUR DECIMAL(12,2) DEFAULT 0,
YEAR_PD_INCUR DECIMAL(12,2) DEFAULT 0,
YEAR_BI_INCUR DECIMAL(12,2) DEFAULT 0,
YEAR_CMP_INCUR DECIMAL(12,2) DEFAULT 0,
YEAR_COL_INCUR DECIMAL(12,2) DEFAULT 0,

QUOTE_COUNT INTEGER DEFAULT 0,
SALES_COUNT INTEGER DEFAULT 0,

Q2_EARNED_PREMIUM_PIP DECIMAL(12,2) DEFAULT 0,
Q2_EARNED_PREMIUM_PD DECIMAL(12,2) DEFAULT 0,
Q2_EARNED_PREMIUM_BI DECIMAL(12,2) DEFAULT 0,
Q2_EARNED_PREMIUM_CMP DECIMAL(12,2) DEFAULT 0,
Q2_EARNED_PREMIUM_COL DECIMAL(12,2) DEFAULT 0,

OL_Q2_EARNED_PREMIUM_PIP DECIMAL(12,2) DEFAULT 0,
OL_Q2_EARNED_PREMIUM_PD DECIMAL(12,2) DEFAULT 0,
OL_Q2_EARNED_PREMIUM_BI DECIMAL(12,2) DEFAULT 0,
OL_Q2_EARNED_PREMIUM_CMP DECIMAL(12,2) DEFAULT 0,
OL_Q2_EARNED_PREMIUM_COL DECIMAL(12,2) DEFAULT 0,

Q2_PIP INTEGER DEFAULT 0,
Q2_PD INTEGER DEFAULT 0,
Q2_BI INTEGER DEFAULT 0,
Q2_CMP INTEGER DEFAULT 0,
Q2_COL INTEGER DEFAULT 0,

Q2_QUOTE_COUNT INTEGER DEFAULT 0,
Q2_SALES_COUNT INTEGER DEFAULT 0,

Q3_EARNED_PREMIUM_PIP DECIMAL(12,2) DEFAULT 0,
Q3_EARNED_PREMIUM_PD DECIMAL(12,2) DEFAULT 0,
Q3_EARNED_PREMIUM_BI DECIMAL(12,2) DEFAULT 0,
Q3_EARNED_PREMIUM_CMP DECIMAL(12,2) DEFAULT 0,
Q3_EARNED_PREMIUM_COL DECIMAL(12,2) DEFAULT 0,

OL_Q3_EARNED_PREMIUM_PIP DECIMAL(12,2) DEFAULT 0,
OL_Q3_EARNED_PREMIUM_PD DECIMAL(12,2) DEFAULT 0,
OL_Q3_EARNED_PREMIUM_BI DECIMAL(12,2) DEFAULT 0,
OL_Q3_EARNED_PREMIUM_CMP DECIMAL(12,2) DEFAULT 0,
OL_Q3_EARNED_PREMIUM_COL DECIMAL(12,2) DEFAULT 0,

DATA_AS_OF DATE,

PIP_FACTOR FLOAT(15) DEFAULT 1,
PD_FACTOR FLOAT(15) DEFAULT 1,
BI_FACTOR FLOAT(15) DEFAULT 1,
CMP_FACTOR FLOAT(15) DEFAULT 1,
COL_FACTOR FLOAT(15) DEFAULT 1,

PRIMARY KEY(STATE, PROGRAM, CLUTCH, NB_RN, TYPE, GROUPING, COUNTY));

set @i = 1;
while (@i <= (select count(*) from @types))
begin
	set @type = (select type from @types where idx = @i);

-- earned premium data

merge Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT as Target

using (select state, coalesce(ProgName,'TOTAL') as program,
CLUTCH, NB_RN, TYPE, Grouping,county,
coalesce(q1_last_pip,0)-coalesce(q2_last_pip,0) as q1_pip,
coalesce(q1_last_pip,0)-coalesce(year_last_pip,0) as year_pip,
coalesce(q2_last_pip,0)-coalesce(q3_last_pip,0) as q2_pip,
coalesce(q3_last_pip,0)-coalesce(q4_last_pip,0) as q3_pip,

coalesce(q1_last_pd,0)-coalesce(q2_last_pd,0) as q1_pd,
coalesce(q1_last_pd,0)-coalesce(year_last_pd,0) as year_pd,
coalesce(q2_last_pd,0)-coalesce(q3_last_pd,0) as q2_pd,
coalesce(q3_last_pd,0)-coalesce(q4_last_pd,0) as q3_pd,

coalesce(q1_last_bi,0)-coalesce(q2_last_bi,0) as q1_bi,
coalesce(q1_last_bi,0)-coalesce(year_last_bi,0) as year_bi,
coalesce(q2_last_bi,0)-coalesce(q3_last_bi,0) as q2_bi,
coalesce(q3_last_bi,0)-coalesce(q4_last_bi,0) as q3_bi,

coalesce(q1_last_cmp,0)-coalesce(q2_last_cmp,0) as q1_cmp,
coalesce(q1_last_cmp,0)-coalesce(year_last_cmp,0) as year_cmp,
coalesce(q2_last_cmp,0)-coalesce(q3_last_cmp,0) as q2_cmp,
coalesce(q3_last_cmp,0)-coalesce(q4_last_cmp,0) as q3_cmp,

coalesce(q1_last_col,0)-coalesce(q2_last_col,0) as q1_col,
coalesce(q1_last_col,0)-coalesce(year_last_col,0) as year_col,
coalesce(q2_last_col,0)-coalesce(q3_last_col,0) as q2_col,
coalesce(q3_last_col,0)-coalesce(q4_last_col,0) as q3_col
from (select 
ProgState as state,
ProgName,
'' as CLUTCH,
case when policy.isRenewal = 0 then 'NB' else 'RN' end as NB_RN,
@type as TYPE,
case @type 
when 'ZIP' then address.zipcode
when 'COUNTY' then coalesce(texasCounty,address.county)
when 'AGENT' then concat(code,'-',subcode) end as Grouping,
coalesce(texasCounty,address.county) as county,
sum(case when coverage = 'PIP' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q1_last_pip,
sum(case when coverage = 'PIP' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q2_last_pip,
sum(case when coverage = 'PIP' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q3_last_pip,
sum(case when coverage = 'PIP' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q4_last_pip,
sum(case when coverage = 'PIP' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as year_last_pip,

-- pd
sum(case when coverage = 'PD' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q1_last_pd,
sum(case when coverage = 'PD' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q2_last_pd,
sum(case when coverage = 'PD' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q3_last_pd,
sum(case when coverage = 'PD' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q4_last_pd,
sum(case when coverage = 'PD' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as year_last_pd,

-- bi
sum(case when coverage = 'BI' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q1_last_bi,
sum(case when coverage = 'BI' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q2_last_bi,
sum(case when coverage = 'BI' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q3_last_bi,
sum(case when coverage = 'BI' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q4_last_bi,
sum(case when coverage = 'BI' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as year_last_bi,

-- cmp
sum(case when coverage = 'OTC' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q1_last_cmp,
sum(case when coverage = 'OTC' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q2_last_cmp,
sum(case when coverage = 'OTC' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q3_last_cmp,
sum(case when coverage = 'OTC' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q4_last_cmp,
sum(case when coverage = 'OTC' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as year_last_cmp,

-- col
sum(case when coverage = 'COLL' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q1_last < @b2_cut_off then @b2_cut_off else @q1_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q1_last_col,
sum(case when coverage = 'COLL' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q2_last < @b2_cut_off then @b2_cut_off else @q2_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q2_last_col,
sum(case when coverage = 'COLL' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q3_last < @b2_cut_off then @b2_cut_off else @q3_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q3_last_col,
sum(case when coverage = 'COLL' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @q4_last < @b2_cut_off then @b2_cut_off else @q4_last end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as q4_last_col,
sum(case when coverage = 'COLL' then
coalesce(round(changeInTPD*
	case when transactionDate > 
		case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end
		or case when changeType = 1 then policy.effectiveDate else dateeffective end > case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end then null
	else
		cast((1 + datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,
			case when case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end >expirationDate 
		then expirationDate else case when left(policyNum,1) <> 'T' and @year_first < @b2_cut_off then @b2_cut_off else @year_first end end
		)) as decimal)/cast(nullif((1+datediff(day,case when changeType = 1 then policy.effectiveDate else dateeffective end,expirationDate)),0) as decimal) end,2),0) end) as year_last_col
from [Windhaven_Report].dbo.CoveragePremium
join [Windhaven_Report].dbo.Policy on Policy.policyID = CoveragePremium.policyId
 join [Windhaven_Report].dbo.producer on policy.producerId = producer.producerId
 join (select vehicle.policyId, min(vehicleNumber) as minVeh
	from [Windhaven_Report].dbo.vehicle
	group by policyId) veh on veh.policyId = policy.policyId
 join [Windhaven_Report].dbo.vehicle on vehicle.policyId = policy.policyId and vehicleNumber = minVeh
 left join [Windhaven_Report].dbo.address on vehicle.garagingAddressId = address.id
join [Periscope_Data].dbo.ProgramNum on ProgNum = ratingProgram
	left join Periscope_Data.B2_Data.TxZips on texasZip = address.zipcode and coalesce(address.county,'') = ''
group by 
ProgState,
case when policy.isRenewal = 0 then 'NB' else 'RN' end,
case @type 
when 'ZIP' then address.zipcode
when 'COUNTY' then coalesce(texasCounty,address.county)
when 'AGENT' then concat(code,'-',subcode) end, coalesce(texasCounty,address.county),
ProgName
 with rollup) b
 where state is not null and grouping is not null and county is not null and NB_RN is not null) as Source
 on (Target.State = Source.state and Target.PROGRAM = Source.program
	and Target.CLUTCH = Source.CLUTCH and Target.TYPE = Source.TYPE
	and Target.GROUPING = Source.Grouping and Target.COUNTY = Source.county
	and Target.NB_RN = Source.NB_RN)

 when matched then
	update set Target.EARNED_PREMIUM_PIP = Source.q1_pip,
		Target.YEAR_EARNED_PREMIUM_PIP = Source.year_pip,
		Target.Q2_EARNED_PREMIUM_PIP = Source.q2_pip,
		Target.Q3_EARNED_PREMIUM_PIP = Source.q3_pip,

		Target.EARNED_PREMIUM_PD = Source.q1_pd,
		Target.YEAR_EARNED_PREMIUM_PD = Source.year_pd,
		Target.Q2_EARNED_PREMIUM_PD = Source.q2_pd,
		Target.Q3_EARNED_PREMIUM_PD = Source.q3_pd,

		Target.EARNED_PREMIUM_BI = Source.q1_bi,
		Target.YEAR_EARNED_PREMIUM_BI = Source.year_bi,
		Target.Q2_EARNED_PREMIUM_BI = Source.q2_bi,
		Target.Q3_EARNED_PREMIUM_BI = Source.q3_bi,

		Target.EARNED_PREMIUM_CMP = Source.q1_cmp,
		Target.YEAR_EARNED_PREMIUM_CMP = Source.year_cmp,
		Target.Q2_EARNED_PREMIUM_CMP = Source.q2_cmp,
		Target.Q3_EARNED_PREMIUM_CMP = Source.q3_cmp,

		Target.EARNED_PREMIUM_COL = Source.q1_col,
		Target.YEAR_EARNED_PREMIUM_COL = Source.year_col,
		Target.Q2_EARNED_PREMIUM_COL = Source.q2_col,
		Target.Q3_EARNED_PREMIUM_COL = Source.q3_col

when not matched by Target then
insert (STATE, PROGRAM, CLUTCH, NB_RN, TYPE, GROUPING, COUNTY,
EARNED_PREMIUM_PIP, YEAR_EARNED_PREMIUM_PIP, Q2_EARNED_PREMIUM_PIP, Q3_EARNED_PREMIUM_PIP,
EARNED_PREMIUM_PD, YEAR_EARNED_PREMIUM_PD, Q2_EARNED_PREMIUM_PD, Q3_EARNED_PREMIUM_PD,
EARNED_PREMIUM_BI, YEAR_EARNED_PREMIUM_BI, Q2_EARNED_PREMIUM_BI, Q3_EARNED_PREMIUM_BI,
EARNED_PREMIUM_CMP, YEAR_EARNED_PREMIUM_CMP, Q2_EARNED_PREMIUM_CMP, Q3_EARNED_PREMIUM_CMP,
EARNED_PREMIUM_COL, YEAR_EARNED_PREMIUM_COL, Q2_EARNED_PREMIUM_COL, Q3_EARNED_PREMIUM_COL)
values (Source.state, Source.program, Source.CLUTCH, Source.NB_RN, Source.TYPE, Source.Grouping, Source.county,
	Source.q1_pip, Source.year_pip, Source.q2_pip, Source.q3_pip,
	Source.q1_pd, Source.year_pd, Source.q2_pd, Source.q3_pd,
	Source.q1_bi, Source.year_bi, Source.q2_bi, Source.q3_bi,
	Source.q1_cmp, Source.year_cmp, Source.q2_cmp, Source.q3_cmp,
	Source.q1_col, Source.year_col, Source.q2_col, Source.q3_col)
;

-- earned premium b2 data

merge Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT as Target
using (select state, coalesce(Program,'TOTAL') as program,
CLUTCH, NB_RN, TYPE, Grouping,county,
q1_pip,
year_pip,
q2_pip,
q3_pip,

q1_pd,
year_pd,
q2_pd,
q3_pd,

q1_bi,
year_bi,
q2_bi,
q3_bi,

q1_cmp,
year_cmp,
q2_cmp,
q3_cmp,

q1_col,
year_col,
q2_col,
q3_col
from (select 
'TX' as state,
Program,
'' as CLUTCH,
nb_rn as NB_RN,
@type as TYPE,
case @type 
when 'ZIP' then zipCode
when 'COUNTY' then texasCounty
when 'AGENT' then case when code is not null then concat(code,'-',subcode) else concat(agentNumber,'-000-000') end end as Grouping,
texasCounty as county,
sum(case when coverage = 'PIP' and reportEnd between @q1_first and @q1_last then earnedPremium end) as q1_pip,
sum(case when coverage = 'PIP' and reportEnd between @year_first and @q1_last then earnedPremium end) as year_pip,
sum(case when coverage = 'PIP' and reportEnd between @q2_first and @q2_last then earnedPremium end) as q2_pip,
sum(case when coverage = 'PIP' and reportEnd between @q3_first and @q3_last then earnedPremium end) as q3_pip,

sum(case when coverage = 'PD' and reportEnd between @q1_first and @q1_last then earnedPremium end) as q1_pd,
sum(case when coverage = 'PD' and reportEnd between @year_first and @q1_last then earnedPremium end) as year_pd,
sum(case when coverage = 'PD' and reportEnd between @q2_first and @q2_last then earnedPremium end) as q2_pd,
sum(case when coverage = 'PD' and reportEnd between @q3_first and @q3_last then earnedPremium end) as q3_pd,

sum(case when coverage = 'BI' and reportEnd between @q1_first and @q1_last then earnedPremium end) as q1_bi,
sum(case when coverage = 'BI' and reportEnd between @year_first and @q1_last then earnedPremium end) as year_bi,
sum(case when coverage = 'BI' and reportEnd between @q2_first and @q2_last then earnedPremium end) as q2_bi,
sum(case when coverage = 'BI' and reportEnd between @q3_first and @q3_last then earnedPremium end) as q3_bi,

sum(case when coverage = 'COM' and reportEnd between @q1_first and @q1_last then earnedPremium end) as q1_cmp,
sum(case when coverage = 'COM' and reportEnd between @year_first and @q1_last then earnedPremium end) as year_cmp,
sum(case when coverage = 'COM' and reportEnd between @q2_first and @q2_last then earnedPremium end) as q2_cmp,
sum(case when coverage = 'COM' and reportEnd between @q3_first and @q3_last then earnedPremium end) as q3_cmp,

sum(case when coverage = 'COL' and reportEnd between @q1_first and @q1_last then earnedPremium end) as q1_col,
sum(case when coverage = 'COL' and reportEnd between @year_first and @q1_last then earnedPremium end) as year_col,
sum(case when coverage = 'COL' and reportEnd between @q2_first and @q2_last then earnedPremium end) as q2_col,
sum(case when coverage = 'COL' and reportEnd between @q3_first and @q3_last then earnedPremium end) as q3_col

from Periscope_Data.B2_Data.Premium
left join Windhaven_Report.dbo.producer on importProducerCode = agentNumber
left join Periscope_Data.B2_Data.TxZips
on zipCode = texasZip
group by 
case @type 
when 'ZIP' then zipCode
when 'COUNTY' then texasCounty
when 'AGENT' then case when code is not null then concat(code,'-',subcode) else concat(agentNumber,'-000-000') end end, texasCounty,
nb_rn,
Program
 with rollup) b
 where state is not null and grouping is not null and county is not null and NB_RN is not null) as Source
 on (Target.State = Source.state and Target.PROGRAM = Source.program
	and Target.CLUTCH = Source.CLUTCH and Target.NB_RN = Source.NB_RN
	and Target.TYPE = Source.TYPE
	and Target.GROUPING = Source.Grouping and Target.COUNTY = Source.county)

 when matched then
	update set Target.EARNED_PREMIUM_PIP = coalesce(Target.EARNED_PREMIUM_PIP,0) + coalesce(Source.q1_pip,0),
		Target.YEAR_EARNED_PREMIUM_PIP = coalesce(Target.YEAR_EARNED_PREMIUM_PIP,0) + coalesce(Source.year_pip,0),
		Target.Q2_EARNED_PREMIUM_PIP = coalesce(Target.Q2_EARNED_PREMIUM_PIP,0) + coalesce(Source.q2_pip,0),
		Target.Q3_EARNED_PREMIUM_PIP = coalesce(Target.Q3_EARNED_PREMIUM_PIP,0) + coalesce(Source.q3_pip,0),

		Target.EARNED_PREMIUM_PD = coalesce(Target.EARNED_PREMIUM_PD,0) + coalesce(Source.q1_pd,0),
		Target.YEAR_EARNED_PREMIUM_PD = coalesce(Target.YEAR_EARNED_PREMIUM_PD,0) + coalesce(Source.year_pd,0),
		Target.Q2_EARNED_PREMIUM_PD = coalesce(Target.Q2_EARNED_PREMIUM_PD,0) + coalesce(Source.q2_pd,0),
		Target.Q3_EARNED_PREMIUM_PD = coalesce(Target.Q3_EARNED_PREMIUM_PD,0) + coalesce(Source.q3_pd,0),

		Target.EARNED_PREMIUM_BI = coalesce(Target.EARNED_PREMIUM_BI,0) + coalesce(Source.q1_bi,0),
		Target.YEAR_EARNED_PREMIUM_BI = coalesce(Target.YEAR_EARNED_PREMIUM_BI,0) + coalesce(Source.year_bi,0),
		Target.Q2_EARNED_PREMIUM_BI = coalesce(Target.Q2_EARNED_PREMIUM_BI,0) + coalesce(Source.q2_bi,0),
		Target.Q3_EARNED_PREMIUM_BI = coalesce(Target.Q3_EARNED_PREMIUM_BI,0) + coalesce(Source.q3_bi,0),

		Target.EARNED_PREMIUM_CMP = coalesce(Target.EARNED_PREMIUM_CMP,0) + coalesce(Source.q1_cmp,0),
		Target.YEAR_EARNED_PREMIUM_CMP = coalesce(Target.YEAR_EARNED_PREMIUM_CMP,0) + coalesce(Source.year_cmp,0),
		Target.Q2_EARNED_PREMIUM_CMP = coalesce(Target.Q2_EARNED_PREMIUM_CMP,0) + coalesce(Source.q2_cmp,0),
		Target.Q3_EARNED_PREMIUM_CMP = coalesce(Target.Q3_EARNED_PREMIUM_CMP,0) + coalesce(Source.q3_cmp,0),

		Target.EARNED_PREMIUM_COL = coalesce(Target.EARNED_PREMIUM_COL,0) + coalesce(Source.q1_col,0),
		Target.YEAR_EARNED_PREMIUM_COL = coalesce(Target.YEAR_EARNED_PREMIUM_COL,0) + coalesce(Source.year_col,0),
		Target.Q2_EARNED_PREMIUM_COL = coalesce(Target.Q2_EARNED_PREMIUM_COL,0) + coalesce(Source.q2_col,0),
		Target.Q3_EARNED_PREMIUM_COL = coalesce(Target.Q3_EARNED_PREMIUM_COL,0) + coalesce(Source.q3_col,0)

when not matched by Target then
insert (STATE, PROGRAM, CLUTCH, NB_RN, TYPE, GROUPING, COUNTY,
EARNED_PREMIUM_PIP, YEAR_EARNED_PREMIUM_PIP, Q2_EARNED_PREMIUM_PIP, Q3_EARNED_PREMIUM_PIP,
EARNED_PREMIUM_PD, YEAR_EARNED_PREMIUM_PD, Q2_EARNED_PREMIUM_PD, Q3_EARNED_PREMIUM_PD,
EARNED_PREMIUM_BI, YEAR_EARNED_PREMIUM_BI, Q2_EARNED_PREMIUM_BI, Q3_EARNED_PREMIUM_BI,
EARNED_PREMIUM_CMP, YEAR_EARNED_PREMIUM_CMP, Q2_EARNED_PREMIUM_CMP, Q3_EARNED_PREMIUM_CMP,
EARNED_PREMIUM_COL, YEAR_EARNED_PREMIUM_COL, Q2_EARNED_PREMIUM_COL, Q3_EARNED_PREMIUM_COL)
values (Source.state, Source.program, Source.CLUTCH, Source.NB_RN, Source.TYPE, Source.Grouping, Source.county,
	Source.q1_pip, Source.year_pip, Source.q2_pip, Source.q3_pip,
	Source.q1_pd, Source.year_pd, Source.q2_pd, Source.q3_pd,
	Source.q1_bi, Source.year_bi, Source.q2_bi, Source.q3_bi,
	Source.q1_cmp, Source.year_cmp, Source.q2_cmp, Source.q3_cmp,
	Source.q1_col, Source.year_col, Source.q2_col, Source.q3_col);

-- loss count data

merge Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT as Target
using (select state, coalesce(ProgName,'TOTAL') as program,
CLUTCH, NB_RN, TYPE, Grouping,county,
q1_pip, q2_pip, year_pip,
q1_pd, q2_pd, year_pd,
q1_bi, q2_bi, year_bi,
q1_cmp, q2_cmp, year_cmp,
q1_col, q2_col, year_col
from (select 
ProgState as state,
ProgName,
'' as CLUTCH,
case when policy.isRenewal = 0 then 'NB' else 'RN' end as NB_RN,
@type as TYPE,
case @type 
when 'ZIP' then address.zipcode
when 'COUNTY' then coalesce(texasCounty,address.county)
when 'AGENT' then concat(code,'-',subcode) end as Grouping,
coalesce(texasCounty,address.county) as county,
count(case when cov = 'PIP' and claim.dateopened between @q1_first and @q1_last then 1 end) as q1_pip,
count(case when cov = 'PIP' and claim.dateopened between @q2_first and @q2_last then 1 end) as q2_pip,
count(case when cov = 'PIP' and claim.dateopened between dateadd(day,1,@year_first) and @q1_last then 1 end) as year_pip,

count(case when cov = 'PD' and claim.dateopened between @q1_first and @q1_last then 1 end) as q1_pd,
count(case when cov = 'PD' and claim.dateopened between @q2_first and @q2_last then 1 end) as q2_pd,
count(case when cov = 'PD' and claim.dateopened between dateadd(day,1,@year_first) and @q1_last then 1 end) as year_pd,

count(case when cov = 'BI' and claim.dateopened between @q1_first and @q1_last then 1 end) as q1_bi,
count(case when cov = 'BI' and claim.dateopened between @q2_first and @q2_last then 1 end) as q2_bi,
count(case when cov = 'BI' and claim.dateopened between dateadd(day,1,@year_first) and @q1_last then 1 end) as year_bi,

count(case when cov = 'CMP' and claim.dateopened between @q1_first and @q1_last then 1 end) as q1_cmp,
count(case when cov = 'CMP' and claim.dateopened between @q2_first and @q2_last then 1 end) as q2_cmp,
count(case when cov = 'CMP' and claim.dateopened between dateadd(day,1,@year_first) and @q1_last then 1 end) as year_cmp,

count(case when cov = 'COL' and claim.dateopened between @q1_first and @q1_last then 1 end) as q1_col,
count(case when cov = 'COL' and claim.dateopened between @q2_first and @q2_last then 1 end) as q2_col,
count(case when cov = 'COL' and claim.dateopened between dateadd(day,1,@year_first) and @q1_last then 1 end) as year_col

 from 
(select claimNum, cov, min(dateChanged) as dateopened, min(claimincidentid) as claimincidentid
from (
select claim.claimNum, claim.claimincidentId,
case a.policyCoveragesId when 20 then 'COL' when 19 then 'CMP' when 11 then 'BI' when 12 then 'PD' when 22 then 'PIP'
	when 15 then 'UMPD' when 14 then 'UMBI' when 27 then 'RENT' when 24 then 'TOW' when 18 then 'UNPD'when 36 then 'UNBI' when 51 then 'CDW'
end as cov,
a.dateOpened as dateChanged
from Windhaven_Report.dbo.ClaimLog a
join Windhaven_Report.dbo.claim on a.claimID = claim.claimID
join Windhaven_Report.dbo.claimincident on claim.claimincidentId = claimincident.claimincidentId and catastropheID=0
union all 
select ClaimNumber as claimNum, claimincidentid,
replace(Coverage,'COM','CMP') as cov, TranDate as dateChanged
from Periscope_Data.B2_Data.Claims_Trans
left join Windhaven_Report.dbo.claim on ClaimNumber = claimNum 
) b
where cov <> '*'
group by claimNum, cov) claim
join [Windhaven_Report].dbo.claimincident on claim.claimincidentId = claimincident.claimincidentId
join [Windhaven_Report].dbo.policy on policy.policyId = claimincident.policyId
 join [Windhaven_Report].dbo.producer on policy.producerId = producer.producerId
 join (select vehicle.policyId, min(vehicleNumber) as minVeh
	from [Windhaven_Report].dbo.vehicle
	group by policyId) veh on veh.policyId = policy.policyId
 join [Windhaven_Report].dbo.vehicle on vehicle.policyId = policy.policyId and vehicleNumber = minVeh
 left join [Windhaven_Report].dbo.address on vehicle.garagingAddressId = address.id
join [Windhaven_Report].dbo.insured on policy.insuredID = insured.insuredId
join [Periscope_Data].dbo.ProgramNum on ProgNum = ratingProgram
	left join Periscope_Data.B2_Data.TxZips on texasZip = address.zipcode and coalesce(address.county,'') = ''
 group by 
ProgState,
case when policy.isRenewal = 0 then 'NB' else 'RN' end,
case @type 
when 'ZIP' then address.zipcode
when 'COUNTY' then coalesce(texasCounty,address.county)
when 'AGENT' then concat(code,'-',subcode) end, coalesce(texasCounty,address.county),
ProgName
 with rollup) b
 where state is not null and grouping is not null and county is not null and NB_RN is not null) as Source
on (Target.STATE = Source.state and Target.PROGRAM = Source.program
	and Target.CLUTCH = Source.CLUTCH and Target.NB_RN = Source.NB_RN and Target.TYPE = Source.TYPE
	and Target.GROUPING = Source.Grouping and Target.COUNTY = Source.county)
when matched then
	update set Target.Q1_PIP = Source.q1_pip, Target.Q2_PIP = Source.q2_pip, Target.YEAR_PIP = Source.year_pip,
		Target.Q1_PD = Source.q1_pd, Target.Q2_PD = Source.q2_pd, Target.YEAR_PD = Source.year_pd,
		Target.Q1_BI = Source.q1_bi, Target.Q2_BI = Source.q2_bi, Target.YEAR_BI = Source.year_bi,
		Target.Q1_CMP = Source.q1_cmp, Target.Q2_CMP = Source.q2_cmp, Target.YEAR_CMP = Source.year_cmp,
		Target.Q1_COL = Source.q1_col, Target.Q2_COL = Source.q2_col, Target.YEAR_COL = Source.year_col
when not matched by Target then
insert (STATE, PROGRAM, CLUTCH, NB_RN, TYPE, GROUPING, COUNTY,
	Q1_PIP, Q2_PIP, YEAR_PIP,
	Q1_PD, Q2_PD, YEAR_PD,
	Q1_BI, Q2_BI, YEAR_BI,
	Q1_CMP, Q2_CMP, YEAR_CMP,
	Q1_COL, Q2_COL, YEAR_COL)
values (Source.state, Source.program, Source.CLUTCH, Source.NB_RN, Source.TYPE,Source.Grouping, Source.County,
	Source.q1_pip, Source.q2_pip, Source.year_pip,
	Source.q1_pd, Source.q2_pd, Source.year_pd,
	Source.q1_bi, Source.q2_bi, Source.year_bi,
	Source.q1_cmp, Source.q2_cmp, Source.year_cmp,
	Source.q1_col, Source.q2_col, Source.year_col);
/*
-- loss amount data


merge Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT as Target
using () as Source
on (Target.STATE = Source. and Target.PROGRAM = Source.
	and Target.CLUTCH = Source. and Target.TYPE = Source.
	and Target.GROUPING = Source. and Target.COUNTY = Source.)
when matched then
	update set
when not matched by Target then
insert (STATE, PROGRAM, CLUTCH, TYPE, GROUPING, COUNTY,)
values ();
*/
-- quotes and sales data

-- silvervine
merge Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT as Target
using (select state, coalesce(ProgName,'TOTAL') as program,
CLUTCH, NB_RN, TYPE, Grouping,coalesce(county,'') as county,
q1_quotes, q1_sales, q2_quotes, q2_sales
from (select 
ProgState as state,
ProgName,
'' as CLUTCH,
'NB' as NB_RN,
@type as TYPE,
case @type 
when 'ZIP' then address.zipcode
when 'COUNTY' then coalesce(texasCounty,address.county)
when 'AGENT' then concat(code,'-',subcode) end as Grouping,
coalesce(texasCounty,address.county) as county,
count(distinct(
case when cast(applicationDate as date) between @q1_first
	and @q1_last then
	concat(policy.producerId,fname1,lname1,address.zipcode,convert(varchar(15),applicationDate,110))end)) as q1_quotes,

count(distinct(case when left(policyNum,1) <> 'Q'
	and applicationDate between @q1_first
	and @q1_last
	 then policyNum end)) as q1_sales,
count(distinct(
case when cast(applicationDate as date) between @q2_first
	and @q2_last then
	concat(policy.producerId,fname1,lname1,address.zipcode,convert(varchar(15),applicationDate,110))end)) as q2_quotes,

count(distinct(case when left(policyNum,1) <> 'Q'
	and applicationDate between @q2_first
	and @q2_last
	 then policyNum end)) as q2_sales

 from 
[Windhaven_Report].dbo.policy
 join [Windhaven_Report].dbo.producer on policy.producerId = producer.producerId
 join (select vehicle.policyId, min(vehicleNumber) as minVeh
	from [Windhaven_Report].dbo.vehicle
	group by policyId) veh on veh.policyId = policy.policyId
 join [Windhaven_Report].dbo.vehicle on vehicle.policyId = policy.policyId and vehicleNumber = minVeh
 left join [Windhaven_Report].dbo.address on vehicle.garagingAddressId = address.id
join [Windhaven_Report].dbo.insured on policy.insuredID = insured.insuredId
join [Periscope_Data].dbo.ProgramNum on ProgNum = ratingProgram
	left join Periscope_Data.B2_Data.TxZips on texasZip = address.zipcode and coalesce(address.county,'') = ''
	where applicationDate < '2017-06-06'
 group by 
ProgState,
case @type 
when 'ZIP' then address.zipcode
when 'COUNTY' then coalesce(texasCounty,address.county)
when 'AGENT' then concat(code,'-',subcode) end, coalesce(texasCounty,address.county),
ProgName
 with rollup) b
 where state is not null and grouping is not null and county is not null and NB_RN is not null) as Source
on (Target.STATE = Source.state and Target.PROGRAM = Source.program
	and Target.CLUTCH = Source.CLUTCH and Target.NB_RN = Source.NB_RN and Target.TYPE = Source.TYPE
	and Target.GROUPING = Source.Grouping and Target.COUNTY = Source.county)
when matched then
	update set Target.QUOTE_COUNT = Source.q1_quotes, Target.SALES_COUNT = Source.q1_sales,
		Target.Q2_QUOTE_COUNT = Source.q2_quotes, Target.Q2_SALES_COUNT = Source.q2_sales
when not matched by Target then
insert (STATE, PROGRAM, CLUTCH, NB_RN, TYPE, GROUPING, COUNTY,
	QUOTE_COUNT, SALES_COUNT, Q2_QUOTE_COUNT, Q2_SALES_COUNT)
values (Source.state, Source.program, Source.CLUTCH, Source.NB_RN, Source.TYPE,Source.Grouping, Source.County,
	Source.q1_quotes, Source.q1_sales, Source.q2_quotes, Source.q2_sales);

-- b2

merge Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT as Target
using (select state, coalesce(Program,'TOTAL') as program,
CLUTCH, NB_RN, TYPE, Grouping,coalesce(county,'') as county,
q1_quotes, q2_quotes
from (select 
'TX' as state,
Program,
'' as CLUTCH,
'NB' as NB_RN,
@type as TYPE,
case @type 
when 'ZIP' then zipCode
when 'COUNTY' then texasCounty
when 'AGENT' then case when code is not null then concat(code,'-',subcode) else concat(agentNumber,'-000-000') end end as Grouping,
texasCounty as county,
sum(case when reportEnd between @q1_first and @q1_last then quoteCount end) as q1_quotes,
sum(case when reportEnd between @q2_first and @q2_last then quoteCount end) as q2_quotes

from Periscope_Data.B2_Data.Quotes
left join Windhaven_Report.dbo.producer on importProducerCode = agentNumber
left join Periscope_Data.B2_Data.TxZips
on zipCode = texasZip
group by 
case @type 
when 'ZIP' then zipCode
when 'COUNTY' then texasCounty
when 'AGENT' then case when code is not null then concat(code,'-',subcode) else concat(agentNumber,'-000-000') end end, texasCounty,
Program) b
 where coalesce(grouping,'') <> '') as Source
 on (Target.State = Source.state and Target.PROGRAM = Source.program
	and Target.CLUTCH = Source.CLUTCH and Target.NB_RN = Source.NB_RN
	and Target.TYPE = Source.TYPE
	and Target.GROUPING = Source.Grouping and Target.COUNTY = Source.county)

 when matched then
	update set Target.QUOTE_COUNT = coalesce(Target.QUOTE_COUNT,0) + coalesce(Source.q1_quotes,0),
		Target.Q2_QUOTE_COUNT = coalesce(Target.Q2_QUOTE_COUNT,0) + coalesce(Source.q2_quotes,0)

when not matched by Target then
insert (STATE, PROGRAM, CLUTCH, NB_RN, TYPE, GROUPING, COUNTY,
QUOTE_COUNT, Q2_QUOTE_COUNT)
values (Source.state, Source.program, Source.CLUTCH, Source.NB_RN, Source.TYPE, Source.Grouping, Source.county,
	Source.q1_quotes, Source.q2_quotes);

-- sales
merge Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT as Target
using (select state, coalesce(Program,'TOTAL') as program,
CLUTCH, NB_RN, TYPE, Grouping,coalesce(county,'') as county,
q1_sales, q2_sales
from (select 
'TX' as state,
Program,
'' as CLUTCH,
'NB' as NB_RN,
@type as TYPE,
case @type 
when 'ZIP' then zipCode
when 'COUNTY' then texasCounty
when 'AGENT' then case when code is not null then concat(code,'-',subcode) else concat(agentNumber,'-000-000') end end as Grouping,
texasCounty as county,
sum(case when reportEnd between @q1_first and @q1_last then salesCount end) as q1_sales,
sum(case when reportEnd between @q2_first and @q2_last then salesCount end) as q2_sales

from Periscope_Data.B2_Data.Sales
left join Windhaven_Report.dbo.producer on importProducerCode = agentNumber
left join Periscope_Data.B2_Data.TxZips
on zipCode = texasZip
group by 
case @type 
when 'ZIP' then zipCode
when 'COUNTY' then texasCounty
when 'AGENT' then case when code is not null then concat(code,'-',subcode) else concat(agentNumber,'-000-000') end end, texasCounty,
Program) b
 where coalesce(grouping,'') <> '') as Source
 on (Target.State = Source.state and Target.PROGRAM = Source.program
	and Target.CLUTCH = Source.CLUTCH and Target.NB_RN = Source.NB_RN
	and Target.TYPE = Source.TYPE
	and Target.GROUPING = Source.Grouping and Target.COUNTY = Source.county)

 when matched then
	update set Target.SALES_COUNT = coalesce(Target.SALES_COUNT,0) + coalesce(Source.q1_sales,0),
		Target.Q2_SALES_COUNT = coalesce(Target.Q2_SALES_COUNT,0) + coalesce(Source.q2_sales,0)

when not matched by Target then
insert (STATE, PROGRAM, CLUTCH, NB_RN, TYPE, GROUPING, COUNTY,
SALES_COUNT, Q2_SALES_COUNT)
values (Source.state, Source.program, Source.CLUTCH, Source.NB_RN, Source.TYPE, Source.Grouping, Source.county,
	Source.q1_sales, Source.q2_sales);


-- qq
merge Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT as Target
using (select state, coalesce(Program,'TOTAL') as program,
CLUTCH, NB_RN, TYPE, Grouping,coalesce(county,'') as county,
q1_quotes, q2_quotes
from (select 
'TX' as state,
Program,
'' as CLUTCH,
'NB' as NB_RN,
@type as TYPE,
case @type 
when 'ZIP' then zipCode
when 'COUNTY' then texasCounty
when 'AGENT' then case when code is not null then concat(code,'-',subcode) else left(concat(agentNumber,'-000-000'),14) end end as Grouping,
texasCounty as county,
sum(case when reportEnd between @q1_first and @q1_last then quoteCount end) as q1_quotes,
sum(case when reportEnd between @q2_first and @q2_last then quoteCount end) as q2_quotes

from Periscope_Data.qqData.quotes
left join Windhaven_Report.dbo.producer on importProducerCode = agentNumber
left join Periscope_Data.B2_Data.TxZips
on zipCode = texasZip
group by 
case @type 
when 'ZIP' then zipCode
when 'COUNTY' then texasCounty
when 'AGENT' then case when code is not null then concat(code,'-',subcode) else left(concat(agentNumber,'-000-000'),14) end end, texasCounty,
Program) b
 where coalesce(grouping,'') <> '') as Source
 on (Target.State = Source.state and Target.PROGRAM = Source.program
	and Target.CLUTCH = Source.CLUTCH and Target.NB_RN = Source.NB_RN
	and Target.TYPE = Source.TYPE
	and Target.GROUPING = Source.Grouping and Target.COUNTY = Source.county)

 when matched then
	update set Target.QUOTE_COUNT = coalesce(Target.QUOTE_COUNT,0) + coalesce(Source.q1_quotes,0),
		Target.Q2_QUOTE_COUNT = coalesce(Target.Q2_QUOTE_COUNT,0) + coalesce(Source.q2_quotes,0)

when not matched by Target then
insert (STATE, PROGRAM, CLUTCH, NB_RN, TYPE, GROUPING, COUNTY,
QUOTE_COUNT, Q2_QUOTE_COUNT)
values (Source.state, Source.program, Source.CLUTCH, Source.NB_RN, Source.TYPE, Source.Grouping, Source.county,
	Source.q1_quotes, Source.q2_quotes);

	set @i = @i+1;
end;

insert into Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT
select STATE, PROGRAM, CLUTCH, NB_RN, 'AGENT GROUP',
concat(left(GROUPING,6),'-',(select count(*) from Windhaven_Report.dbo.producer where code = left(GROUPING,6) and cancelledDate is null),' Locations')
AS AGENT_GROUPING,
COUNTY,
SUM(EARNED_PREMIUM_PIP), SUM(EARNED_PREMIUM_PD), SUM(EARNED_PREMIUM_BI), SUM(EARNED_PREMIUM_CMP), SUM(EARNED_PREMIUM_COL),
SUM(OL_EARNED_PREMIUM_PIP), SUM(OL_EARNED_PREMIUM_PD), SUM(OL_EARNED_PREMIUM_BI), SUM(OL_EARNED_PREMIUM_CMP), SUM(OL_EARNED_PREMIUM_COL),
SUM(Q1_PIP), SUM(Q1_PD), SUM(Q1_BI), SUM(Q1_CMP), SUM(Q1_COL),
SUM(YEAR_EARNED_PREMIUM_PIP), SUM(YEAR_EARNED_PREMIUM_PD), SUM(YEAR_EARNED_PREMIUM_BI), 
	SUM(YEAR_EARNED_PREMIUM_CMP), SUM(YEAR_EARNED_PREMIUM_COL),
SUM(OL_YEAR_EARNED_PREMIUM_PIP), SUM(OL_YEAR_EARNED_PREMIUM_PD), SUM(OL_YEAR_EARNED_PREMIUM_BI),
	SUM(OL_YEAR_EARNED_PREMIUM_CMP), SUM(OL_YEAR_EARNED_PREMIUM_COL),
SUM(YEAR_PIP), SUM(YEAR_PD), SUM(YEAR_BI), SUM(YEAR_CMP), SUM(YEAR_COL),
SUM(YEAR_PIP_INCUR), SUM(YEAR_PD_INCUR), SUM(YEAR_BI_INCUR), SUM(YEAR_CMP_INCUR), SUM(YEAR_COL_INCUR),
SUM(QUOTE_COUNT), SUM(SALES_COUNT),
SUM(Q2_EARNED_PREMIUM_PIP), SUM(Q2_EARNED_PREMIUM_PD), SUM(Q2_EARNED_PREMIUM_BI),
	SUM(Q2_EARNED_PREMIUM_CMP), SUM(Q2_EARNED_PREMIUM_COL),
SUM(OL_Q2_EARNED_PREMIUM_PIP), SUM(OL_Q2_EARNED_PREMIUM_PD), SUM(OL_Q2_EARNED_PREMIUM_BI), 
	SUM(OL_Q2_EARNED_PREMIUM_CMP), SUM(OL_Q2_EARNED_PREMIUM_COL),
SUM(Q2_PIP), SUM(Q2_PD), SUM(Q2_BI), SUM(Q2_CMP), SUM(Q2_COL),
SUM(Q2_QUOTE_COUNT), SUM(Q2_SALES_COUNT),
SUM(Q3_EARNED_PREMIUM_PIP), SUM(Q3_EARNED_PREMIUM_PD), SUM(Q3_EARNED_PREMIUM_BI),
	SUM(Q3_EARNED_PREMIUM_CMP), SUM(Q3_EARNED_PREMIUM_COL),
SUM(OL_Q3_EARNED_PREMIUM_PIP), SUM(OL_Q3_EARNED_PREMIUM_PD), SUM(OL_Q3_EARNED_PREMIUM_BI),
	SUM(OL_Q3_EARNED_PREMIUM_CMP), SUM(OL_Q3_EARNED_PREMIUM_COL),
null,1,1,1,1,1
from Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT
where TYPE = 'AGENT'
GROUP BY STATE, PROGRAM, CLUTCH, NB_RN, 
left(GROUPING,6),
COUNTY
having (select count(*) from Windhaven_Report.dbo.producer where code = left(GROUPING,6) and cancelledDate is null) > 0

update Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT
set DATA_AS_OF = @q1_last;

select * from Periscope_Data.dbo.MONTHLY_PRICING_MEETING_EXCLCAT;


GO

