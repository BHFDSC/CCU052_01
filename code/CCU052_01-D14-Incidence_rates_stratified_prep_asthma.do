*Asthma INCIDENCE RATES- STRATIFIED by age, gender, region

****************************************************************
*Numerator
***************************************************************

forvalues i=1/10{
use "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\chunk`i'.dta", clear 

*Generate a month/year variable from the date of incident COPD/asthma etc- this will be the grouping variable for numberator counts 

gen out_asthma_date=out_asthma_gdppr_date
replace out_asthma_date=out_asthma_hes_apc_date if out_asthma_hes_apc_date<out_asthma_gdppr_date
format out_asthma_date %td 

gen out_asthma_flag=1 if out_asthma_date!=.

keep if out_asthma_flag==1 
codebook PERSON_ID 
codebook PERSON_ID if cov_hx_out_asthma_flag==. 
keep if cov_hx_out_asthma_flag==.

gen month_date=month(out_asthma_date) 
gen year_date=year(out_asthma_date)
gen day_date=1
gen month_year=mdy(month_date, day_date, year_date)
format month_year %td

sort month_year
gen age=month_year-DOB
replace age=age/365.25

gen age_group=1 if age>=0 & age<5
replace age_group=2 if age>=5 & age<10
replace age_group=3 if age>=10 & age<15
replace age_group=4 if age>=15 & age<20
replace age_group=5 if age>=20 & age<30
replace age_group=6 if age>=30 & age<40
replace age_group=7 if age>=40 & age<50
replace age_group=8 if age>=50 & age<60
replace age_group=9 if age>=60 & age<70
replace age_group=10 if age>=70 

label define lab_age 1"0-4" 2"5-9" 3"10-14" 4"15-19" 5"20-30" 6"30-40" 7"40-50" 8"50-60" 9"60-70" 10"70+" 
label values age_group lab_age 


*stratify by sex, age, region
sort month_year SEX region age_group 
by month_year SEX region age_group: gen litn=_n
by month_year SEX region age_group: gen bign=_N 
keep if litn==bign 
drop litn 
rename bign tot_asthma_`i'
keep month_year SEX region age_group tot_asthma_`i'  
save "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_age_sex_numerator_chunk`i'", replace 
}

*Merge all together and sum counts from each count for each month
use "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_age_sex_numerator_chunk1", clear 
forvalues i=2/10{
	merge 1:1 month_year SEX region age_group using "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_age_sex_numerator_chunk`i'"
	drop _m
}

sort month_year	SEX region age_group
forvalues i=1/10{
	replace tot_asthma_`i'=0 if tot_asthma_`i'==.
}

gen numerator=tot_asthma_1 +tot_asthma_2+ tot_asthma_3+ tot_asthma_4+ tot_asthma_5+ tot_asthma_6+ tot_asthma_7 +tot_asthma_8+ tot_asthma_9+ tot_asthma_10
drop tot_asthma_1 tot_asthma_2 tot_asthma_3 tot_asthma_4 tot_asthma_5 tot_asthma_6 tot_asthma_7 tot_asthma_8 tot_asthma_9 tot_asthma_10
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_age_sex_numerator_all", replace 

*******************************************************************
*Denominator
*******************************************************************

forvalues x=1/10{
use "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\chunk`x'.dta", clear 

gen month0=date("01/11/2019", "DMY")

gen month1=date("01/12/2019", "DMY")

gen month2=date("01/01/2020", "DMY")
gen month3=date("01/02/2020", "DMY")
gen month4=date("01/03/2020", "DMY")
gen month5=date("01/04/2020", "DMY")
gen month6=date("01/05/2020", "DMY")
gen month7=date("01/06/2020", "DMY")
gen month8=date("01/07/2020", "DMY")
gen month9=date("01/08/2020", "DMY")
gen month10=date("01/09/2020", "DMY")
gen month11=date("01/10/2020", "DMY")
gen month12=date("01/11/2020", "DMY")
gen month13=date("01/12/2020", "DMY")

gen month14=date("01/01/2021", "DMY")
gen month15=date("01/02/2021", "DMY")
gen month16=date("01/03/2021", "DMY")
gen month17=date("01/04/2021", "DMY")
gen month18=date("01/05/2021", "DMY")
gen month19=date("01/06/2021", "DMY")
gen month20=date("01/07/2021", "DMY")
gen month21=date("01/08/2021", "DMY")
gen month22=date("01/09/2021", "DMY")
gen month23=date("01/10/2021", "DMY")
gen month24=date("01/11/2021", "DMY")
gen month25=date("01/12/2021", "DMY")

gen month26=date("01/01/2022", "DMY")
gen month27=date("01/02/2022", "DMY")
gen month28=date("01/03/2022", "DMY")
gen month29=date("01/04/2022", "DMY")
gen month30=date("01/05/2022", "DMY")
gen month31=date("01/06/2022", "DMY")
gen month32=date("01/07/2022", "DMY")
gen month33=date("01/08/2022", "DMY")
gen month34=date("01/09/2022", "DMY")
gen month35=date("01/10/2022", "DMY")
gen month36=date("01/11/2022", "DMY")
gen month37=date("01/12/2022", "DMY")
gen month38=date("01/01/2023", "DMY")

gen month39=date("01/02/2023", "DMY")
gen month40=date("01/03/2023", "DMY")
gen month41=date("01/04/2023", "DMY")
gen month42=date("01/05/2023", "DMY")
gen month43=date("01/06/2023", "DMY")
gen month44=date("01/07/2023", "DMY")

format month0-month44 %td

gen out_asthma_date=out_asthma_gdppr_date
replace out_asthma_date=out_asthma_hes_apc_date if out_asthma_hes_apc_date<out_asthma_gdppr_date
format out_asthma_date %td 

gen out_asthma_flag=1 if out_asthma_date!=.


forvalues i= 0/43{
	local j=`i'+1
preserve
keep if cov_hx_out_asthma_flag==.
keep if fu_end_date>month`i' 
drop if out_asthma_date<month`i'

gen censor_date=month`j' 
replace censor_date=fu_end_date if fu_end_date>month`i' & fu_end_date<month`j'
replace censor_date=out_asthma_date if out_asthma_date>month`i' & out_asthma_date<month`j'
format censor_date %td

gen person_time=censor_date-month`i'
keep PERSON_ID month`i' SEX region DOB person_time

*****gen age=month_year-DOB
gen age=month`i'-DOB
replace age=age/365.25

gen age_group=1 if age>=0 & age<5
replace age_group=2 if age>=5 & age<10
replace age_group=3 if age>=10 & age<15
replace age_group=4 if age>=15 & age<20
replace age_group=5 if age>=20 & age<30
replace age_group=6 if age>=30 & age<40
replace age_group=7 if age>=40 & age<50
replace age_group=8 if age>=50 & age<60
replace age_group=9 if age>=60 & age<70
replace age_group=10 if age>=70 

label define lab_age 1"0-4" 2"5-9" 3"10-14" 4"15-19" 5"20-30" 6"30-40" 7"40-50" 8"50-60" 9"60-70" 10"70+" 
label values age_group lab_age 

*****

sort SEX region age_group 
by SEX region age_group: gen tot_person_time=sum(person_time)
by SEX region age_group: gen litn=_n
by SEX region age_group: gen bign=_N 
keep if litn==bign 
keep month`i' tot_person_time SEX region age_group
rename month`i' month_year
duplicates drop 
save "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_chunk_`x'_age_sex_denom_month`i'", replace 
restore 
}

use "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_chunk_`x'_age_sex_denom_month0", clear 
forvalues i=1/43{
append using "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_chunk_`x'_age_sex_denom_month`i'"
}
save "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_age_sex_denom_all_chunk_`x'", replace 
}
 
forvalue i=1/10{
	use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_age_sex_denom_all_chunk_`i'", clear
	rename tot_person_time tot_person_time`i'
save "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_age_sex_denom_all_chunk_`i'", replace 
}

*************************************************
*Format numerator and denomintor to calculate rates
*************************************************
use "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_age_sex_denom_all_chunk_1", clear 
forvalues i=2/10{ 
	merge 1:1 month_year SEX region age_group using "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_age_sex_denom_all_chunk_`i'"
	drop _m
}

gen tot_person_time=tot_person_time1+ tot_person_time2+ tot_person_time3+ tot_person_time4 +tot_person_time5+ tot_person_time6+ tot_person_time7 +tot_person_time8 +tot_person_time9 + tot_person_time10
gen tot_person_time_str=tot_person_time
tostring tot_person_time_str, replace 
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_age_sex_denom_all", replace 

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_age_sex_numerator_all", clear 
merge 1:1 month_year SEX region age_group using "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\asthma_age_sex_denom_all"
drop _m
recode numerator .=0
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", replace 

gen region2=. 
replace region2=1 if region=="East Midlands"
replace region2=2 if region=="East of England"
replace region2=3 if region=="London"
replace region2=4 if region=="North East"
replace region2=5 if region=="North West"
replace region2=6 if region=="South East"
replace region2=7 if region=="South West"
replace region2=8 if region=="West Midlands"
replace region2=9 if region=="Yorkshire and The Humber"
label define lab_region 1"East midlands" 2"East of England" 3"London" 4"North east" 5"North west" 6"South east" 7"South west" 8"West midlands" 9"Yorkshire and the humber"
label values region2 lab_region 
drop region 
rename region2 region

keep if region!=.
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", replace 




