*Calculate rates 
**************************************************
 
 ********************************************************
*1) OVERALL IMD STRATIFIED RATES PER MONTH 
********************************************************
*1a) Crude rates stratified by IMD /////////////////////

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_COPD_rates_all_imd_ethnic", clear 

sort month_year IMD_2019_DECILES 
by month_year IMD_2019_DECILES: egen overall_numerator=total(numerator)
by month_year IMD_2019_DECILES: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year IMD_2019_DECILES overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

*44 months * 11 sexes=484
forvalues i=1/484{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\crude_COPD_rates_by_imd", replace 

export delimited month_year  overall_numerator overall_denom IMD_2019_DECILES rate_est rate_se rate_lb rate_ub  using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\crude_COPD_rates_by_imd.csv" , datafmt replace 

*2a) Crude rates stratified by ethnicity /////////////////////

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_COPD_rates_all_imd_ethnic", clear 

sort month_year ethnicity 
by month_year ethnicity: egen overall_numerator=total(numerator)
by month_year ethnicity: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year ethnicity overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

*44 months * 6 ethnic groups=264
forvalues i=1/264{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\crude_COPD_rates_by_ethnic", replace 

export delimited month_year  overall_numerator overall_denom ethnicity rate_est rate_se rate_lb rate_ub  using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\crude_COPD_rates_by_ethnic.csv" , datafmt replace 

******************************************************
*1b) Age & sex adjusted IMD stratified monthly rates //////////////////////////

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_COPD_rates_all_imd_ethnic", clear 

sort month_year SEX age_group IMD_2019_DECILES
by month_year SEX age_group IMD_2019_DECILES: egen overall_numerator=total(numerator)
by month_year SEX age_group IMD_2019_DECILES: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year SEX age_group overall_numerator overall_denom IMD_2019_DECILES
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_weights_40plus.dta"
drop _m


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
*keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort month_year IMD_2019_DECILES
by month_year IMD_2019_DECILES: egen step2=total(age_sex_standardised_rate)

*sum expected denominator
sort month_year IMD_2019_DECILES
by month_year IMD_2019_DECILES: egen step3=total(europeanstandardpopulation)


*calculate rates wiwth new numerator (step2) and denominator (step3)
keep month_year IMD_2019_DECILES step3 step2  
duplicates drop 

gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/484{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}

twoway line rate_est month_year, by(IMD_2019_DECILES)

save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\adj_COPD_rates_by_imd", replace 

export delimited month_year IMD_2019_DECILES rate_est rate_lb rate_ub using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\adj_COPD_rates_by_imd.csv" , datafmt replace 


******************************************************
*2b) Age & sex adjusted ethnicity stratified monthly rates //////////////////////////

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_COPD_rates_all_imd_ethnic", clear 

sort month_year SEX age_group ethnicity
by month_year SEX age_group ethnicity: egen overall_numerator=total(numerator)
by month_year SEX age_group ethnicity: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year SEX age_group overall_numerator overall_denom ethnicity
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_weights_40plus.dta"
drop _m


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
*keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort month_year ethnicity
by month_year ethnicity: egen step2=total(age_sex_standardised_rate)

*sum expected denominator
sort month_year ethnicity
by month_year ethnicity: egen step3=total(europeanstandardpopulation)


*calculate rates wiwth new numerator (step2) and denominator (step3)
keep month_year ethnicity step3 step2  
duplicates drop 

gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/264{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}

twoway line rate_est month_year, by(ethnicity)

save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\adj_COPD_rates_by_ethnic", replace 

export delimited month_year ethnicity rate_est rate_lb rate_ub using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\adj_COPD_rates_by_ethnic.csv" , datafmt replace 


