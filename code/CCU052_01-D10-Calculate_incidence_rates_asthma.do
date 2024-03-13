*Calculate rates -asthma
**************************************************
*1) OVERALL RATES PER MONTH 
**************************************************
*1a) Crude rates ////////////////////////////////

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 

sort month_year 
by month_year: egen overall_numerator=total(numerator)
by month_year: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/44{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}

twoway (line rate_est month_year)
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\1a_crude_asthma_rates", replace 

export delimited month_year overall_numerator overall_denom rate_est rate_se rate_lb rate_ub  using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\1a_crude_asthma_rates.csv" , datafmt replace 

save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\1a_crude_asthma_rates", replace 

export delimited month_year overall_numerator overall_denom rate_est rate_se rate_lb rate_ub  using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\1a_crude_asthma_rates.csv" , datafmt replace 


*1b) Age & sex adjusted overall monthly rates //////////////////////////////

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 

sort month_year SEX age_group
by month_year SEX age_group: egen overall_numerator=total(numerator)
by month_year SEX age_group: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year SEX age_group overall_numerator overall_denom
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_weights.dta"
drop _m


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort month_year
by month_year: egen step2=total(age_sex_standardised_rate)

*sum denominator 
sort month_year
by month_year: egen step3=total(europeanstandardpopulation)

*calculate rates iwth new numerator (step2) and denominator (step3)
keep month_year  step2  step3
duplicates drop 
gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/44{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}

twoway (line rate_est month_year)

save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\1b_adj_asthma_rates", replace 

export delimited month_year  rate_est rate_lb rate_ub using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\1b_adj_asthma_rates.csv" , datafmt replace 


 
 ********************************************************
*2) OVERALL SEX STRATIFIED RATES PER MONTH 
********************************************************
*2a) Crude rates stratified by sex in children and in adults/////////////////////

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 
*keep children
keep if age_group<5

sort month_year SEX 
by month_year SEX: egen overall_numerator=total(numerator)
by month_year SEX: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year SEX overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

*44 months * 2 sexes=88
forvalues i=1/88{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\2a_crude_asthma_rates_by_sex_children", replace 

export delimited month_year SEX overall_numerator overall_denom rate_est rate_se rate_lb rate_ub  using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\2a_crude_asthma_rates_by_sex_children.csv" , datafmt replace 

*********adults 
use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 
*keep adults
keep if age_group>=5

sort month_year SEX 
by month_year SEX: egen overall_numerator=total(numerator)
by month_year SEX: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year SEX overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

*44 months * 2 sexes=88
forvalues i=1/88{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\2a_crude_asthma_rates_by_sex_adults", replace 

export delimited month_year SEX overall_numerator overall_denom rate_est rate_se rate_lb rate_ub  using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\2a_crude_asthma_rates_by_sex_adults.csv" , datafmt replace 

***all 
use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 

sort month_year SEX 
by month_year SEX: egen overall_numerator=total(numerator)
by month_year SEX: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year SEX overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

*44 months * 2 sexes=88
forvalues i=1/88{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\2a_crude_asthma_rates_by_sex", replace 

export delimited month_year SEX overall_numerator overall_denom rate_est rate_se rate_lb rate_ub  using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\2a_crude_asthma_rates_by_sex.csv" , datafmt replace 


*2b) Age adjusted sex stratified monthly rates //////////////////////////
	
use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 
*keep children 
keep if age_group<5

sort month_year age_group SEX 
by month_year age_group SEX: egen overall_numerator=total(numerator)
by month_year age_group SEX : egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days


keep month_year SEX age_group overall_numerator overall_denom
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_weights_children.dta"
drop _m


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
*keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort month_year sex
by month_year sex: egen step2=total(age_sex_standardised_rate)

*sum expected denom
sort month_year sex
by month_year sex: egen step3=total(europeanstandardpopulation)


*calculate rates wiwth new numerator (step2) and denominator (step3)
keep month_year sex step3 step2  
duplicates drop 

gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/88{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}

twoway (line rate_est month_year if sex==1, lcolor(blue)) (line rate_est month_year if sex==2, lcolor(red))
	
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\2b_adj_asthma_rates_by_sex_children", replace 

export delimited month_year sex rate_est rate_lb rate_ub using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\2b_adj_asthma_rates_by_sex_children.csv" , datafmt replace 


***adults 
use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 
*keep adults
keep if age_group>=5

sort month_year age_group SEX 
by month_year age_group SEX: egen overall_numerator=total(numerator)
by month_year age_group SEX : egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days


keep month_year SEX age_group overall_numerator overall_denom
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_weights_adults.dta"
drop _m


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
*keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort month_year sex
by month_year sex: egen step2=total(age_sex_standardised_rate)

*sum expected denom
sort month_year sex
by month_year sex: egen step3=total(europeanstandardpopulation)


*calculate rates wiwth new numerator (step2) and denominator (step3)
keep month_year sex step3 step2  
duplicates drop 

gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/88{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}

twoway (line rate_est month_year if sex==1, lcolor(blue)) (line rate_est month_year if sex==2, lcolor(red))
	
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\2b_adj_asthma_rates_by_sex_adults", replace 

export delimited month_year sex rate_est rate_lb rate_ub using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\2b_adj_asthma_rates_by_sex_adults.csv" , datafmt replace 

***all 
use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 

sort month_year age_group SEX 
by month_year age_group SEX: egen overall_numerator=total(numerator)
by month_year age_group SEX : egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days


keep month_year SEX age_group overall_numerator overall_denom
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_weights.dta"
drop _m


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
*keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort month_year sex
by month_year sex: egen step2=total(age_sex_standardised_rate)

*sum expected denom
sort month_year sex
by month_year sex: egen step3=total(europeanstandardpopulation)


*calculate rates wiwth new numerator (step2) and denominator (step3)
keep month_year sex step3 step2  
duplicates drop 

gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/88{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}

twoway (line rate_est month_year if sex==1, lcolor(blue)) (line rate_est month_year if sex==2, lcolor(red))
	
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\2b_adj_asthma_rates_by_sex", replace 

export delimited month_year sex rate_est rate_lb rate_ub using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\2b_adj_asthma_rates_by_sex.csv" , datafmt replace 

 
 ********************************************************
*3) OVERALL Age STRATIFIED RATES PER MONTH 
********************************************************
*3a) Crude rates stratified by age group /////////////////////in men and women? 

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 
*in men 
keep if SEX=="1"
sort month_year age_group 
by month_year age_group: egen overall_numerator=total(numerator)
by month_year age_group: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year age_group overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

*44 months * 10 age groups=440
forvalues i=1/440{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\3a_crude_asthma_rates_by_age_men", replace 

export delimited month_year overall_numerator overall_denom age_group rate_est rate_se rate_lb rate_ub  using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\3a_crude_asthma_rates_by_age_men.csv" , datafmt replace 

*in women ************************

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 
*in women 
keep if SEX=="2"
sort month_year age_group 
by month_year age_group: egen overall_numerator=total(numerator)
by month_year age_group: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year age_group overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

*44 months * 10 age groups=440
forvalues i=1/440{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\3a_crude_asthma_rates_by_age_women", replace 

export delimited month_year overall_numerator overall_denom age_group rate_est rate_se rate_lb rate_ub  using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\3a_crude_asthma_rates_by_age_women.csv" , datafmt replace 

*overall

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 

sort month_year age_group 
by month_year age_group: egen overall_numerator=total(numerator)
by month_year age_group: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year age_group overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

*44 months * 10 age groups=440
forvalues i=1/440{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\3a_crude_asthma_rates_by_age", replace 

export delimited month_year overall_numerator overall_denom age_group rate_est rate_se rate_lb rate_ub  using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\3a_crude_asthma_rates_by_age.csv" , datafmt replace 


*3b) Sex adjusted age stratified monthly rates //////////////////////////

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 

sort month_year SEX age_group
by month_year SEX age_group: egen overall_numerator=total(numerator)
by month_year SEX age_group: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year SEX age_group overall_numerator overall_denom
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_weights.dta"
drop _m


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
*keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort month_year age_group
by month_year age_group: egen step2=total(age_sex_standardised_rate)

*sum expected denominator
sort month_year age_group
by month_year age_group: egen step3=total(europeanstandardpopulation)


*calculate rates wiwth new numerator (step2) and denominator (step3)
keep month_year age_group step3 step2  
duplicates drop 

gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/440{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}

twoway line rate_est month_year, by(age_group)



save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\3b_adj_asthma_rates_by_age", replace 

export delimited month_year age_group rate_est rate_lb rate_ub using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\3b_adj_asthma_rates_by_age.csv" , datafmt replace 


********************************************************
*4) OVERALL REGION STRATIFIED RATES PER MONTH 
********************************************************
*4a) Crude rates stratified by region /////////////////////


use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 

sort month_year region 
by month_year region: egen overall_numerator=total(numerator)
by month_year region: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year region overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

*44 months * 9 regions=396
forvalues i=1/396{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\4a_crude_asthma_rates_by_region", replace 

export delimited month_year overall_numerator overall_denom region rate_est rate_se rate_lb rate_ub  using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\4a_crude_asthma_rates_by_region.csv" , datafmt replace 


*4b) Age & sex adjusted region stratified monthly rates //////////////////////////

use  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_asthma_rates_all", clear 

sort month_year SEX age_group region
by month_year SEX age_group region: egen overall_numerator=total(numerator)
by month_year SEX age_group region: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year SEX age_group overall_numerator overall_denom region
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\age_sex_weights.dta"
drop _m


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
*keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort month_year region
by month_year region: egen step2=total(age_sex_standardised_rate)

*sum expected denominator
sort month_year region
by month_year region: egen step3=total(europeanstandardpopulation)


*calculate rates wiwth new numerator (step2) and denominator (step3)
keep month_year region step3 step2  
duplicates drop 

gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/396{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}

twoway line rate_est month_year, by(region)

twoway (line rate_est month_year if region==1, lcolor(red) legend(label(1 "East midlands"))) (line rate_est month_year if region==2, lcolor(orange) legend(label( 2 "East England"))) (line rate_est month_year if region==3, lcolor(yellow) legend(label(3 "London"))) (line rate_est month_year if region==4, lcolor(green) legend(label(4 "NE"))) (line rate_est month_year if region==5, lcolor(blue) legend(label(5 "NW"))) (line rate_est month_year if region==6,lcolor(indigo) legend(label(6 "SE"))) (line rate_est month_year if region==7, lcolor(purple) legend(label(7 "SW"))) (line rate_est month_year if region==8, lcolor(pink) legend(label(8 "W midlands"))) (line rate_est month_year if region==9, lcolor(black) legend(label(9 "Yorkshire")))


save  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\4b_adj_asthma_rates_by_region", replace 

export delimited month_year region rate_est rate_lb rate_ub using  "D:\PhotonUser\My Files\Home Folder\StataFiles\CCU052\Final_rates\4b_adj_asthma_rates_by_region.csv" , datafmt replace 

