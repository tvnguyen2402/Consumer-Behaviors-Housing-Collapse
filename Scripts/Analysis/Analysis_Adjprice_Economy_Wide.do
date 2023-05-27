clear
cap log close
set more off

global rawdata	"/Users/tvnguyen@middlebury.edu/Desktop/Econthesis/RAWDATA"
global mainpath "/Users/tvnguyen@middlebury.edu/Desktop/Econthesis"

capture mkdir "$mainpath/DATAIN"
global datain "$mainpath/DATAIN"

capture mkdir "$mainpath/DATAOUT"
global dataout "$mainpath/DATAOUT"

capture  mkdir "$mainpath/OUTPUT"
global output "$mainpath/OUTPUT"

/* Define global */
global filename beer blades carbbev cigets coffee coldcer deod diapers factiss fzdinent fzpizza hhclean hotdog laundet margbutr mayo milk mustketc paptowl peanbutr photo razors saltsnck shamp soup spagsauc sugarsub toitisu toothbr toothpa yogurt

**************************************************
** Generate the store-UPC level quarterly price **
**************************************************
local weeks3 "1218_1269"
local weeks4 "1270_1321" 
local weeks5 "1322_1373" 
local weeks6 "1374_1426"
local weeks7 "1427_1478"
local weeks8 "1479_1530" 
local weeks9 "1531_1582" 
local weeks10 "1583_1634"
local weeks11 "1635_1686"
local weeks12 "1687_1739"

tokenize $filename 

forvalues k=12/12{

forvalues i=1(1)31 {
	
	use $dataout/y`k'/merged_demo_``i''_`weeks`k''2.dta, clear
	
	drop if IRI_KEY == .

	** obtain time **
	gen year = year(Calendarweekendingon)
	gen month = month(Calendarweekendingon)
	
	** generate price for items **
	gen PRICE = DOLLARS/UNITS
    
	** economy-wide price **
	bysort SY GE VEND ITEM: egen yr_prod_price = mean(PRICE)
	bysort SY GE VEND ITEM: gen id = _n
	keep if id ==1
	drop id 

	keep SY GE VEND ITEM year yr_prod_price
	
	rename yr_prod_price y`k'_yr_prod_price
	
	save $datain/yr_prod_price_``i''_y`k'_v2.dta, replace
}
}

************************************
** Merge retail sales with prices **
************************************

** 2007 **

local weeks3 "1218_1269"
local weeks4 "1270_1321" 
local weeks5 "1322_1373" 
local weeks6 "1374_1426"
local weeks7 "1427_1478"
local weeks8 "1479_1530" 
local weeks9 "1531_1582" 
local weeks10 "1583_1634"
local weeks11 "1635_1686"
local weeks12 "1687_1739"


tokenize $filename 

forvalues k=7/7{

forvalues i=1(1)31 {
	
	use $dataout/y`k'/merged_demo_``i''_`weeks`k''.dta, clear
	
	drop if IRI_KEY == .

	gen year = year(Calendarweekendingon)

	** Need to merge in prices **
	gen PRICE = DOLLARS/UNITS
	
	bysort IRI_KEY SY GE VEND ITEM: egen store_yr_prod_pch = sum(DOLLARS)
	bysort IRI_KEY SY GE VEND ITEM: egen store_yr_prod_price = mean(PRICE)
	bysort IRI_KEY SY GE VEND ITEM: gen id = _n
	keep if id ==1
	drop id 
	
	** 2008 **
	merge m:1 SY GE VEND ITEM using $datain/econ_wide_p/yr_prod_price_``i''_y8_v2.dta
	*merge m:1 SY GE VEND ITEM using $datain/cbsa_yr_prod_price_beer_y8_v2.dta
	keep if _m != 2
	drop _m
	
	** 2009 **
	merge m:1 SY GE VEND ITEM using $datain/econ_wide_p/yr_prod_price_``i''_y9_v2.dta
	*merge m:1 SY GE VEND ITEM using $datain/cbsa_yr_prod_price_beer_y9_v2.dta
	keep if _m != 2
	drop _m
	
	** 2010 **
	merge m:1 SY GE VEND ITEM using $datain/econ_wide_p/yr_prod_price_``i''_y10_v2.dta
	*merge m:1 SY GE VEND ITEM using $datain/cbsa_yr_prod_price_beer_y10_v2.dta
	keep if _m != 2
	drop _m
	
	** 2011 **
	merge m:1 SY GE VEND ITEM using $$datain/econ_wide_p/yr_prod_price_``i''_y11_v2.dta
	*merge m:1 SY GE VEND ITEM using $datain/cbsa_yr_prod_price_beer_y11_v2.dta
	keep if _m != 2
	drop _m
	
	** 2011 **
	merge m:1 SY GE VEND ITEM using $$datain/econ_wide_p/yr_prod_price_``i''_y12_v2.dta
	*merge m:1 SY GE VEND ITEM using $datain/cbsa_yr_prod_price_beer_y11_v2.dta
	keep if _m != 2
	drop _m

	drop WEEK UNITS DOLLARS F D1 PR PRICE
	
	gen y12_price = y12_yr_prod_price
	replace y12_price = y11_yr_prod_price if y12_yr_prod_price == .
	replace y12_price = y10_yr_prod_price if y11_yr_prod_price == .
	replace y12_price = y9_yr_prod_price if y10_yr_prod_price == .
	replace y12_price = y8_yr_prod_price if y9_yr_prod_price == .
	replace y12_price = store_yr_prod_price if y8_yr_prod_price == .
	
	gen store_yr_prod_pch_y11_price = store_yr_prod_pch*y11_price/store_yr_prod_price
	
	** aggregate to the store-level retail sales (yearly) **
	bysort IRI_KEY year: egen store_yr_tot_pch = sum(store_yr_prod_pch_y11_price)
	bysort IRI_KEY year: gen id = _n
	keep if id == 1
	drop id 
	
	drop SY GE VEND ITEM Calendarweekstartingon Calendarweekendingon store_yr_prod_pch store_yr_prod_price y11_yr_prod_price y10_yr_prod_price y9_yr_prod_price y8_yr_prod_price y11_price  store_yr_prod_pch_y11_price
	
	save $dataout/merged_demo_``i''_y`k'_prices_v2.dta, replace
}
}

** 2008 **

local weeks3 "1218_1269"
local weeks4 "1270_1321" 
local weeks5 "1322_1373" 
local weeks6 "1374_1426"
local weeks7 "1427_1478"
local weeks8 "1479_1530" 
local weeks9 "1531_1582" 
local weeks10 "1583_1634"
local weeks11 "1635_1686"
local weeks12 "1687_1739"


tokenize $filename 

forvalues k=8/8{

forvalues i=1(1)31 {
	
	use $dataout/y`k'/merged_demo_``i''_`weeks`k''.dta, clear
	
	drop if IRI_KEY == .

	gen year = year(Calendarweekendingon)

	** Need to merge in prices **
	gen PRICE = DOLLARS/UNITS
	
	bysort IRI_KEY SY GE VEND ITEM: egen store_yr_prod_pch = sum(DOLLARS)
	bysort IRI_KEY SY GE VEND ITEM: egen store_yr_prod_price = mean(PRICE)
	bysort IRI_KEY SY GE VEND ITEM: gen id = _n
	keep if id ==1
	drop id 
	
	** 2009 **
	merge m:1 SY GE VEND ITEM using $datain/econ_wide_p/yr_prod_price_``i''_y9_v2.dta
	keep if _m != 2
	drop _m
	
	** 2010 **
	merge m:1 SY GE VEND ITEM using $datain/econ_wide_p/yr_prod_price_``i''_y10_v2.dta
	keep if _m != 2
	drop _m
	
	** 2011 **
	merge m:1 SY GE VEND ITEM using $datain/econ_wide_p/yr_prod_price_``i''_y11_v2.dta
	keep if _m != 2
	drop _m
	
	** 2012 **
	merge m:1 SY GE VEND ITEM using $datain/econ_wide_p/yr_prod_price_``i''_y12_v2.dta
	keep if _m != 2
	drop _m


	drop WEEK UNITS DOLLARS F D1 PR PRICE
	
	gen y12_price = y12_yr_prod_price
	replace y12_price = y11_yr_prod_price if y12_yr_prod_price == .
	replace y12_price = y10_yr_prod_price if y11_yr_prod_price == .
	replace y12_price = y9_yr_prod_price if y10_yr_prod_price == .
	replace y12_price = store_yr_prod_price if y9_yr_prod_price == .
	
	gen store_yr_prod_pch_y12_price = store_yr_prod_pch*y12_price/store_yr_prod_price
	
	** aggregate to the store-level retail sales (yearly) **
	bysort IRI_KEY year: egen store_yr_tot_pch = sum(store_yr_prod_pch_y12_price)
	bysort IRI_KEY year: gen id = _n
	keep if id == 1
	drop id 
	
	drop SY GE VEND ITEM Calendarweekstartingon Calendarweekendingon store_yr_prod_pch store_yr_prod_price y12_yr_prod_price y11_yr_prod_price y10_yr_prod_price y9_yr_prod_price y12_price store_yr_prod_pch_y12_price
	
	save $dataout/merged_demo_``i''_y`k'_prices_v2.dta, replace
}
}

** 2012 **


local weeks3 "1218_1269"
local weeks4 "1270_1321" 
local weeks5 "1322_1373" 
local weeks6 "1374_1426"
local weeks7 "1427_1478"
local weeks8 "1479_1530" 
local weeks9 "1531_1582" 
local weeks10 "1583_1634"
local weeks11 "1635_1686"
local weeks12 "1687_1739"


tokenize $filename 

forvalues k=12/12{

forvalues i=1(1)31 {
	
	use $dataout/y`k'/merged_demo_``i''_`weeks`k''2.dta, clear
	
	drop if IRI_KEY == .

	gen year = year(Calendarweekendingon)

	bysort IRI_KEY year: egen store_yr_tot_pch = sum(DOLLARS)
	bysort IRI_KEY year: gen id = _n
	keep if id == 1
	drop id 
	
	drop WEEK UNITS DOLLARS F D1 PR 
	
	drop SY GE VEND ITEM Calendarweekstartingon Calendarweekendingon 
	
	save $dataout/merged_demo_``i''_y`k'_prices_v2.dta, replace
}

}

*************************************
** Append years of the same product *
*************************************

tokenize $filename 

forvalues i=1(1)31 {
	
	use $dataout/merged_demo_``i''_y8_prices_v2, clear
	append using $dataout/merged_demo_``i''_y12_prices_v2.dta
	
	save $dataout/store_yr_``i''_y12price.dta, replace
}

***********************************************
** Merge different categories items together **
***********************************************

tokenize $filename 

use $dataout/store_yr_beer_y12price.dta, clear

forvalues i=2(1)31 {
	append using $dataout/store_yr_``i''_y12price.dta
}

save $dataout/store_yr_all_y12price, replace

*****************************************************************
** Aggregate category retail data to yearly aggregate level **
*****************************************************************

use $dataout/store_yr_all_y12price, clear

bysort IRI_KEY year: egen store_yr_all_pch = sum(store_yr_tot_pch)
bysort IRI_KEY year: gen id = _n
keep if id ==1
drop id 

drop store_yr_tot_pch

label var IRI_KEY "Store Code"
label var year "Year"
label var store_yr_all_pch "Yearly Store Sales"

save $dataout/store_yr_all_aggr_y12price, replace

*************************************
** Merge in CBSA and MSA Crosswalk **
*************************************

use $dataout/store_yr_all_aggr_y12price, clear
		
*merge retail data with zip code - cbsa crosswalk **
		
*merge m:1 County_Key using $datain/cbsa_counties.dta
merge m:1 County_Key using $datain/cbsa_counties.dta
drop if _m == 2
drop _m
	
*merge retail data with cbsa - msa crosswalk **
		
merge m:1 cbsa_code using $datain/cbsa_msa.dta
drop if _m == 2
drop _m

save $dataout/store_yr_all_aggr_y12price2, replace

************************************************************
** Merge Store-level Retail data with Housing Wealth Data **
************************************************************

use $dataout/store_yr_all_aggr_y12price2_v2, clear

/*
merge m:1 County_Key year using $dataout/zillow_hw_yr_cty.dta
drop if _m == 2
drop _m
*/

merge m:1 County_Key year using $datain/kaplan_cty_wealth.dta
drop if _m == 2
drop _m

** Merge in ACS 2010 Total Number of Households

merge m:1 County_Key using $datain/acs_cty_tot_hh.dta
drop if _m == 2
drop _m 


************************************************
** Merge in MSA Employment Shares by Industry **
************************************************

** From Strobel and Vavra (2019) **
merge m:1 County_Key year using $datain/cty_empshare.dta
keep if _m != 2
drop _m

************************
** Homeownership Rate **
************************

merge m:1 County_Key using $datain/homeownership.dta

keep if _m != 2
drop _m

save $dataout/store_yr_retail_cty_housingw_y12price_v2.dta, replace

***********************************************
** Compute the change in Housing Wealth 07-11 *
***********************************************

use $dataout/store_yr_retail_cty_housingw_y11price_v2.dta, clear

** Generate ind. and dep. variables **

keep if year == 2006 | year == 2007 | year == 2008 | year == 2010 | year == 2011

** Generate ind. and dep. variables **

gen log_cty_06_housingw = log(cty_06_housingw)
gen log_cty_07_housingw = log(cty_07_housingw)
gen log_cty_08_housingw = log(cty_08_housingw)
gen log_cty_10_housingw = log(cty_10_housingw)
gen log_cty_11_housingw = log(cty_11_housingw)

gen c_cty_06_11_housingw = log_cty_11_housingw - log_cty_06_housingw
gen c_cty_07_11_housingw = log_cty_11_housingw - log_cty_07_housingw
gen c_cty_07_10_housingw = log_cty_10_housingw - log_cty_07_housingw
gen c_cty_08_11_housingw = log_cty_11_housingw - log_cty_08_housingw


*********************************************
** Compute the change in Expenditures 08-11 *
*********************************************

bysort IRI_KEY year: gen pch07 = store_yr_all_pch if year == 2007
bysort IRI_KEY: egen store_07_all_pch = mean(pch07)
drop pch07

bysort IRI_KEY year: gen pch08 = store_yr_all_pch if year == 2008
bysort IRI_KEY: egen store_08_all_pch = mean(pch08)
drop pch08

bysort IRI_KEY year: gen pch10 = store_yr_all_pch if year == 2010
bysort IRI_KEY: egen store_10_all_pch = mean(pch10)
drop pch10

bysort IRI_KEY year: gen pch11 = store_yr_all_pch if year == 2011
bysort IRI_KEY: egen store_11_all_pch = mean(pch11)
drop pch11

gen log_store_07_pch = log(store_07_all_pch)
gen log_store_08_pch = log(store_08_all_pch)
gen log_store_10_pch = log(store_10_all_pch)
gen log_store_11_pch = log(store_11_all_pch)

gen c_store_07_11_pch = log_store_11_pch - log_store_07_pch
gen c_store_07_10_pch = log_store_10_pch - log_store_07_pch
gen c_store_08_10_pch = log_store_10_pch - log_store_08_pch
gen c_store_08_11_pch = log_store_11_pch - log_store_08_pch

**************
** Controls **
**************

** Housing Wealth **

gen cty_hh_hw_07 = cty_07_housingw/(1000*cty_tot_hh)
gen cty_hh_hw_08 = cty_08_housingw/(1000*cty_tot_hh)

** Share of housing wealth in household wealth **

bysort IRI_KEY: gen hw08 = H_v0 if year == 2008
bysort IRI_KEY: egen cty_08_hw_v2 = mean(hw08)
drop hw08

bysort IRI_KEY: gen nw08 = (NW_v0) if year == 2008
bysort IRI_KEY: egen cty_08_nw_v2 = mean(nw08)
drop nw08

gen cty_08_hw_share = cty_08_hw_v2/cty_08_nw_v2

**

bysort IRI_KEY: gen hw07 = H_v0 if year == 2007
bysort IRI_KEY: egen cty_07_hw_v2 = mean(hw07)
drop hw07

bysort IRI_KEY: gen nw07 = (NW_v0) if year == 2007
bysort IRI_KEY: egen cty_07_nw_v2 = mean(nw07)
drop nw07

gen cty_07_hw_share = cty_07_hw_v2/cty_07_nw_v2

**

bysort IRI_KEY: gen hw06 = H_v0 if year == 2006
bysort IRI_KEY: egen cty_06_hw_v2 = mean(hw06)
drop hw06

bysort IRI_KEY: gen nw06 = (NW_v0) if year == 2006
bysort IRI_KEY: egen cty_06_nw_v2 = mean(nw06)
drop nw06

gen cty_06_hw_share = cty_06_hw_v2/cty_06_nw_v2

** housing Leverage **

bysort IRI_KEY: gen hlev07 = cty_yr_hlev if year == 2007
bysort IRI_KEY: egen cty_07_hlev = mean(hlev07)
drop hlev07

bysort IRI_KEY: gen hlev08 = cty_yr_hlev if year == 2008
bysort IRI_KEY: egen cty_08_hlev = mean(hlev08)
drop hlev08

** Mortgage to Income Ratio **
bysort IRI_KEY: gen totdebt08 = cty_yr_tot_debt if year == 2008
bysort IRI_KEY: egen cty_08_debt = mean(totdebt08)
drop totdebt08

gen cty_08_debt_inc = cty_08_debt/cty_08_inc

bysort IRI_KEY: gen mrtg08 = cty_yr_mrtg if year == 2008
bysort IRI_KEY: egen cty_08_mrtg = mean(mrtg08)
drop mrtg08

gen cty_08_mrtg_inc = cty_08_mrtg/cty_08_inc


** Weekly Wage **

bysort IRI_KEY: gen wage07 = qcew_all_wkly_wage if year == 2007
bysort IRI_KEY: egen cty_07_wwage = mean(wage07)
drop wage07

bysort IRI_KEY: gen wage08 = qcew_all_wkly_wage if year == 2008
bysort IRI_KEY: egen cty_08_wwage = mean(wage08)
drop wage08

bysort IRI_KEY: gen wage10 = qcew_all_wkly_wage if year == 2010
bysort IRI_KEY: egen cty_10_wwage = mean(wage10)
drop wage10

bysort IRI_KEY: gen wage11 = qcew_all_wkly_wage if year == 2011
bysort IRI_KEY: egen cty_11_wwage = mean(wage11)
drop wage11

gen c_cty_07_11_wwage = cty_11_wwage - cty_07_wwage
gen c_cty_08_11_wwage = cty_11_wwage - cty_08_wwage

** Unemployment Rate **

bysort IRI_KEY: gen uempr07 = unemp_rate_laus if year == 2007
bysort IRI_KEY: egen cty_07_uempr = mean(uempr07)
drop uempr07

bysort IRI_KEY: gen uempr08 = unemp_rate_laus if year == 2008
bysort IRI_KEY: egen cty_08_uempr = mean(uempr08)
drop uempr08

bysort IRI_KEY: gen uempr10 = unemp_rate_laus if year == 2010
bysort IRI_KEY: egen cty_10_uempr = mean(uempr10)
drop uempr10

bysort IRI_KEY: gen uempr11 = unemp_rate_laus if year == 2011
bysort IRI_KEY: egen cty_11_uempr = mean(uempr11)
drop uempr11


gen c_cty_07_11_uempr = cty_11_uempr - cty_07_uempr
gen c_cty_08_11_uempr = cty_11_uempr - cty_08_uempr


** Construction share **


bysort IRI_KEY: gen construction07 = constructionshare if year == 2007
bysort IRI_KEY: egen cty_07_construction = mean(construction07)
drop construction07

bysort IRI_KEY: gen construction08 = constructionshare if year == 2008
bysort IRI_KEY: egen cty_08_construction = mean(construction08)
drop construction08

bysort IRI_KEY: gen construction11 = constructionshare if year == 2011
bysort IRI_KEY: egen cty_11_construction = mean(construction11)
drop construction11

gen c_cty_07_11_construction = cty_11_construction - cty_07_construction
gen c_cty_08_11_construction = cty_11_construction - cty_08_construction

** Grocery Retail share **

bysort IRI_KEY: gen retail07 = foodanddrugshare if year == 2007
bysort IRI_KEY: egen cty_07_retail = mean(retail07)
drop retail07

bysort IRI_KEY: gen retail08 = foodanddrugshare if year == 2008
bysort IRI_KEY: egen cty_08_retail = mean(retail08)
drop retail08

bysort IRI_KEY: gen retail11 = foodanddrugshare if year == 2011
bysort IRI_KEY: egen cty_11_retail = mean(retail11)
drop retail11

gen c_cty_07_11_retail = cty_11_retail - cty_07_retail
gen c_cty_08_11_retail = cty_11_retail - cty_08_retail

** Non-tradeshare **

bysort IRI_KEY: gen nontrade07 = nontradeshare if year == 2007
bysort IRI_KEY: egen cty_07_nontrade = mean(nontrade07)
drop nontrade07

bysort IRI_KEY: gen nontrade08 = nontradeshare if year == 2008
bysort IRI_KEY: egen cty_08_nontrade = mean(nontrade08)
drop nontrade08

bysort IRI_KEY: gen nontrade11 = nontradeshare if year == 2011
bysort IRI_KEY: egen cty_11_nontrade = mean(nontrade11)
drop nontrade11

gen c_cty_07_11_nontrade = cty_11_nontrade - cty_07_nontrade
gen c_cty_08_11_nontrade = cty_11_nontrade - cty_08_nontrade

** Homeownership **

bysort IRI_KEY: gen hos07 = homeowner1 if year == 2007
bysort IRI_KEY: egen cty_07_home = mean(hos07)
drop hos07

bysort IRI_KEY: gen hos08 = homeowner1 if year == 2008
bysort IRI_KEY: egen cty_08_home = mean(hos08)
drop hos08

**************************
** Store-level Data set **
**************************

bysort IRI_KEY: gen id = _n
keep if id == 1
drop id 

******************************
** Merge in Saiz Instrument **
******************************

keep if msanecma != . 

*merge m:1 msanecma using $datain/saiz_instrument.dta
merge m:1 cbsa_code using $datain/instruments.dta
drop if _m == 2
drop _m

gen elasticity2 = elasticity^2
gen elasticity3 = elasticity^3
gen elasticity4 = elasticity^4

label var elasticity2 "Housing Supply Elasticity (Quadratic)"
label var elasticity3 "Housing Supply Elasticity (Cubic)"

save $dataout/store_yr_cretail_cty_chousingw_y11price_inst_v2.dta, replace


*************************
** Regression Analysis **
*************************
********************
** OLS Regression **
********************

use $dataout/store_yr_cretail_cty_chousingw_y11price_inst_v2.dta, clear
cd $output

keep if c_store_08_11_pch !=. & elasticity !=. & c_cty_08_11_housingw != . & cty_hh_inc_08 != . & cty_hh_hw_08 != . & cty_08_hlev != .  &  cty_08_construction != . & cty_08_retail != . & cty_08_nontrade != . & log_store_08_pch != . & c_cty_08_11_wwage !=. & c_cty_08_11_uempr !=.

gen cty_hh_inc_08_v2 = cty_hh_inc_08/1000
gen cty_hh_hw_08_v2 = cty_hh_hw_08/1000
gen cty_08_wwage_v2 = cty_08_wwage/100
gen c_cty_08_11_wwage_v2 = c_cty_08_11_wwage/100

local controls1 = "cty_hh_inc_08_v2 cty_hh_hw_08_v2 cty_08_hlev cty_08_construction cty_08_retail cty_08_nontrade "

local controls2 = "c_cty_08_11_wwage_v2 c_cty_08_11_uempr  cty_hh_inc_08_v2 cty_hh_hw_08_v2 cty_08_hlev cty_08_construction cty_08_retail cty_08_nontrade "

reg c_store_08_11_pch c_cty_08_11_housingw [aw = log_store_08_pch], cluster(County_Key) 
outreg2 using store_saiz_y11_v2, replace

reg c_store_08_11_pch c_cty_08_11_housingw `controls2' [aw = log_store_08_pch], cluster(County_Key)
outreg2 using store_saiz_y11_v2, append

********************************
** Second-stage IV regression **
********************************

use $dataout/store_yr_cretail_cty_chousingw_y11price_inst_v2.dta, clear
cd $output

keep if c_store_08_11_pch !=. & elasticity !=. & c_cty_08_11_housingw != . & cty_hh_inc_08 != . & cty_hh_hw_08 != . & cty_08_hlev != .  &  cty_08_construction != . & cty_08_retail != . & cty_08_nontrade != . & log_store_08_pch != . & c_cty_08_11_wwage !=. & c_cty_08_11_uempr !=.

gen cty_hh_inc_08_v2 = cty_hh_inc_08/1000
gen cty_hh_hw_08_v2 = cty_hh_hw_08/1000
gen cty_08_wwage_v2 = cty_08_wwage/100
gen c_cty_08_11_wwage_v2 = c_cty_08_11_wwage/100

local controls1 = "cty_hh_inc_08_v2 cty_hh_hw_08_v2 cty_08_hlev cty_08_construction cty_08_retail cty_08_nontrade "

local controls2 = "c_cty_08_11_wwage_v2 c_cty_08_11_uempr  cty_hh_inc_08_v2 cty_hh_hw_08_v2 cty_08_hlev cty_08_construction cty_08_retail cty_08_nontrade "

// Linear Instrument 

ivreghdfe c_store_08_11_pch (c_cty_08_11_housingw = elasticity)  [aw = log_store_08_pch], cluster(County_Key) 
outreg2 using store_saiz_y11_v2, append

ivreghdfe c_store_08_11_pch (c_cty_08_11_housingw = elasticity) `controls1' [aw = log_store_08_pch], cluster(County_Key) 
outreg2 using store_saiz_y11_v2, append

ivreghdfe c_store_08_11_pch (c_cty_08_11_housingw = elasticity) `controls2' [aw = log_store_08_pch], cluster(County_Key) 
outreg2 using store_saiz_y11_v2, append



// Non-Linear Instrument
ivreghdfe c_store_08_11_pch (c_cty_08_11_housingw = elasticity*)  [aw = log_store_08_pch], cluster(County_Key) 
outreg2 using store_saiz_y11_v2, append

ivreghdfe c_store_08_11_pch (c_cty_08_11_housingw = elasticity*) `controls1' [aw = log_store_08_pch], cluster(County_Key)
outreg2 using store_saiz_y11_v2, append

ivreghdfe c_store_08_11_pch (c_cty_08_11_housingw = elasticity*) `controls2' [aw = log_store_08_pch], cluster(County_Key) 
outreg2 using store_saiz_y11_v2, append

//2007 

// Linear Instrument 

ivreg2 c_store_08_11_pch (c_cty_07_11_housingw = elasticity)  [aw = log_store_08_pch], cluster(County_Key) 
weakivtest
outreg2 using store_saiz_y11_v2, append

ivreg2 c_store_08_11_pch (c_cty_07_11_housingw = elasticity) `controls1' [aw = log_store_08_pch], cluster(County_Key) 
weakivtest
outreg2 using store_saiz_y11_v2, append

ivreg2 c_store_08_11_pch (c_cty_07_11_housingw = elasticity) `controls2' [aw = log_store_08_pch], cluster(County_Key) 
weakivtest
outreg2 using store_saiz_y11_v2, append



// Non-Linear Instrument
ivreg2 c_store_08_11_pch (c_cty_07_11_housingw = elasticity*)  [aw = log_store_08_pch], cluster(County_Key)
weakivtest 
outreg2 using store_saiz_y11_v2, append

ivreg2 c_store_08_11_pch (c_cty_07_11_housingw = elasticity*) `controls1' [aw = log_store_08_pch], cluster(County_Key)
weakivtest
outreg2 using store_saiz_y11_v2, append

ivreg2 c_store_08_11_pch (c_cty_07_11_housingw = elasticity*) `controls2' [aw = log_store_08_pch], cluster(County_Key) 
weakivtest
outreg2 using store_saiz_y11_v2, append

//2006

// Linear Instrument 

ivreghdfe c_store_08_11_pch (c_cty_06_11_housingw = elasticity)  [aw = log_store_08_pch], cluster(County_Key) 
outreg2 using store_saiz_y11_v2, append

ivreghdfe c_store_08_11_pch (c_cty_06_11_housingw = elasticity) `controls1' [aw = log_store_08_pch], cluster(County_Key) 
outreg2 using store_saiz_y11_v2, append

ivreghdfe c_store_08_11_pch (c_cty_06_11_housingw = elasticity) `controls2' [aw = log_store_08_pch], cluster(County_Key) 
outreg2 using store_saiz_y11_v2, append



// Non-Linear Instrument
ivreghdfe c_store_08_11_pch (c_cty_06_11_housingw = elasticity*)  [aw = log_store_08_pch], cluster(County_Key) 
outreg2 using store_saiz_y11_v2, append

ivreghdfe c_store_08_11_pch (c_cty_06_11_housingw = elasticity*) `controls1' [aw = log_store_08_pch], cluster(County_Key)
outreg2 using store_saiz_y11_v2, append

ivreghdfe c_store_08_11_pch (c_cty_06_11_housingw = elasticity*) `controls2' [aw = log_store_08_pch], cluster(County_Key) 
outreg2 using store_saiz_y11_v2, append




************************************
** Merge retail sales with prices **
************************************

** 2008 **

local weeks3 "1218_1269"
local weeks4 "1270_1321" 
local weeks5 "1322_1373" 
local weeks6 "1374_1426"
local weeks7 "1427_1478"
local weeks8 "1479_1530" 
local weeks9 "1531_1582" 
local weeks10 "1583_1634"
local weeks11 "1635_1686"
local weeks12 "1687_1739"


tokenize $filename 

forvalues k=8/8{

forvalues i=1(1)31 {
	
	use $dataout/y`k'/merged_demo_``i''_`weeks`k''.dta, clear
	
	drop if IRI_KEY == .

	gen year = year(Calendarweekendingon)

	** Need to merge in prices **
	gen PRICE = DOLLARS/UNITS
	
	bysort IRI_KEY SY GE VEND ITEM: egen store_yr_prod_pch = sum(DOLLARS)
	bysort IRI_KEY SY GE VEND ITEM: egen store_yr_prod_price = mean(PRICE)
	bysort IRI_KEY SY GE VEND ITEM: gen id = _n
	keep if id ==1
	drop id 
	
	** 2009 **
	merge m:1 SY GE VEND ITEM using $datain/cbsa_yr_prod_price_``i''_y9_v2.dta
	*merge m:1 SY GE VEND ITEM using $datain/cbsa_yr_prod_price_beer_y9_v2.dta
	keep if _m != 2
	drop _m
	
	** 2010 **
	merge m:1 SY GE VEND ITEM using $datain/cbsa_yr_prod_price_``i''_y10_v2.dta
	*merge m:1 SY GE VEND ITEM using $datain/cbsa_yr_prod_price_beer_y10_v2.dta
	keep if _m != 2
	drop _m
	
	** 2011 **
	merge m:1 SY GE VEND ITEM using $datain/cbsa_yr_prod_price_``i''_y11_v2.dta
	*merge m:1 SY GE VEND ITEM using $datain/cbsa_yr_prod_price_beer_y11_v2.dta
	keep if _m != 2
	drop _m

	drop WEEK UNITS DOLLARS F D1 PR PRICE
	
	gen y11_price = y11_yr_prod_price
	replace y11_price = y10_yr_prod_price if y11_yr_prod_price == .
	replace y11_price = y9_yr_prod_price if y10_yr_prod_price == .
	replace y11_price = store_yr_prod_price if y9_yr_prod_price == .
	
	save $dataout/``i''_y`k'_y11prices_v2.dta, replace
}
}


tokenize $filename 

use $dataout/beer_y8_y11prices_v2.dta, clear

forvalues i=2(1)31 {
	append using $dataout/``i''_y8_y11prices_v2.dta
}

save $dataout/all_y11prices_v2, replace

** Merge quality **

gen y11 = y11_yr_prod_price != .
sum y11

gen y10 = y10_yr_prod_price != .
sum y10

gen y9 = y9_yr_prod_price != .
sum y9
