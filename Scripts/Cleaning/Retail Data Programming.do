

********************************************************
*** Tai Nguyen -- October 2021						 *** 
*** This code cleans IRI retail data 			     *** 
*** Department of Economics -- Middlebury College    *** 
********************************************************

clear
cap log close
set more off

***********************
** Define file paths **
***********************

global mainpath "/Users/tvnguyen/Desktop/Research 2023/Unpacking Housing Wealth Effect"
global rawdata	"$mainpath/DATA/RAW"

capture mkdir "$mainpath/DATA/INPUT"
global input "$mainpath/DATA/INPUT"

capture mkdir "$mainpath/DATA/TEMP"
global temp "$mainpath/DATA/TEMP"

capture  mkdir "$mainpath/OUTPUT"
global output "$mainpath/OUTPUT"

global filename beer blades carbbev cigets coffee coldcer deod diapers factiss fzdinent fzpizza hhclean hotdog laundet margbutr mayo milk mustketc paptowl peanbutr photo razors saltsnck shamp soup spagsauc sugarsub toitisu toothbr toothpa yogurt 

local weeks2    "1166_1217"
local weeks3    "1218_1269"
local weeks4 	"1270_1321" 
local weeks5 	"1322_1373" 
local weeks6 	"1374_1426"
local weeks7 	"1427_1478"
local weeks8 	"1479_1530" 
local weeks9 	"1531_1582" 
local weeks10 	"1583_1634"
local weeks11 	"1635_1686"
local weeks12 	"1687_1739"

*******************************************************************
** Import and clean raw retail data by product category and year **
*******************************************************************

/* All Drug Stores */

forvalues k=2(1)12 {
	
	tokenize $filename
	
	forvalues i=1/31 {
		import delimited $rawdata/RawRetail/``i''_drug_`weeks`k''.dat, delimiter(whitespace, collapse)case(preserve) colrange(:12) clear
		
		gen F1 = real(F)
		gen D1 = real(D)
		order F1, b(F)
		order D1, b(PR)

		/*initate the leftward shift for the first variable*/
		replace IRI_KEY = WEEK if IRI_KEY ==.

		unab vars : * 
		tokenize `vars' 

/*reiterate the leftward shift for every variable from the second variable to the eighth variable*/

		forval l = 2/8 {
			local m = `l'+1
			local n = `l'-1
			replace ``l''=``m'' if ``l''==``n'' & v12 !=.
		}

		replace F=D if F1 == DOLLARS
		replace D1 = PR if D1 ==.
		replace PR = v12 if v12 !=.
		
		drop F1 D v12
		
		tokenize $filename
		save $temp/``i''_drug_`weeks`k''.dta, replace

	}
}

/* All Grocery Stores */

forvalues k=2(1)2{
	
	tokenize $filename
	
	forvalues i=1/31 {
		import delimited $rawdata/RawRetail/``i''_groc_`weeks`k''.dat, delimiter(whitespace, collapse)case(preserve) colrange(:12) clear
		
		gen F1 = real(F)
		gen D1 = real(D)
		order F1, b(F)
		order D1, b(PR)


		/*initate the leftward shift for the first variable*/
		replace IRI_KEY = WEEK if IRI_KEY ==.

		unab vars : * 
		tokenize `vars' 

/*reiterate the leftward shift for every variable from the second variable to the eighth variable*/

		forval l = 2/8 {
			local m = `l'+1
			local n = `l'-1
			replace ``l''=``m'' if ``l''==``n'' & v12 !=.
		}

		replace F=D if F1 == DOLLARS
		replace D1 = PR if D1 ==.
		replace PR = v12 if v12 !=.
		
		drop F1 D v12
		
		tokenize $filename
		save $temp/``i''_groc_`weeks`k''.dta, replace

	}
}

*************************************************************************
*Data Programming *** Append Data from Groceries Stores and Drug Stores *
*************************************************************************

local weeks2  "1166_1217"
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

forvalues k=2(1)12 {
	tokenize $filename
	forvalues i=1/31 {
	use $datain/``i''_groc_`weeks`k''.dta, replace
	append using $datain/``i''_drug_`weeks`k''.dta
	save $datain/appended_``i''_`weeks`k''.dta, replace
	}
}
********************************************
** Merge data sets with store demographic **
********************************************

/* Prep Store demographic files */
import excel $rawdata/store_demo.xls, sheet(LonLat_store_zip) firstrow clear
save $datain/store_demo.dta, replace

import excel $rawdata/RawRetail/store_demo2.xlsx, sheet(IRI_Key zip and county) firstrow clear
keep IRI_KEY Zip_name Zip_key Zip_lon Zip_lat County_Key County_Name State_Name
bysort IRI_KEY: gen id = _n
keep if id == 1
drop id 
save $temp/store_demo2.dta, replace

/* Depending on the year, need to use the right store demographic file */s
forvalues k=2(1)12{
	tokenize $filename 
	forvalues i=1/31{
		use $temp/appended_``i''_`weeks`k''.dta, clear
		merge m:1 IRI_KEY using $datain/store_demo2.dta
		keep if _m == 3
		drop _m
		save $temp/merged_demo_``i''_`weeks`k''.dta, replace
	}
}

************************************************
** Merge all data sets with week translation  **
************************************************

import excel "$rawdata/RawRetail/IRI week translation.xls", firstrow clear
keep IRIWeek Calendarweekstartingon Calendarweekendingon
rename IRIWeek WEEK
save $temp/week_trans2.dta, replace

**

import excel "$rawdata/RawRetail/IRI week translation1.xls", firstrow clear
keep IRIWeek Calendarweekstartingon Calendarweekendingon
rename IRIWeek WEEK
save $datain/week_trans1.dta, replace

/* Depending on the year, need to use the right week translation file */
forvalues k=2(1)12{
	tokenize $filename 
	forvalues i=1/31{
		use $temp/merged_demo_``i''_`weeks`k''.dta, clear
		merge m:1 WEEK using $datain/week_trans1.dta
		keep if _m == 3
		drop _m
		save $temp/merged_demo_``i''_`weeks`k''2.dta, replace
	}
}

*****************************************************
** Aggregate retail data to store-level (monthly)  **
*****************************************************

tokenize $filename 

forvalues i=1(1)31 {
	forvalues k=2/12{
	use $temp/merged_demo_``i''_`weeks`k''2.dta, clear
	
	gen year = year(Calendarweekendingon)
	gen month = month(Calendarweekendingon)

	bysort IRI_KEY year month: egen store_month_tot_pch = sum(DOLLARS)
	bysort IRI_KEY year month: gen id =_n
	keep if id ==1
	drop id 

	drop WEEK SY GE VEND ITEM UNITS DOLLARS F D1 PR
	save $temp/store_month_``i''_`weeks`k''.dta, replace
	}
}

******************************************************************************************
** Aggregate retail data to store-level (quarterly) and append years of the same product *
******************************************************************************************

tokenize $filename 

forvalues i=1(1)31 {
	forvalues k=2/12{
		append using $temp/store_month_``i''_`weeks`k''.dta
	}
	
	gen quarter = cond(month <= 3, 1, cond(month <= 6, 2, cond(month <= 9, 3, cond(month <= 12, 4,0))))
	bysort IRI_KEY year quarter: egen store_qtr_tot_pch = sum(store_month_tot_pch)
	bysort IRI_KEY year quarter: gen id = _n
	keep if id ==1
	drop id 
	drop month store_month_tot_pch
	save $temp/y`k'_store_qtr_``i''.dta, replace
}

*******************************************
** Merge aggregated retail data together **
*******************************************

tokenize $filename 

forvalues k=2/12{
forvalues i=1(1)31 {
	append using $dataout/y`k'_store_qtr_``i''.dta
}
save $temp/y`k'_store_qtr_all.dta, replace
}

*****************************************************************
** Aggregate category retail data to quarterly aggregate level **
*****************************************************************

forvalues k=2/12{
	append $temp/y`k'store_qtr_all, clear
}

bysort IRI_KEY year quarter: egen store_qtr_all_pch = sum(store_qtr_tot_pch)
bysort IRI_KEY year quarter: gen id = _n
keep if id ==1
drop id 

drop store_qtr_tot_pch

label var IRI_KEY "Store Code"
label var year "Year"
label var quarter "Quarter"
label var store_qtr_all_pch "Quarterly Store Sales"

save $temp/store_qtr_all_aggr, replace

*outsheet using $output/math118_store_sales.csv, delimiter(",") replace

*****************************************************
** Merge Retail Data with CSBA and MSA identifiers **
*****************************************************

use $temp/store_qtr_all_aggr, clear
keep if IRI_KEY != . 
		
*merge retail data with zip code - cbsa crosswalk **
		
merge m:1 County_Key using $datain/cbsa_counties.dta
drop if _m == 2
drop _m

*merge retail data with cbsa - msa crosswalk **

merge m:1 cbsa_code using $datain/cbsa_msa.dta
drop if _m == 2
drop _m
drop cbsa_name 
label var msanecma "MSA Code"
label var msa_name "MSA Name"

save$dataout/store_qtr_all_aggr2, replace

