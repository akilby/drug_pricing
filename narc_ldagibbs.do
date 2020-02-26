********************************************************************************
* Project: MAT
* Author: Jackie Reimer
* Date Created: October 5th, 2019
* Purpose: Try out ldagibbs package in this context

********************************************************************************

********************************************************************************
* 1. Setup
********************************************************************************

/*a. Limits ------------------------------------------------------------------*/
	cap log close
	clear all
	set more off
	


/*b. Directories -------------------------------------------------------------*/

if "`c(username)'" == "jackiereimer" {
	glob dir "/Users/jackiereimer/Dropbox"
}

else if "`c(username)'" == "akilby" {
	glob dir "/Users/akilby/Dropbox"
}

	glob data "$dir/drug_pricing_data/opiates/use_data"
	glob log  "$dir/Drug Pricing Project/drug_pricing/log"
	glob output "$dir/Drug Pricing Project/analysis_output"
	
/*b. Log ---------------------------------------------------------------------*/

	log using "$log/narc_ldagibbs_`c(current_date)'.log", replace
	
********************************************************************************
* 2. Prepare Data
********************************************************************************

/*a. Load data ---------------------------------------------------------------*/
	
	*i. Load all comments
	insheet using "$data/comments/all_comments.csv"
	
	*ii. Save as tempfile
	tempfile comments
	save `comments'
	
	*iii. Load all top level threads
	insheet using "$data/threads/all_dumps.csv", clear
	
	rename v1 post_id
	rename v2 url
	rename v3 no_comments
	rename v4 tiny_url
	rename v5 poster_id
	rename v6 post_title
	rename v7 post_body
	rename v8 time
	
	label post_id "Post ID"
	label url "Post URL"
	label no_comments "Number of Comments on Thread"
	label tiny_url "Abridged Post URL"
	
	
	
	*iv. Append comments
	append using `comments'
	
	*v. Keep relevant var
	
	
