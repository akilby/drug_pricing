********************************************************************************
* Project: MAT
* Author: Jackie Reimer
* Date Created: October 5th, 2019
* Purpose: Clean /r/opiates data for ldagibbs

********************************************************************************

********************************************************************************
* 1. Setup
********************************************************************************

/*a. Limits ------------------------------------------------------------------*/
	cap log close
	clear all
	set more off
	
	ssc install txttool

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
	glob keyterm "$dir/Drug Pricing Project/keyterm_lists/"
	
/*b. Log ---------------------------------------------------------------------*/

	log using "$log/narc_clean_for_stata_`c(current_date)'.log", replace
	
********************************************************************************
* 2. Prepare Data
********************************************************************************

/*a. Clean comments data -----------------------------------------------------*/
	
	*i. Load all comments
	insheet using "$data/comments/all_comments.csv", clear
	
	*ii. Name variables
	rename v1 post_id
	rename v2 url
	rename v3 ref_post_id
	rename v4 post_text
	rename v5 poster_id
	rename v6 time
		
	*iii. label var
	label var post_id "Post ID"
	label var url "Post URL"
	label var ref_post_id "Thread Being Referenced"
	label var post_text "Content of Post"
	label var poster_id "Post Author ID"
	label var time "Time"

		*v. save
	save "$data/comments/all_comments.dta", replace
	
	
/*b. Clean threads data ------------------------------------------------------*/
	
	*i. Load all top level threads
	insheet using "$data/threads/all_dumps.csv", clear
	
	
	*ii. Name variables
	rename v1 post_id
	rename v2 url
	rename v3 no_comments
	rename v4 tiny_url
	rename v5 poster_id
	rename v6 post_title
	rename v7 post_body
	rename v8 time
	
	*iii. Concatenate Post title and Body
	egen post_text = concat(post_title post_body), punct(" ")
	
	*iv. label var
	label var post_id "Post ID"
	label var url "Post URL"
	label var no_comments "Number of Comments on Thread"
	label var tiny_url "Abridged Post URL"
	label var poster_id "Post Author ID"
	label var post_title "Title of Thread"
	label var post_body "Body of Thread"
	label var time "Time"
	label var post_text "Content of Post"
	
	*v. save
	save "$data/threads/all_dumps.dta", replace
	

********************************************************************************
* 2. Append Data
********************************************************************************

append using "$data/comments/all_comments.dta"

********************************************************************************
* 3. Clean Data
********************************************************************************


txttool post_text, gen(clean_post_text) stem sub("$keyterm/mat/naloxone_words.txt")
txttool clean_post_text, replace(clean_p1ost_text) stem sub("$keyterm/mat/naloxone_words.txt")					


* Save

save "$data/all_posts.dta", replace

cap log close
