#!/bin/bash
# run script for this project

while getopts "p" opt; do
	case ${opt} in
		"spacy")
			echo "spacy"
			;;
		"main")
			echo "main"
			;;
		*)
			echo "invalid arg"
			;;
	esac
done
