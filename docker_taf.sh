#!/bin/bash

# Initialize variables
branch_name=""
cherry_pick=""
ini_file=""
confFile=""
parameter=""
mode=""

# Process command-line arguments
while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        --branch_name)
        branch_name="$2"
        shift # past argument
        shift # past value
        ;;

        --cherry_pick)
        cherry_pick="$2"
        shift # past argument
        shift # past value
        ;;

        --ini_file)
        ini_file="$2"
        shift # past argument
        shift # past value
        ;;

        --confFile)
        confFile="$2"
        shift # past argument
        shift # past value
        ;;

        --parameter)
        parameter="$2"
        shift # past argument
        shift # past value
        ;;

        --mode)
        mode="$2"
        shift # past argument
        shift # past value
        ;;

        *)
        echo "Unknown option: $key"
        exit 1
        ;;
    esac
done

# Execute commands based on the presence of arguments
if [[ ! -z "$branch_name" ]]; then
    git checkout "$branch_name"
    git fetch
    git reset --hard origin/"$branch_name"
    git clean --force
fi

if [[ ! -z "$cherry_pick" ]]; then
    eval "$cherry_pick"
fi

python3 -m pip install --user -r requirements.txt
if [[ ! -z "$ini_file" && ! -z "$confFile" && ! -z "$parameter" && ! -z "$mode" ]]; then
    python3 testrunner.py -i "$ini_file" -c "$confFile" -p "$parameter" -m "$mode"
fi