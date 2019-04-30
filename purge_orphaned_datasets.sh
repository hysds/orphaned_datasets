# !/bin/bash

source $HOME/verdi/bin/activate

BASE_PATH=$(dirname "${BASH_SOURCE}")

# check args
if [ "$#" -eq 2 ]; then
  grq_es_url=$1
  s3_url=$2

  echo s3_url
  echo dataset

else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# start s3 crawler job
echo "##########################################" 1>&2
echo -n "starting job"
# date 1>&2
python $BASE_PATH/purge_orphaned_datasets.py "$grq_es_url" "s3_url"     ### > notify_by_email.log 2>&1s

deactivate
exit 0


# send email
# echo "##########################################" 1>&2
# echo -n "Sending email: " 1>&2
# date 1>&2
# python $BASE_PATH/notify_by_email.py "$id" "$url" "$emails" "$rule_name" "$component" > notify_by_email.log 2>&1
# STATUS=$?

# echo -n "Finished sending email: " 1>&2
# date 1>&2
# if [ $STATUS -ne 0 ]; then
#   echo "Failed to send email." 1>&2
#   cat notify_by_email.log 1>&2
#   echo "{}"
#   exit $STATUS
# fi
