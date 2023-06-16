# Main python venv
if [ -d .venv ]; then
  echo "Recreating venv (./.venv)"
  rm -rf .venv
else
  echo "Creating venv (./.venv)"
fi
python -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install confluent-kafka kafka-python matplotlib

main_pwd=`pwd`
for dir in benches/*/
do
  # remove the trailing "/"
  dir=${dir%*/}
  # only take what's after the final "/"
  dir=${dir##*/}

  if [ -d benches/$dir/.venv ]; then
    echo "Recreating venv (./benches/$dir/.venv)"
    rm -rf benches/$dir/.venv
  else
    echo "Creating venv (./benches/$dir)"
  fi

  python -m venv benches/$dir/.venv
  benches/$dir/.venv/bin/pip install --upgrade pip
  benches/$dir/.venv/bin/pip install -r benches/$dir/requirements.txt
done
