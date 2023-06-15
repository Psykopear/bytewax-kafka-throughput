# Main python venv
python -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install confluent-kafka kafka-python bytewax

for dir in src/*/
do
    # remove the trailing "/"
    dir=${dir%*/}
    # only take what's after the final "/"
    dir=${dir##*/}
    echo src/$dir
    cd src/$dir
    python -m venv .venv
    .venv/bin/pip install --upgrade pip
    .venv/bin/pip install -r requirements.txt
    cd ../../
done
