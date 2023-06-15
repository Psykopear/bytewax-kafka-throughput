# Main python venv
for dir in src/*/
do
    # remove the trailing "/"
    dir=${dir%*/}
    # only take what's after the final "/"
    dir=${dir##*/}
    echo "Preparing src/$dir"
    cd src/$dir
    python -m venv .venv
    .venv/bin/pip install --upgrade pip
    .venv/bin/pip install -r requirements.txt
    cd ../../
done
