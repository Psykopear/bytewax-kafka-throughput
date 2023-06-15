pythons=""

# Main python venv
python -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install confluent-kafka kafka-python bytewax

# A python venv for each bytewax version/commit/ref
for i do
  python -m venv .bytewax-"$i"
  .bytewax-"$i"/bin/pip install --upgrade pip
  .bytewax-"$i"/bin/pip install confluent-kafka kafka-python
  .bytewax-"$i"/bin/pip install git+https://github.com/bytewax/bytewax.git@"$i"
done
