pythons=""

for i do
  # Create virtualenv, upgrade pip, install the proper version of bytewax
  python -m venv .bytewax-"$i"
  .bytewax-"$i"/bin/pip install --upgrade pip
  .bytewax-"$i"/bin/pip install confluent-kafka kafka-python
  .bytewax-"$i"/bin/pip install git+https://github.com/bytewax/bytewax.git@"$i"

  if [[ -z "$pythons" ]]; then
    pythons=.bytewax-"$i"/bin/python
  else
    pythons=$pythons,.bytewax-"$i"/bin/python
  fi
done

# hyperfine -L python $pythons "{python} dataflows/0_10_erlend.py"
# hyperfine -L python $pythons "{python} dataflows/main_erlend.py"
