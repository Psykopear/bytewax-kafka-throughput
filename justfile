redpanda:
  docker compose down -v
  docker compose up -d

prepare_envs:
  sh prepare_envs.sh

produce:
  .venv/bin/python produce.py

sample:
  .venv/bin/python sample.py
