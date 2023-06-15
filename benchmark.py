def run_producer(limit, topic):
    process = subprocess.Popen(["python", "produce.py", limit, topic])
    return process
