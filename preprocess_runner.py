from preprocess import PreprocessJob

preprocess_job = PreprocessJob()

# running job and printing output into file
with preprocess_job.make_runner() as runner:
    runner.run()
    total = runner.counters()[0]["counters"]["total"]
    category = runner.counters()[0]["categories"]

    with open("counters.txt", "w") as writer:
        writer.write(str(total) + " " + str(category))

    for key, value in preprocess_job.parse_output(runner.cat_output()):
        print(key, value, "\n", end="")
