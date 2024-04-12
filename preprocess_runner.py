from preprocess import PreprocessJob

preprocess_job = PreprocessJob()

# running job and printing output into file
with preprocess_job.make_runner() as runner:
    runner.run()
