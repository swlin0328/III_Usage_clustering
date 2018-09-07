import subprocess

def run():
	process = subprocess.Popen(['Rscript', './clustering_num_evaluation.R'])
	process.wait()

if __name__ == '__main__':
	run()
