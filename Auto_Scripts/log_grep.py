import subprocess 
import ast
import csv
class Main:

#sample format : grep', '-r', '172.31.12.242', '/data/public/OPS/Alerts/Alert_Temp_Driver.py

	def func(self):
	
		try:
			
			f = open('touchpoint_raj.csv')
			csv_f = csv.reader(f)

			for row in csv_f:
          			row= ' '.join([str(elem) for elem in row])
				row="'"+row+"'"
				Out=[]
		#		pat= row+ ":"+"No such file or directory\n"
#				print(pat,"printers")
				row = ast.literal_eval(row)
				out=subprocess.Popen(row,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)

				stdout,stderr = out.communicate()
				if stdout != '' and not stdout.endswith("directory\n") and not stdout.endswith("Permission denied\n"):
					print(row,stdout)
					print("**************************************************************")
					print(' ')

		except Exception as e:

			print(e)


run=Main()
run.func()


