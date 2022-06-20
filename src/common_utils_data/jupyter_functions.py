import re 
import nbformat
from nbconvert import PythonExporter
import warnings 

warnings.filterwarnings('ignore')

def nbconverter(notebookPath,directionPath=None):
	"""method name is the same as jupyter default converter name :
	   Nbconvert(notebookPath,directionPath) 
	:param notebookPath: source path of the ipynb file you want to convert
	:param direction Path : direction path of the formatted .py file 
	"""
	try:
		with open(notebookPath,'r',encoding='utf-8') as fh:
			nb = nbformat.reads(fh.read(), nbformat.NO_CONVERT)
	except FileNotFoundError:
		if '.ipynb' not in notebookPath:
			notebookPath +=  '.ipynb'
		with open(notebookPath,'r',encoding='utf-8') as fh:
			nb = nbformat.reads(fh.read(), nbformat.NO_CONVERT)

	if directionPath == None:
		directionPath = notebookPath.replace('.ipynb','.py')

	pattern_input = r'# In\[[\d\s]*\]:'
	pattern_comment = '^#.+'
	pattern_variables = r'^[a-zA-Z0-9_]+\[?[ |0-9]*\]? *$'
	pattern_square_bracket = r'^\[.*\]$'
	pattern_string = r"^[\'|\"].*[\'|\"] *$"
	pattern_number = r'^\d+ *[\+|\-|\*|\/]? *\d* *$'

	def print_match(matched):
		return 'print({})'.format(matched.group().strip())

	exporter =     PythonExporter()
	source, meta = exporter.from_notebook_node(nb)

	source = source.split('\n')[2:]
	source = [ t for t in source if t and not re.match(pattern_input,t)]
	source = [ '\n' + t if re.match(pattern_comment,t) else t for t in source ]
	source = [re.sub(pattern=pattern_variables,repl=print_match,string=t) for t in source]
	source = [re.sub(pattern=pattern_square_bracket,repl=print_match,string=t) for t in source]
	source = [re.sub(pattern=pattern_string,repl=print_match,string=t) for t in source]

	source = '\n'.join(source) + '\n\n'
	with open(directionPath, 'w+',encoding='utf-8') as fh:
		fh.write(source)
		print('{} has been saved'.format(directionPath))



nbconverter(r"C:\Users\Administrator.DG-11030335\Scripts\voc_alarm\外销意见反馈预警监控\convert2alarm.ipynb")