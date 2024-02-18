import ctypes
from scipy import *
from scipy.linalg import *
import numpy as np
import sys
import csv
# Initialize CULA library 
libculaC=ctypes.CDLL('libcula_lapack.so',mode=ctypes.RTLD_GLOBAL)
libculaC.culaGetStatusString.restype=ctypes.c_char_p
info=libculaC.culaInitialize()
# import data
def fileCov(na):
	rmean = zeros(na)
	y = zeros((na, na))
	with open('covMatrix.csv', 'r') as f1:
		reader = csv.reader(f1)
		i = 0
		for row in reader:
			y[i] = np.array(row, float64)
			i = i +1		
	with open('rMean.csv', 'r') as f2:
		reader = csv.reader(f2)
		for row in reader:
			rmean = np.array(row, float64)
	return rmean, y
def generateCov(na, ns):
	rmean = zeros(na)
	rvolat = zeros(na)
	for i in range(na):
		t1 = np.random.uniform(-7.0, 7.0)
		rmean[i] = t1
		rvolat[i] = math.fabs(t1)/7.0
	print(rmean)
	print(rvolat)
	z = zeros((na,ns))
	for j in range(na):
		r = np.random.normal(rmean[j], rvolat[j], ns)
		z[j] = r
	return rmean, np.cov(z)
na = 256
ns = 1000
na = 8
rmean, y = fileCov(na)
print("Y:")
print(y)
print("rmean:")
print(rmean)
#sys.exit()
np.random.seed(0)
#rmean, y = generateCov(na, ns)
A = zeros((na+2,na+2))
for i in range(na):
	for j in range(na):
		A[i][j] = y[i][j]
	A[i][na] = -rmean[i]
	A[i][na+1] = -1.0
for j in range(na):
	A[na][j] = rmean[j]
	A[na+1][j] = 1.0
x = zeros(na+2)
b = zeros(na+2)
b[na] = 57.64
b[na+1] = 1.0
print("This is A: ")
print(A)
print("--- End A ---")
# declare a matrix to hold transposed A
A_t = zeros((na+2,na+2))
A_t= np.array(A.T,order='C')
print("A transposed as A_t: ")
print(A_t)
#The following three lines converts the numpy pointers to  ctypes for CULA use.
A_t = A_t.astype(np.float32)
c_float_p = ctypes.POINTER(ctypes.c_float)
A1_p = A_t.ctypes.data_as(c_float_p)
#
x = zeros(na)
b = zeros(10)
b[na] = 57.64
b[na+1] = 1.0
print("THIS IS b:  ")
print(b)
print("---- END ----")
print("THIS IS x before solver:  ")
print(x)
print("---- END ----")
xpy = zeros(na)
x = x.astype(np.float32)
x_p = x.ctypes.data_as(c_float_p) # to receive answer
b = b.astype(np.float32)
b_p = b.ctypes.data_as(c_float_p) # calculated off of
xpy = solve(A,b)
#Sgesv(n,nrhs,a,lda,ipiv,b,ldb)
# solving A * X = B where A = a, X = b, and B = b in above  notation
#1 n = number of linear equations
#2 nrhs = number of columns of the matrix B
#3 a = pointer to matrix A1_p
#4 lda = leading dimension of the array A.
#5 ipiv = an int array, output, with dimension N, defines permutation matrix(used for working)
#6 b = pointer input/output, dimension nrhs, matrix b
#7 lbd = int as input is leading dimension of the array b
libculaC.culaSgesv(10,1,A1_p,10,x_p,b_p,10)
print("LibCULA SGESV: ")
# converting cula pointer to numpy so as to extract the data
a = np.fromiter(b_p, dtype=np.float32, count=10)
b = np.fromiter(x_p, dtype=np.float32, count=10)
print(a)
print("----end o/p---")
print("This is python solver: ")
print(xpy)
print("---end o/p from python--")
print("Data in working matrix: ")
print(b)
print("--end of working matrix")
libculaC.culaShutdown()
