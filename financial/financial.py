import pycuda.gpuarray as gpuarray
import pycuda.autoinit  # this is necessary to create the proper device context even though it isn't explicitly called
import numpy as np
import ctypes
from scipy.linalg import solve
import csv

# cuSOLVER
_libcusolver = ctypes.cdll.LoadLibrary('libcusolver.so')

# Initialize wrappers
cusolverDnCreate = _libcusolver.cusolverDnCreate
cusolverDnCreate.restype = ctypes.c_int
cusolverDnCreate.argtypes = [ctypes.POINTER(ctypes.c_void_p)]

cusolverDnDestroy = _libcusolver.cusolverDnDestroy
cusolverDnDestroy.restype = ctypes.c_int
cusolverDnDestroy.argtypes = [ctypes.c_void_p]

cusolverDnDgetrf = _libcusolver.cusolverDnDgetrf
cusolverDnDgetrf.restype = ctypes.c_int
cusolverDnDgetrf.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]

cusolverDnDgetrs = _libcusolver.cusolverDnDgetrs
cusolverDnDgetrs.restype = ctypes.c_int
cusolverDnDgetrs.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p]

# create handle
handle = ctypes.c_void_p()
cusolverDnCreate(ctypes.byref(handle))


def solve_with_cusolver(handle, A, b):
    """
    Solves the linear system A * x = B using cuSolver.
    """
    A_gpu = gpuarray.to_gpu(A)
    b_gpu = gpuarray.to_gpu(b)
    d_A = A_gpu.gpudata
    d_b = b_gpu.gpudata

    # Workspace and pivot array for LU decomposition
    m, n = A.shape
    workspace = gpuarray.empty(m, dtype=np.float64)
    pivot_gpu = gpuarray.empty(min(m, n), dtype=np.int32)
    info_gpu = gpuarray.empty(1, dtype=np.int32)

    # Perform LU decomposition
    cusolverDnDgetrf(handle, int(m), int(n), ctypes.cast(int(d_A), ctypes.POINTER(ctypes.c_double)), int(m),
                     ctypes.cast(int(workspace.gpudata), ctypes.POINTER(ctypes.c_double)),
                     ctypes.cast(int(pivot_gpu.gpudata), ctypes.POINTER(ctypes.c_int)),
                     ctypes.cast(int(info_gpu.gpudata), ctypes.POINTER(ctypes.c_int)))
    # Solve Ax = B using the LU factorization
    cusolverDnDgetrs(handle, 0, int(n), int(1), ctypes.cast(int(d_A), ctypes.POINTER(ctypes.c_double)), int(m),
                     ctypes.cast(int(pivot_gpu.gpudata), ctypes.POINTER(ctypes.c_int)),
                     ctypes.cast(int(d_b), ctypes.POINTER(ctypes.c_double)), int(n),
                     ctypes.cast(int(info_gpu.gpudata), ctypes.POINTER(ctypes.c_int)))

    # Copy the result back to host
    x = b_gpu.get()
    return x

# import data
def fileCov(na):
    rmean = np.zeros(na)
    y = np.zeros((na, na))
    with open('covMatrix.csv', 'r') as f1:
        reader = csv.reader(f1)
        i = 0
        for row in reader:
            y[i] = np.array(row, dtype=np.float64)
            i += 1
    with open('rMean.csv', 'r') as f2:
        reader = csv.reader(f2)
        for row in reader:
            rmean = np.array(row, dtype=np.float64)
    return rmean, y

na = 8
ns = 1000
rmean, y = fileCov(na)
print("Y:")
print(y)
print("rmean:")
print(rmean)

A = np.zeros((na + 2, na + 2))
for i in range(na):
    for j in range(na):
        A[i][j] = y[i][j]
    A[i][na] = -rmean[i]
    A[i][na + 1] = -1.0

for j in range(na):
    A[na][j] = rmean[j]
    A[na + 1][j] = 1.0

x = np.zeros(na + 2)
B = np.zeros(na + 2)
B[na] = 57.64
B[na + 1] = 1.0

A = A.astype(np.float64)
B = B.astype(np.float64)

print("This is A: ")
print(A)
print("--- End A ---")

print("Data in working matrix: ")
print(B)
print("--end of working matrix")
print("THIS IS x before solver:  ")
print(x)
print("---- END ----")

# Solve the system using cuSolver
solution_cusolver = solve_with_cusolver(handle, A, B)

print("cuSolver solution:")
print(solution_cusolver)

xpy = solve(A.T, B)
print("This is python solver: ")
print(xpy)

# Destroy cuSolver handle
cusolverDnDestroy(handle)
