A simple, limited and user friendly way to run your program for a bunch of different combinations of parameters stored in various config files.

Supports both single node threading and MPI cluster parallelization by passing use_mpi=[False]/True to the controller constructor.

For MPI mode, run your script as

```
mpirun -n [nprocs] python my_script.py
```

A parameter is described as the config file in which it is stored, and the regex pattern from which it can be found.

A set of this parameter is then given as a list or a linspace-like initializer function.

A controller object is fed these set instances, and run all combinations of them on a specified number of threads for a given program execution rule, i.e. a function which tells the script what to do once a new set of parameters is initialized and ready to go.

The original config files will not be touched, copies will be used by each thread.

See the testbed() function and run the python script directly for an example and more information.





