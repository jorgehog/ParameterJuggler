from itertools import product, combinations
from re import sub, findall
import threading
import time
import shutil
import os
import sys
import random

try:
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    mpi_success = True
except ImportError:
    mpi_success = False
    pass

class Worker(threading.Thread):

    def __init__(self, controller, exec_program_rule, proc, *args, **kwargs):

        self.controller = controller
        self.exec_program_rule = exec_program_rule
        self.proc = proc

        self.args = args
        self.kwargs = kwargs

        super(Worker, self).__init__()

    def stop(self):
        self.controller.stopped = True

    def run(self):
        while not len(self.controller.combinations) == 0:
            parameters = self.controller.get_parameters_thread()
            success = self.controller.run_parameters(self.exec_program_rule,
                                                     parameters,
                                                     self.proc,
                                                     *self.args,
                                                     **self.kwargs)

            if success != 0 and not self.stopped:
                self.controller.stop(self.proc, success)


class ParameterSet:

    def __init__(self,
                 config_filename,
                 variable_pattern_in_config,
                 regex_flags=[]):

        if type(variable_pattern_in_config) is not list:
            variable_pattern_in_config = [variable_pattern_in_config]

        with open(config_filename, "r") as f:

            txt = f.read()
            for pattern in variable_pattern_in_config:
                match = findall(pattern, txt, *regex_flags)

            if not match:
                raise RuntimeError("Pattern '%s' does not match anything in config file '%s'" % (pattern,
                                                                                                 config_filename))

        self.config_filename = config_filename
        self.variable_pattern_in_config = variable_pattern_in_config
        self.set = None

        self.regex_flags = regex_flags

        self.current_index = 0

        self.child = None
        self.parent = None

    def initialize_set(self, set):

        if len(self.variable_pattern_in_config) != 1:
            if len(set) != len(self.variable_pattern_in_config):
                print len(set), len(self.variable_pattern_in_config)
                raise RuntimeError("Set length must match the number of patterns.")

            self.set = zip(*set)

        else:
            self.set = [tuple([x]) for x in set]

    @staticmethod
    def invert_regex(pattern):

        inverted_pattern = ""

        if pattern[0] != "(":
            inverted_pattern += "("

        for i in range(0, len(pattern)):

            if pattern[i] == "(":
                inverted_pattern += ")"

            elif pattern[i] == ")":
                inverted_pattern += "("

            else:
                inverted_pattern += pattern[i]

        if pattern[-1] != ")":
            inverted_pattern += ")"

        return inverted_pattern.replace("()", "")

    def get_config_name(self, proc):

        ending = "_%d" % proc

        if "." in self.config_filename:

            split = self.config_filename.split(".")
            pre = split[:-1]
            suf = split[-1]

            return ".".join(pre) + ending + "." + suf

        else:

            return self.config_filename + ending

    def copy_config(self, controller, proc):
        config_out = self.get_config_name(proc)

        if config_out in controller.copied_config_files:
            return

        shutil.copy(self.config_filename, config_out)

        controller.register_config_file(config_out)

    def write_value(self, values, proc):

        config_out = self.get_config_name(proc)

        with open(config_out, 'r') as config_file:

            config_raw_text = config_file.read()


        for i, value in enumerate(values):
            pattern = self.invert_regex(self.variable_pattern_in_config[i])
            config_raw_text = sub(pattern,
                                  r"%s%s%s" % (r"\g<1>", value, r"\g<2>"),
                                  config_raw_text,
                                  *self.regex_flags)

        with open(config_out, 'w') as config_file:
            config_file.write(config_raw_text)


class ParameterSetController:

    def __init__(self, use_mpi=False):
        self.parameter_sets = []
        self.copied_config_files = []

        self.all_threads = []
        self.parameter_lock = threading.Lock()

        self.shuffle = False

        self.stopped = False

        self.use_mpi = use_mpi

        self.repeats = 1

        if use_mpi and not mpi_success:
            raise ImportError("Missing dependecy for mpi: mpi4py")

    def set_repeats(self, N):
        self.repeats = N

    def register_parameter_set(self, parameter_set):

        if len(self.parameter_sets) != 0:
            parameter_set.parent = self.parameter_sets[-1]
            self.parameter_sets[-1].child = parameter_set

        self.parameter_sets.append(parameter_set)

    def prepare(self):

        for parameter_set in self.parameter_sets:
            parameter_set.prepare()

    def register_config_file(self, config_name):

        if not config_name in self.copied_config_files:
            self.copied_config_files.append(config_name)


    def clean(self):
        for config_name in self.copied_config_files:
            os.remove(config_name)
        self.copied_config_files = []

    @staticmethod
    def get_rank(use_mpi):
        if use_mpi:
            return comm.rank
        else:
            return 0

    def n_nodes(self):
        if self.use_mpi:
            return comm.size-1
        else:
            return 1

    def run(self, execute_program_rule, *args, **kwargs):

        n = 1
        for parameter_set in self.parameter_sets:
            n *= len(parameter_set.set)

        n *= self.repeats

        skip = self.use_mpi
        if "ask" in kwargs:

            if not self.use_mpi:
                if kwargs["ask"] is False:
                    skip = True
            kwargs.pop("ask")

        if "shuffle" in kwargs:
            if kwargs.pop("shuffle") is True:
                self.shuffle = True

        n_procs = self.n_nodes()

        if "n_procs" in kwargs:

            if not self.use_mpi:
                n_procs = kwargs["n_procs"]

                if n_procs <= 1:
                    n_procs = 1

            kwargs.pop("n_procs")

        if n_procs > n:
            if self.get_rank(self.use_mpi) == 0:
                print "Number of processes %d is too high for %d processes. Truncating..." % (n_procs, n)
                n_procs = n

        if skip:
            ans = ""
        else:
            ans = raw_input("Press enter to run %d simulations." % n)

        if ans != "":
            print "exiting.."
            return 1

        self.combinations = list(product(*[pset.set for pset in self.parameter_sets]))*self.repeats

        n_per_proc = len(self.combinations)/n_procs
        remainder = len(self.combinations) - n_per_proc*n_procs

        if self.get_rank(self.use_mpi) == 0:
            print "%d*%d + %d processes" % (n_per_proc,
                                            n_procs,
                                            remainder)

        self.start_runs(execute_program_rule, n_procs, *args, **kwargs)

        self.clean()

        if self.stopped:
            return 1
        else:
            return 0

    def clear(self):
        self.parameter_sets = []
        self.repeats = 1

    def dump_config_files(self, proc):
        for parameter_set in self.parameter_sets:
            parameter_set.copy_config(self, proc)

    def start_runs(self, execute_program_rule, n_procs, *args, **kwargs):

        if self.use_mpi:

            #master
            if self.get_rank(self.use_mpi) == 0:
                n_jobs = len(self.combinations)

                #seed slaves
                for proc in range(1, comm.size):
                    comm.send(self.get_parameters(), dest=proc, tag=proc)

                status = MPI.Status()
                n = 0
                while n != n_jobs:
                    success = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                    n+=1

                    if success != 0:
                        self.stop(status.Get_source(), success)

                    comm.send(self.get_parameters(), dest=status.Get_source(), tag=status.Get_tag())

            #slave
            else:
                self.dump_config_files(comm.rank)

                parameters = comm.recv(source=0, tag=comm.rank)

                while parameters is not None:
                    success = self.run_parameters(execute_program_rule, parameters, comm.rank, *args, **kwargs)

                    comm.send(success, dest=0, tag=comm.rank)
                    parameters = comm.recv(source=0, tag=comm.rank)

        else:

            self.all_threads = []
            self.stopped = False

            for proc in range(n_procs):

                self.dump_config_files(proc)

                thread = Worker(self, execute_program_rule, proc, *args, **kwargs)

                self.all_threads.append(thread)

                thread.start()

            for thread in self.all_threads:
                thread.join()

    def get_parameters(self):

        if not self.combinations:
            return None

        if self.shuffle:
            i = random.randint(0, len(self.combinations) - 1)
            parameters = self.combinations.pop(i)
        else:
            parameters = self.combinations.pop()

        return parameters

    def get_parameters_thread(self):

        self.parameter_lock.acquire()
        parameters = self.get_parameters()
        self.parameter_lock.release()

        return parameters

    def run_parameters(self, execute_program_rule, parameters, proc, *args, **kwargs):
            self.write_combination(proc, parameters)
            return execute_program_rule(proc, parameters, *args, **kwargs)

    def write_combination(self, proc, combination):

        for parameter_set, value in zip(self.parameter_sets, combination):
            parameter_set.write_value(value, proc)

    def stop(self, which, status):
        print "Process %d ended with exit status %d: Stopping..." % (which, status)

        if not self.use_mpi:
            for thread in self.all_threads:
                thread.stop()

def quick_replace(cfg, name, value, all_ranks=False):

    skip = False
    if not all_ranks and mpi_success:
        if MPI.COMM_WORLD.rank != 0:
            skip = True

    if not skip:
        cfg_str = ""
        with open(cfg, 'r') as f_read:
            cfg_str = f_read.read()

        repl = sub(r"(%s\s*=\s*[\"\']?).*?([\"\']?;)" % name, "\g<1>%s\g<2>" % str(value), cfg_str)

        with open(cfg, 'w') as f_write:
            f_write.write(repl)

    if mpi_success:
        MPI.COMM_WORLD.Barrier()


def exec_test_function(proc, combination):

    combination = tuple([x[0] for x in combination])

    time.sleep(proc/5.*random.random())

    testfile_name = "/tmp/test_paramloop_%d.cfg" % proc

    with open(testfile_name, 'r') as testfile:
        testfile_raw_text = testfile.read()

    results = findall(r"a\s*\=\s*\[\s*(\d+)\s*\,\s*(.*)\s*\,\s*(\d+)\s*\]", testfile_raw_text)[0]


    for i, result in enumerate(results):
        if float(result) == combination[i]:
            print "combination", combination, "success."
            return 0
        else:
            print "combination", combination, "failed on proc %d: %s != %s" % (proc, str(results), str(combination))
            return 1

n_per_proc = []

def exec_test_function2(proc, combination):

    time.sleep(proc/5.*random.random())

    testfile_name = "/tmp/test_paramloop_%d.cfg" % proc
    testfile2_name = "/tmp/test_paramloop2_%d.cfg" % proc

    with open(testfile_name, 'r') as testfile:
        testfile_raw_text = testfile.read()
    with open(testfile2_name, 'r') as testfile:
        testfile2_raw_text = testfile.read()

    a1 = findall(r"a\s*\=\s*\[\s*(\d+)\s*\,\s*.*\s*\,\s*\d+\s*\]", testfile_raw_text)[0]
    a2 = findall(r"a\s*\=\s*(\d+)\;", testfile2_raw_text)[0]
    b2 = findall(r"b\s*\=\s*(\d+)\;", testfile2_raw_text)[0]

    success = (int(a1) == combination[0][0]) and (int(a2) == combination[1][0]) and (int(b2) == combination[1][1])

    n_per_proc[proc] += 1

    if not success:
        print "combination", combination, "failed on proc %d:" % proc, a1, a2, b2, "!=", combination
        return 1
    else:
        print "combination", combination, "success"
        return 0



def testbed():

    global n_per_proc

    testfile_name = "/tmp/test_paramloop.cfg"

    use_mpi = mpi_success and "mpi" in sys.argv[1:]
    rank = ParameterSetController.get_rank(use_mpi)

    if rank == 0:
        with open(testfile_name, 'w') as testfile:
            testfile.write("a=[0,1,2]\n")
    if use_mpi:
        comm.Barrier()

    any_number = r".*?"

    set1 = ParameterSet(testfile_name, r"a\=\[(%s)\,%s\,%s\]" % (any_number, any_number, any_number))
    set2 = ParameterSet(testfile_name, r"a\=\[%s\,(%s)\,%s\]" % (any_number, any_number, any_number))
    set3 = ParameterSet(testfile_name, r"a\=\[%s\,%s\,(%s)\]" % (any_number, any_number, any_number))

    set1.initialize_set([0, 1, 2, 3])
    set2.initialize_set([-2, -0.5, 1])
    set3.initialize_set([0, 30, 60, 90])

    controller = ParameterSetController(use_mpi=use_mpi)

    controller.register_parameter_set(set1)
    controller.register_parameter_set(set2)
    controller.register_parameter_set(set3)

    controller.set_repeats(2)

    controller.run(exec_test_function, ask=False, n_procs=20, shuffle=True)

    #Double file case
    #and double params

    testfile2_name = "/tmp/test_paramloop2.cfg"

    if rank == 0:
        with open(testfile2_name, 'w') as testfile:
            testfile.write("a=3;\nb=5;")
    if use_mpi:
        comm.Barrier()

    set4 = ParameterSet(testfile2_name, [r"a\=(%s)\;" % any_number,
                                         r"b\=(%s)\;" % any_number])
    set4.initialize_set([[ 1,  2,  3,  4],
                         [11, 12, 13, 14]])

    controller2 = ParameterSetController(use_mpi=use_mpi)

    controller2.register_parameter_set(set1)
    controller2.register_parameter_set(set4)

    controller2.set_repeats(10)

    n_procs = 20

    if use_mpi:
        n_per_proc = [0 for i in range(comm.size)]
    else:
        n_per_proc = [0 for i in range(n_procs)]

    controller2.run(exec_test_function2, ask=False, n_procs=n_procs, shuffle=False)

    if use_mpi:
        comm.Barrier()
        time.sleep(comm.rank/100.)
        print comm.rank, n_per_proc[comm.rank]
    else:
        for proc, n in enumerate(n_per_proc):
            print proc, n


def testbed_MPI():

    if comm.rank == 0:
        my_list = [0,1,2,3,4,5,6,7,8,9,10]
        n_jobs = len(my_list)

        print "Master start:", sum(my_list)

        #Seeding slaves
        for i in range(1, comm.size):

            if not my_list:
                item = None
            else:
                item = my_list.pop()

            comm.send(item, dest=i, tag=0)

        #recieving slave result and distributing new jobs
        s = 0
        n = 0
        status = MPI.Status()
        while n != n_jobs:
            s += comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            n += 1

            if my_list:
                obj = my_list.pop()
            else:
                obj = None

            comm.send(obj, dest=status.Get_source(), tag=status.Get_tag())

        print "Master finished:", s

    else:
        item = comm.recv(source=0, tag=0)

        while item is not None:
            print item, comm.rank

            comm.send(item, dest=0, tag=comm.rank)
            item = comm.recv(source=0, tag=comm.rank)

        print "Rank %d finished." % comm.rank


if __name__ == "__main__":
    if mpi_success and "mpibed" in sys.argv[1:]:
       testbed_MPI()
    else:
        testbed()


