from itertools import product, combinations
from re import sub, findall
import threading
import time
import shutil
import os
import sys

try:
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    mpi_success = True
except ImportError:
    mpi_success = False
    pass

class Worker(threading.Thread):

    def __init__(self, controller, exec_program_rule, chunk, proc, *args, **kwargs):

        self.controller = controller
        self.exec_program_rule = exec_program_rule
        self.chunk = chunk
        self.proc = proc

        self.args = args
        self.kwargs = kwargs

        super(Worker, self).__init__()

    def stop(self):
        self.controller.stopped = True

    def run(self):
        self.controller.run_chunk(self.exec_program_rule, self.chunk, self.proc, *self.args, **self.kwargs)


class ParameterSet:

    def __init__(self,
                 config_filename,
                 variable_pattern_in_config,
                 regex_flags=[]):

        with open(config_filename, "r") as f:

            match = findall(variable_pattern_in_config, f.read(), *regex_flags)

        if not match:
            raise RuntimeError("Pattern '%s' does not match anything in config file '%s'" % (variable_pattern_in_config, config_filename))

        self.config_filename = config_filename
        self.variable_pattern_in_config = variable_pattern_in_config

        self.regex_flags = regex_flags

        self.current_index = 0

        self.child = None
        self.parent = None

    def initialize_set(self, set):

        self.set = set

    def initialize_set_incr(self,
                            start,
                            stop,
                            increment,
                            increment_rule=lambda previous, increment: previous + increment):

        set = []

        value = start

        while value <= stop:

            set.append(value)

            value = increment_rule(value, increment)

        self.initialize_set(set)

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

    def write_value(self, value, proc):

        config_out = self.get_config_name(proc)

        with open(config_out, 'r') as config_file:

            prev_config_raw_text = config_file.read()

        new_config_raw_text = sub(self.invert_regex(self.variable_pattern_in_config),
                                  r"%s%s%s" % (r"\g<1>", value, r"\g<2>"),
                                  prev_config_raw_text,
                                  *self.regex_flags)

        with open(config_out, 'w') as config_file:
            config_file.write(new_config_raw_text)





class ParameterSetController:

    def __init__(self, use_mpi=False):
        self.parameter_sets = []
        self.copied_config_files = []

        self.all_threads = []
        self.stopped = False

        self.use_mpi = use_mpi

        if use_mpi and not mpi_success:
            raise ImportError("Missing dependecy for mpi: mpi4py")

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

    def get_rank(self):
        if self.use_mpi:
            return comm.rank
        else:
            return 0

    def n_nodes(self):
        if self.use_mpi:
            return comm.size
        else:
            return 1

    def run(self, execute_program_rule, *args, **kwargs):

        n = 1
        for parameter_set in self.parameter_sets:
            n *= len(parameter_set.set)

        skip = self.use_mpi
        if "ask" in kwargs:

            if not self.use_mpi:
                if kwargs["ask"] is False:
                    skip = True
            kwargs.pop("ask")

        n_procs = self.n_nodes()

        if "n_procs" in kwargs:

            if not self.use_mpi:
                n_procs = kwargs["n_procs"]

                if n_procs <= 1:
                    n_procs = 1

            kwargs.pop("n_procs")

        if n_procs > n:
            if self.get_rank() == 0:
                raise ValueError("Number of processes %d is too high for %d processes." % (n_procs, n))
            else:
                return 1

        if skip:
            ans = ""
        else:
            ans = raw_input("Press either: \n\tenter to run %d simulations."
                            "\n\tt [time per sim in seconds] for a time estimate. "
                            "\n\tq to end.\n" % n)

        if ans.startswith("t "):
            time = float(ans.lstrip("t "))
            t = n*time/n_procs

            unit = ""
            if t > 3600:
                t /= 3600.
                unit = "hours"

            elif t > 60:
                t /= 60.
                unit = "minutes"

            else:
                unit = "seconds"

            print "Expected time: %g %s" % (t, unit)

            return self.run(execute_program_rule, *args, **kwargs)

        elif ans == "q":
            print "exiting.."
            return 1

        combinations = list(product(*[pset.set for pset in self.parameter_sets]))

        n_per_proc = len(combinations)/n_procs

        if self.get_rank() == 0:
            print n_per_proc, "processes"


        remainder = len(combinations) - n_per_proc*n_procs

        self.all_threads = []
        self.stopped = False

        chunks = [None for n in range(n_procs)]

        prev = 0
        for proc in range(n_procs):

            N = n_per_proc

            if proc < remainder:
                N += 1

            chunks[proc] = combinations[prev:prev+N]

            prev += N

        self.start_runs(execute_program_rule, chunks, *args, **kwargs)

        self.clean()

        return 0

    def clear(self):
        self.parameter_sets = []

    def dump_config_files(self, proc):
        for parameter_set in self.parameter_sets:
            parameter_set.copy_config(self, proc)

    def start_runs(self, execute_program_rule, chunks, *args, **kwargs):

        if self.use_mpi:

            self.dump_config_files(comm.rank)

            chunk = chunks[comm.rank]

            self.run_chunk(execute_program_rule, chunk, comm.rank, *args, **kwargs)

        else:

            for proc, chunk in enumerate(chunks):

                self.dump_config_files(proc)

                thread = Worker(self, execute_program_rule, chunk, proc, *args, **kwargs)

                self.all_threads.append(thread)

                thread.start()

            for thread in self.all_threads:
                thread.join()

    def run_chunk(self, execute_program_rule, chunk, proc, *args, **kwargs):

        for combination in chunk:

            if self.stopped:
                break

            self.write_combination(proc, combination)

            success = execute_program_rule(proc, combination, *args, **kwargs)

            if success != 0 and not self.stopped:
                self.stop(proc, success)

    def write_combination(self, proc, combination):

        for parameter_set, value in zip(self.parameter_sets, combination):
            parameter_set.write_value(value, proc)

    def stop(self, which, status):
        print "Process %d ended with exit status %d: Stopping..." % (which, status)

        if not self.use_mpi:
            for thread in self.all_threads:
                thread.stop()

def quick_replace(cfg, name, value):

    cfg_str = ""
    with open(cfg, 'r') as f_read:
        cfg_str = f_read.read()

    repl = sub(r"(%s\s*=\s*[\"\']?).*?([\"\']?;)" % name, "\g<1>%s\g<2>" % str(value), cfg_str)

    with open(cfg, 'w') as f_write:
        f_write.write(repl)



def exec_test_function(proc, combination):

    time.sleep(proc/10.)

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

def testbed():

    testfile_name = "/tmp/test_paramloop.cfg"

    with open(testfile_name, 'w') as testfile:

        testfile.write("a=[0,1,2]\n")

    any_number = r".*?"

    set1 = ParameterSet(testfile_name, r"a\=\[(%s)\,%s\,%s\]" % (any_number, any_number, any_number))
    set2 = ParameterSet(testfile_name, r"a\=\[%s\,(%s)\,%s\]" % (any_number, any_number, any_number))
    set3 = ParameterSet(testfile_name, r"a\=\[%s\,%s\,(%s)\]" % (any_number, any_number, any_number))

    set1.initialize_set_incr( 0,   3,   1)
    set2.initialize_set([-2, -0.5, 1])
    set3.initialize_set_incr( 0, 100,  30, )

    controller = ParameterSetController(use_mpi=len(sys.argv) != 1)

    controller.register_parameter_set(set1)
    controller.register_parameter_set(set2)
    controller.register_parameter_set(set3)

    controller.run(exec_test_function, ask=False, n_procs=20)

if __name__ == "__main__":
    testbed()


